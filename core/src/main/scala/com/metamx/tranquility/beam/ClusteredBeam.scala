/*
 * Tranquility.
 * Copyright 2013, 2014, 2015  Metamarkets Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.metamx.tranquility.beam

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metamx.common.scala.{untyped, Logging}
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.event._
import com.metamx.common.scala.event.emit.emitAlert
import com.metamx.common.scala.option._
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util.Future
import com.twitter.util.FuturePool
import java.util.UUID
import java.util.concurrent.Executors
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.chrono.ISOChronology
import org.scala_tools.time.Implicits._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.reflectiveCalls
import scala.util.Random

/**
 * Beam composed of a stack of smaller beams. The smaller beams are split across two axes: timestamp (time shard
 * of the data) and partition (shard of the data within one time interval). The stack of beams for a particular
 * timestamp are created in a coordinated fashion, such that all ClusteredBeams for the same identifier will have
 * semantically identical stacks. This interaction is mediated through zookeeper. Beam information persists across
 * ClusteredBeam restarts.
 *
 * In the case of Druid, each merged beam corresponds to one segment partition number, and each inner beam corresponds
 * to either one index task or a set of redundant index tasks.
 *
 * {{{
 *                                            ClusteredBeam
 *
 *                                   +-------------+---------------+
 *               2010-01-02T03:00:00 |                             |   2010-01-02T04:00:00
 *                                   |                             |
 *                                   v                             v
 *
 *                         +----+ Merged +----+                   ...
 *                         |                  |
 *                    partition 1         partition 2
 *                         |                  |
 *                         v                  v
 *
 *                     Decorated           Decorated
 *
 *                   InnerBeamType       InnerBeamType
 * }}}
 */
class ClusteredBeam[EventType: Timestamper, InnerBeamType <: Beam[EventType]](
  zkBasePath: String,
  identifier: String,
  tuning: ClusteredBeamTuning,
  curator: CuratorFramework,
  emitter: ServiceEmitter,
  timekeeper: Timekeeper,
  objectMapper: ObjectMapper,
  beamMaker: BeamMaker[EventType, InnerBeamType],
  beamDecorateFn: (Interval, Int) => Beam[EventType] => Beam[EventType],
  beamMergeFn: Seq[Beam[EventType]] => Beam[EventType],
  alertMap: Dict
) extends Beam[EventType] with Logging
{
  def getInterval() = None

  require(tuning.partitions > 0, "tuning.partitions > 0")
  require(tuning.minSegmentsPerBeam > 0, "tuning.minSegmentsPerBeam > 0")
  require(tuning.maxSegmentsPerBeam >= tuning.minSegmentsPerBeam, "tuning.maxSegmentsPerBeam >= tuning.minSegmentsPerBeam")

  // Thread pool for blocking zk operations
  private[this] val zkFuturePool = FuturePool(
    Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("ClusteredBeam-ZkFuturePool-%s" format UUID.randomUUID)
        .build()
    )
  )

  // Location of beam-related metadata in ZooKeeper.
  private[this] def zpath(path: String): String = {
    require(path.nonEmpty, "path must be nonempty")
    "%s/%s/%s" format(zkBasePath, identifier, path)
  }

  private[this] def zpathWithDefault(path: String, default: => Array[Byte]): String = {
    zpath(path) withEffect {
      p =>
        if (curator.checkExists().forPath(p) == null) {
          try {
            curator.create().creatingParentsIfNeeded().forPath(p, default)
          }
          catch {
            case e: NodeExistsException => // suppress
          }
        }
    }
  }

  // Mutex for modifying beam metadata.
  private[this] val mutex = new InterProcessSemaphoreMutex(curator, zpath("mutex"))

  // We will refuse to create beams earlier than this timestamp. The purpose of this is to prevent recreating beams
  // that we thought were closed.
  @volatile private[this] var localLatestCloseTime = new DateTime(0, ISOChronology.getInstanceUTC)

  private[this] val rand = new Random

  private[this] var zkMetaCache = {
    try {
      val dataPath = zpathWithDefault("data", ClusteredBeamMeta.empty.toBytes(objectMapper))
      curator.sync().forPath(dataPath)
      ClusteredBeamMeta.fromBytes(objectMapper, curator.getData.forPath(dataPath)).fold(
        e => {
          emitAlert(e, log, emitter, WARN, "Failed to read beam data from cache: %s" format identifier, alertMap)
          throw e
        },
        meta => meta
      )
    }
    catch {
      // Fail if we fail to read from Zookeeper during startup
      case e: Throwable => throw e
    }
  }

  // Reverse sorted list (by interval start time) of Merged beams we are currently aware of
  private[this] var beams: List[Beam[EventType]] = Nil

  // Lock updates to "localLatestCloseTime" and "beams" to prevent races.
  private[this] val beamWriteMonitor = new AnyRef

  private[this] lazy val data = new {
    val dataPath = zpathWithDefault("data", ClusteredBeamMeta.empty.toBytes(objectMapper))

    def modify(f: ClusteredBeamMeta => ClusteredBeamMeta): Future[ClusteredBeamMeta] = zkFuturePool {
      mutex.acquire()
      try {
        curator.sync().forPath(dataPath)
        val prevMeta = ClusteredBeamMeta.fromBytes(objectMapper, curator.getData.forPath(dataPath)).fold(
          e => {
            emitAlert(e, log, emitter, WARN, "Failed to read beam data from cache: %s" format identifier, alertMap)
            throw e
          },
          meta => meta
        )
        val newMeta = f(prevMeta)
        if (newMeta != prevMeta) {
          zkMetaCache = newMeta
          val newMetaBytes = newMeta.toBytes(objectMapper)
          log.info("Writing new beam data to[%s]: %s", dataPath, new String(newMetaBytes))
          curator.setData().forPath(dataPath, newMetaBytes)
        }
        newMeta
      }
      catch {
        case e: Throwable =>
          // Log Throwables to avoid invisible errors caused by https://github.com/twitter/util/issues/100.
          log.error(e, "Failed to update cluster state: %s", identifier)
          throw e
      }
      finally {
        mutex.release()
      }
    }
  }

  @volatile private[this] var open = true

  val timestamper: EventType => DateTime = {
    val theImplicit = implicitly[Timestamper[EventType]].timestamp _
    t => theImplicit(t).withZone(DateTimeZone.UTC)
  }

  private[this] def beam(beamPair :(Interval, Option[Beam[EventType]]), now: DateTime): Future[Beam[EventType]] = {
    val creationInterval = new Interval(
      tuning.segmentBucket(now - tuning.windowPeriod).start,
      tuning.segmentBucket(Seq(now + tuning.warmingPeriod, now + tuning.windowPeriod).maxBy(_.millis)).end
    )
    val futureBeamOption = beamPair match {
      case _ if !open => Future.value(None)
      case (interval, Some(foundBeam)) if foundBeam.getInterval().get.end + tuning.windowPeriod <= now => Future.value(None)
      case (interval, Some(foundBeam)) => Future.value(Some(foundBeam))
      case (interval, None) if interval.start <= localLatestCloseTime => Future.value(None)
      case (interval, None) if interval.end + tuning.windowPeriod <= now => Future.value(None)
      case (interval, None) if !creationInterval.overlaps(interval) => Future.value(None)
      case (interval, None) if interval.toDurationMillis == 0 => Future.value(None)
      case (interval, None) =>
        // We may want to create new merged beam(s). Acquire the zk mutex and examine the situation.
        // This could be more efficient, but it's happening infrequently so it's probably not a big deal.
        data.modify {
          prev =>
            log.info("Trying to create new beam with interval [%s]", interval)
            // We want to create this new beam
            // But first let us check in ZK if there is already any beam in ZK covering this interval
            val beamDicts: Seq[untyped.Dict] = prev.beamDictss.collectFirst[Seq[untyped.Dict]]({
              case x if new Interval(x._2.head.get("interval").get, ISOChronology.getInstanceUTC).overlaps(interval) => x._2
            }) getOrElse Nil

            if (beamDicts.size >= tuning.partitions) {
              log.info(
                "Merged beam already created for identifier[%s] interval[%s], with sufficient partitions (target = %d, actual = %d)",
                identifier,
                interval,
                tuning.partitions,
                beamDicts.size
              )
              prev
            } else if (interval.start <= prev.latestCloseTime) {
              log.info(
                "Global latestCloseTime[%s] for identifier[%s] has moved past timestamp[%s], not creating merged beam",
                prev.latestCloseTime,
                identifier,
                interval.start
              )
              prev
            } else if (beamDicts.nonEmpty &&
                !beamDicts.exists(
                  beamDict => new Interval(beamDict.get("interval").get, ISOChronology.getInstanceUTC).contains(interval)
                ))
            {
              throw new IllegalStateException(
                "WTF?? Requested to create a beam for interval [%s] which overlaps with existing beam [%s]" format(interval, beamDicts)
              )
            } else {
              // Create the new beam
              assert(beamDicts.size < tuning.partitions)
              assert(interval.start > prev.latestCloseTime)

              val numSegmentsToCover = tuning.minSegmentsPerBeam +
                rand.nextInt(tuning.maxSegmentsPerBeam - tuning.minSegmentsPerBeam + 1)
              var intervalToCover = new Interval(
                interval.start.millis,
                tuning.segmentGranularity.increment(interval.start, numSegmentsToCover).millis,
                ISOChronology.getInstanceUTC
              )
              var timestampsToCover = tuning.segmentGranularity.getIterable(intervalToCover).asScala.map(_.start)

              // Check if we are trying to create a beam not covering an entire segment bucket
              if (!tuning.segmentGranularity.widen(interval).equals(interval)) {
                log.warn("Creating partial beam with interval [%s] as beam [%s] already covers some segment portion", beamDicts, interval)
                intervalToCover =  interval
                timestampsToCover = Iterable(intervalToCover.start)
              }

              // OK, create them where needed.
              val newInnerBeamDictsByPartition = new mutable.HashMap[Int, Dict]
              val newBeamDictss: Map[Long, Seq[Dict]] = (prev.beamDictss filterNot {
                case (millis, beam) =>
                  // Expire old beamDicts
                  new Interval(beam.head.get("interval").get, ISOChronology.getInstanceUTC).end + tuning.windowPeriod < now
              }) ++ (for (ts <- timestampsToCover) yield {
                val tsPrevDicts = prev.beamDictss.getOrElse(ts.millis, Nil)
                log.info(
                  "Creating new merged beam for identifier[%s] timestamp[%s] (target = %d, actual = %d)",
                  identifier,
                  ts,
                  tuning.partitions,
                  tsPrevDicts.size
                )
                val tsNewDicts = tsPrevDicts ++ ((tsPrevDicts.size until tuning.partitions) map {
                  partition =>
                    newInnerBeamDictsByPartition.getOrElseUpdate(
                    partition, {
                      // Create sub-beams and then immediately close them, just so we can get the dict representations.
                      // Close asynchronously, ignore return value.
                      beamMaker.newBeam(intervalToCover, partition).withFinally(_.close()) {
                        beam =>
                          val beamDict = beamMaker.toDict(beam)
                          log.info("Created beam: %s", objectMapper.writeValueAsString(beamDict))
                          beamDict
                      }
                    }
                    )
                })
                (ts.millis, tsNewDicts)
              })
              val newLatestCloseTime = new DateTime(
                (Seq(prev.latestCloseTime.millis) ++ (prev.beamDictss.keySet -- newBeamDictss.keySet)).max,
                ISOChronology.getInstanceUTC
              )
              ClusteredBeamMeta(newLatestCloseTime, newBeamDictss)
            }
        } rescue {
          case e: Throwable =>
            Future.exception(
              new IllegalStateException(
                "Failed to save new beam for identifier[%s] timestamp[%s]" format(identifier, interval.start), e
              )
            )
        } map {
          meta =>
            // Update local stuff with our goodies from zk.
            beamWriteMonitor.synchronized {
              localLatestCloseTime = meta.latestCloseTime
              val timestamp = interval.start
              // Only add the beams we actually wanted at this time. This is because there might be other beams in ZK
              // that we don't want to add just yet, on account of maybe they need their partitions expanded (this only
              // happens when they are the necessary ones).
              if (!beams.exists(beam => beam.getInterval().get.start.eq(timestamp)) &&
                meta.beamDictss.contains(timestamp.millis)) {

                val beamDicts = meta.beamDictss(timestamp.millis)
                log.info("Adding beams for identifier[%s] timestamp[%s]: %s", identifier, timestamp, beamDicts)
                // Should have better handling of unparseable zk data. Changing BeamMaker implementations currently
                // just causes exceptions until the old dicts are cleared out.
                beams = beamMergeFn(
                  beamDicts.zipWithIndex map {
                    case (beamDict, partitionNum) =>
                      val decorate = beamDecorateFn(tuning.segmentBucket(timestamp), partitionNum)
                      decorate(beamMaker.fromDict(beamDict))
                  }
                ) +: beams
              }
              // Remove beams that are gone from ZK metadata. They have expired.
              val expiredBeams = beams.filterNot(beam => meta.beamDictss.contains(beam.getInterval().get.start.millis))

              for (beam <- expiredBeams) {
                log.info("Removing beams for identifier[%s] timestamp[%s]", identifier, beam.getInterval().get.start)
                // Close asynchronously, ignore return value.
                beam.close()
              }
              beams = beams.diff(expiredBeams)
              // This may not be required as we always create and add beams in chronological order
              // so in effect the list should always be reverse sorted
              beams = beams.sortBy(-_.getInterval().get.start.millis)

              // Return requested beam. It may not have actually been created, so it's an Option.
              beams.find(beam => beam.getInterval().get.contains(timestamp)) ifEmpty {
                log.info(
                  "Turns out we decided not to actually make beams for identifier[%s] timestamp[%s]. Returning None.",
                  identifier,
                  timestamp
                )
              }
            }
        }
    }
    futureBeamOption map {
      beamOpt =>
        // If we didn't find a beam, then create a special dummy beam just for this batch. This allows us to apply
        // any merge or decorator logic to dropped events, which is nice if there are side effects (such as metrics
        // emission, logging, or alerting).
        beamOpt.getOrElse(
          beamMergeFn(
            (0 until tuning.partitions) map {
              partition =>
                beamDecorateFn(beamPair._1, partition)(new NoopBeam[EventType])
            }
          )
        )
    }
  }

  def groupEvents(event: EventType, intervalBeamPair: mutable.Map[Interval, Option[Beam[EventType]]]): (Interval, Option[Beam[EventType]]) = {
    val eventTimestamp = timestamper(event)
    // looks in sequential order for the predicate to be true
    // Most of the times head beam can handle the event as the beams is reverse sorted list by start time of interval

    intervalBeamPair.find(_._1.contains(eventTimestamp)) match {
      case Some(x) => (x._1, x._2)
      case None =>
        val requiredInterval = tuning.segmentBucket(eventTimestamp).withChronology(ISOChronology.getInstanceUTC)
        // Check to see if the interval for which we want to create a beam overlaps with interval of any existing beam
        // this may happen if segment granularity is changed
        val actualInterval = zkMetaCache.beamDictss.toSeq.sortBy(-_._1) collectFirst {
          case x if new Interval(x._2.head.get("interval").get, ISOChronology.getInstanceUTC).contains(eventTimestamp) =>
            new Interval(x._2.head.get("interval").get, ISOChronology.getInstanceUTC)
        } match {
          case Some(x) => x
          case None =>
            zkMetaCache.beamDictss.toSeq.sortBy(-_._1) collectFirst {
              case x if new Interval(x._2.head.get("interval").get, ISOChronology.getInstanceUTC).overlaps(requiredInterval) =>
                new Interval(x._2.head.get("interval").get, ISOChronology.getInstanceUTC)
            } match {
              case None => requiredInterval
              case Some(x) =>
                new Interval(
                  Math.max(x.end.millis, requiredInterval.start.millis),
                  Math.max(x.end.millis, requiredInterval.end.millis),
                  ISOChronology.getInstanceUTC
                )
            }
        }
        intervalBeamPair.put(actualInterval, None)
        (actualInterval, None)
    }
  }

  def propagate(events: Seq[EventType]) = {
    val now = timekeeper.now.withZone(DateTimeZone.UTC)
    val intervalBeamPair = new mutable.HashMap[Interval, Option[Beam[EventType]]]()
    beams map {
      beam => intervalBeamPair.+=((beam.getInterval().get, Some(beam)))
    }
    val grouped = events.groupBy(groupEvents(_, intervalBeamPair)).toSeq.sortBy(_._1._1.start.millis)
    // Possibly warm up future beams
    def toBeWarmed(dt: DateTime, end: DateTime): List[DateTime] = {
      if (dt <= end) {
        dt :: toBeWarmed(tuning.segmentBucket(dt).end, end)
      } else {
        Nil
      }
    }
    val warmingBeams = Future.collect(for (
      latestEvent <- grouped.lastOption.map(_._2.maxBy(timestamper(_).millis)).map(timestamper).toList;
      tbwTimestamp <- toBeWarmed(latestEvent, latestEvent + tuning.warmingPeriod) if tbwTimestamp > latestEvent &&
        !beams.exists(_.getInterval().get.contains(tbwTimestamp))
    ) yield {
      // Create beam asynchronously
      beam((tuning.segmentBucket(tbwTimestamp).withChronology(ISOChronology.getInstanceUTC), None), now)
    })
    // Propagate data
    val countFutures = for ((beamIntervalPair, eventGroup) <- grouped) yield {
      beam(beamIntervalPair, now) onFailure {
        e =>
          emitAlert(e, log, emitter, WARN, "Failed to create merged beam: %s" format identifier, alertMap)
      } flatMap {
        beam =>
          // We expect beams to handle retries, so if we get an exception here let's drop the batch
          beam.propagate(eventGroup) rescue {
            case e: DefunctBeamException =>
              val timestamp = beam.getInterval().get.start
              // Just drop data until the next segment starts. At some point we should look at doing something
              // more intelligent.
              emitAlert(
                e, log, emitter, WARN, "Beam defunct: %s" format identifier,
                alertMap ++
                  Dict(
                    "eventCount" -> eventGroup.size,
                    "timestamp" -> timestamp.toString,
                    "beam" -> beam.toString
                  )
              )
              data.modify {
                prev =>
                  ClusteredBeamMeta(
                    Seq(prev.latestCloseTime, timestamp).maxBy(_.millis),
                    prev.beamDictss - timestamp.millis
                  )
              } onSuccess {
                meta =>
                  beamWriteMonitor.synchronized {
                    beams = beams diff List(beam)
                  }
              } map (_ => 0)

            case e: Exception =>
              emitAlert(
                e, log, emitter, WARN, "Failed to propagate events: %s" format identifier,
                alertMap ++
                  Dict(
                    "eventCount" -> eventGroup.size,
                    "timestamp" -> beam.getInterval().get.start.toString,
                    "beams" -> beam.toString
                  )
              )
              Future.value(0)
          }
      }
    }
    val countFuture = Future.collect(countFutures).map(_.sum)
    warmingBeams.flatMap(_ => countFuture) // Resolve only when future beams are warmed up.
  }

  def close() = {
    beamWriteMonitor.synchronized {
      open = false
      val closeFuture = Future.collect(beams map (_.close())) map (_ => ())
      beams = beams diff beams
      closeFuture
    }
  }

  override def toString = "ClusteredBeam(%s)" format identifier
}

/**
 * Metadata stored in ZooKeeper for a ClusteredBeam.
 *
 * @param latestCloseTime Most recently shut-down interval (to prevent necromancy).
 * @param beamDictss Map of interval start -> beam metadata, partition by partition.
 */
case class ClusteredBeamMeta(latestCloseTime: DateTime, beamDictss: Map[Long, Seq[Dict]])
{
  def toBytes(objectMapper: ObjectMapper) = objectMapper.writeValueAsBytes(
    Dict(
      // latestTime is only being written for backwards compatibility
      "latestTime" -> new DateTime(
        (Seq(latestCloseTime.millis) ++ beamDictss.keys).max,
        ISOChronology.getInstanceUTC
      ).toString,
      "latestCloseTime" -> latestCloseTime.toString,
      "beams" -> beamDictss.map(kv => (new DateTime(kv._1, ISOChronology.getInstanceUTC).toString, kv._2))
    )
  )
}

object ClusteredBeamMeta
{
  def empty = ClusteredBeamMeta(new DateTime(0, ISOChronology.getInstanceUTC), Map.empty)

  def fromBytes[A](objectMapper: ObjectMapper, bytes: Array[Byte]): Either[Exception, ClusteredBeamMeta] = {
    try {
      val d = objectMapper.readValue(bytes, classOf[Dict])
      val beams: Map[Long, Seq[Dict]] = dict(d.getOrElse("beams", Dict())) map {
        case (k, vs) =>
          val ts = new DateTime(k, ISOChronology.getInstanceUTC)
          val beamDicts = list(vs) map (dict(_))
          (ts.millis, beamDicts)
      }
      val latestCloseTime = new DateTime(d.getOrElse("latestCloseTime", 0L), ISOChronology.getInstanceUTC)
      Right(ClusteredBeamMeta(latestCloseTime, beams))
    }
    catch {
      case e: Exception =>
        Left(e)
    }
  }
}
