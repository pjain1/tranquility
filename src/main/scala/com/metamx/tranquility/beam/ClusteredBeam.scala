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
import com.metamx.common.Granularity
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.common.scala.event._
import com.metamx.common.scala.event.emit.emitAlert
import com.metamx.common.scala.option._
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped._
import com.metamx.emitter.service.ServiceEmitter
import com.metamx.tranquility.druid.DruidBeamMaker
import com.metamx.tranquility.typeclass.Timestamper
import com.twitter.util.{Await, Future, FuturePool}
import java.util.UUID
import java.util.concurrent.Executors
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.joda.time.{DateTime, Interval}
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
  @volatile private[this] var localLatestCloseTime = new DateTime(0)

  @volatile private[this] var zkMetadataCache: ClusteredBeamMeta =  {
    try {
      val dataPath = zpathWithDefault("data", ClusteredBeamMeta.empty.toBytes(objectMapper))
      curator.sync().forPath(dataPath)
      val prevMeta = ClusteredBeamMeta.fromBytes(objectMapper, curator.getData.forPath(dataPath)).fold(
        e => {
          emitAlert(e, log, emitter, WARN, "Failed to read beam data from cache: %s" format identifier, alertMap)
          throw e
        },
        meta => meta
      )
      prevMeta
    }
    catch {
      case e: Throwable =>
        // Log Throwables to avoid invisible errors caused by https://github.com/twitter/util/issues/100.
        log.error(e, "Failed to sync with zookeeper: %s", identifier)
        throw e
    }
  }

  private[this] val rand = new Random

  // Merged beams we are currently aware of, interval start millis -> merged beam.
  private[this] val beams = ConcurrentMap[Long, Beam[EventType]]()

  // Lock updates to "localLatestCloseTime" and "beams" to prevent races.
  private[this] val beamWriteMonitor = new AnyRef

  @volatile private[this] var allowGranularityChange = false

  private[this] val timestamper = implicitly[Timestamper[EventType]].timestamp _

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
          val newMetaBytes = newMeta.toBytes(objectMapper)
          log.info("Writing new beam data to[%s]: %s", dataPath, new String(newMetaBytes))
          curator.setData().forPath(dataPath, newMetaBytes)
        }
        zkMetadataCache = newMeta

        log.debug(zkMetadataCache.latestCloseTime.toString)
        zkMetadataCache.beamDictss foreach {
          beamDict =>
            log.debug(beamDict._2.toString())
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

  private[this] def beam(timestamp: DateTime, now: DateTime): Future[Beam[EventType]] = {
    val bucket = tuning.segmentBucket(timestamp)
    val creationInterval = new Interval(
      tuning.segmentBucket(now - tuning.windowPeriod).start,
      tuning.segmentBucket(Seq(now + tuning.warmingPeriod, now + tuning.windowPeriod).maxBy(_.millis)).end
    )
    val windowInterval = new Interval(
      tuning.segmentBucket(now - tuning.windowPeriod).start,
      tuning.segmentBucket(now + tuning.windowPeriod).end
    )
    log.debug("Looking for beam having start time [%s]", timestamp)
    val futureBeamOption = beams.get(timestamp.millis) match {
      case _ if !open => Future.value(None)
      // If the granularity has changed since tranquility restart and we found some beam that can handle this event return that beam
      // In some scenarios the events may be dropped either by the client after some retry if the task corresponding to beam is already finished
      // or by Druid if the event is outside the window period
      // This case be further optimized to considering the actual granularity of the beam instead of tuning.segmentBucket to find out windowInterval
      case Some(x) if allowGranularityChange => {
        log.warn("Mismatch in segment granularities detected between the current segment granularity and the segment granularity of beam to which these events belong, operating in safe mode - will not warm up beams and perform strict checking of intervals for beams until unless all the previous beams and/or current partial beams are closed")
        Future.value(Some(x))
      }
      case Some(x) if windowInterval.overlaps(bucket) => Future.value(Some(x))
      case Some(x) => Future.value(None)
      case None if timestamp <= localLatestCloseTime => Future.value(None)
      case None if !creationInterval.overlaps(bucket) => Future.value(None)
      case None =>
        // We may want to create new merged beam(s). Acquire the zk mutex and examine the situation.
        // This could be more efficient, but it's happening infrequently so it's probably not a big deal.
        data.modify {
          prev =>
            val prevBeamDicts = prev.beamDictss.getOrElse(timestamp.millis, Nil)
            if (prevBeamDicts.size >= tuning.partitions) {
              log.info(
                "Merged beam already created for identifier[%s] timestamp[%s], with sufficient partitions (target = %d, actual = %d)",
                identifier,
                timestamp,
                tuning.partitions,
                prevBeamDicts.size
              )
              prev
            } else if (timestamp <= prev.latestCloseTime) {
              log.info(
                "Global latestCloseTime[%s] for identifier[%s] has moved past timestamp[%s], not creating merged beam",
                prev.latestCloseTime,
                identifier,
                timestamp
              )
              prev
            } else {
              assert(prevBeamDicts.size < tuning.partitions)
              assert(timestamp > prev.latestCloseTime)

              var optionalIntervalToCover = None: Option[Interval]
              var optionalTimestampsToCover = None: Option[Iterable[DateTime]]
              if(allowGranularityChange) {
                // Do not cover multiple segments until we are operating normally again
                optionalIntervalToCover = Some(timestamp to bucket.end)
                optionalTimestampsToCover = Some(Seq(timestamp))
                if(timestamp.millis != bucket.start.millis) {
                  log.warn("Creating a partial beam with interval [%s] as there might already be some beams present for the bucket [%s], looks like segment granularity is changed in between the interval",
                    optionalIntervalToCover,
                    bucket
                  )
                }
              } else {
                // We might want to cover multiple time segments in advance.
                val numSegmentsToCover = tuning.minSegmentsPerBeam +
                  rand.nextInt(tuning.maxSegmentsPerBeam - tuning.minSegmentsPerBeam + 1)
                optionalIntervalToCover = Some(timestamp to tuning.segmentGranularity.increment(timestamp, numSegmentsToCover))
                optionalTimestampsToCover = Some(tuning.segmentGranularity.getIterable(optionalIntervalToCover.get).asScala.map(_.start))
              }

              val intervalToCover = optionalIntervalToCover.get
              val timestampsToCover = optionalTimestampsToCover.get

              // OK, create them where needed.
              val newInnerBeamDictsByPartition = new mutable.HashMap[Int, Dict]
              val newBeamDictss: Map[Long, Seq[Dict]] = (prev.beamDictss filterNot {
                case (millis, beam) =>
                  // Expire old beamDicts
                  // We would like to use the segment granularity of beam to check whether we can expire it or not
                  // If not present then use the current tuning segment granularity
                  DruidBeamMaker.getSegmentGranularity(beam.head.getOrElse("granularityId", "NONE").toString) match {
                    case Some(granularity) => granularity.increment(new DateTime(millis)) + tuning.windowPeriod < now
                    case None => tuning.segmentGranularity.increment(new DateTime(millis)) + tuning.windowPeriod < now
                  }
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
                    newInnerBeamDictsByPartition.getOrElseUpdate(partition, {
                      // Create sub-beams and then immediately close them, just so we can get the dict representations.
                      // Close asynchronously, ignore return value.
                      beamMaker.newBeam(intervalToCover, partition, tuning.segmentGranularity, allowGranularityChange).withFinally(_.close()) {
                        beam =>
                          val beamDict = beamMaker.toDict(beam)
                          log.info("Created beam: %s", objectMapper.writeValueAsString(beamDict))
                          beamDict
                      }
                    })
                })
                (ts.millis, tsNewDicts)
              })
              val newLatestCloseTime = new DateTime(
                (Seq(prev.latestCloseTime.millis) ++ (prev.beamDictss.keySet -- newBeamDictss.keySet)).max
              )

              ClusteredBeamMeta(
                newLatestCloseTime,
                newBeamDictss
              )
            }
        } rescue {
          case e: Throwable =>
            Future.exception(
              new IllegalStateException(
                "Failed to save new beam for identifier[%s] timestamp[%s]" format(identifier, timestamp), e
              )
            )
        } map {
          meta =>
            // Update local stuff with our goodies from zk.
            beamWriteMonitor.synchronized {
              localLatestCloseTime = meta.latestCloseTime
              // Only add the beams we actually wanted at this time. This is because there might be other beams in ZK
              // that we don't want to add just yet, on account of maybe they need their partitions expanded (this only
              // happens when they are the necessary ones).
              if (!beams.contains(timestamp.millis) && meta.beamDictss.contains(timestamp.millis)) {
                val beamDicts = meta.beamDictss(timestamp.millis)
                log.info("Adding beams for identifier[%s] timestamp[%s]: %s", identifier, timestamp, beamDicts)
                // Should have better handling of unparseable zk data. Changing BeamMaker implementations currently
                // just causes exceptions until the old dicts are cleared out.
                beams(timestamp.millis) = beamMergeFn(
                  beamDicts.zipWithIndex map {
                    case (beamDict, partitionNum) =>
                      val decorate = beamDecorateFn(bucket, partitionNum)
                      decorate(beamMaker.fromDict(beamDict, allowGranularityChange))
                  }
                )
              }
              // Remove beams that are gone from ZK metadata. They have expired.
              for ((timestamp, beam) <- beams -- meta.beamDictss.keys) {
                log.info("Removing beams for identifier[%s] timestamp[%s]", identifier, timestamp)
                // Close asynchronously, ignore return value.
                beams(timestamp).close()
                beams.remove(timestamp)
              }
              // Return requested beam. It may not have actually been created, so it's an Option.
              beams.get(timestamp.millis) ifEmpty {
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
                beamDecorateFn(bucket, partition)(new NoopBeam[EventType])
            }
          )
        )
    }
  }

  def groupEvents(eventType: EventType, clusteredBeamMeta: ClusteredBeamMeta): DateTime = {
    allowGranularityChange = false
    val eventTimestamp = timestamper(eventType)
    val eventBucket = tuning.segmentBucket(eventTimestamp)
    // ZK does not know about any beams
    if (clusteredBeamMeta.equals(ClusteredBeamMeta.empty) || clusteredBeamMeta.beamDictss.isEmpty) {
      eventBucket.start
    } else {
      val latestBeam = clusteredBeamMeta.beamDictss.get(clusteredBeamMeta.beamDictss.map(_._1).max).get.head
      val latestBeamInterval = new Interval(latestBeam("interval"))
      val newSegGranMillis = tuning.segmentGranularity.bucket(new DateTime()).toInterval.toDurationMillis

      // Look into ZK metadata if there is any active beam that can handle this event
      clusteredBeamMeta.beamDictss find {
        beam =>
          beam._2 exists {
            beamData =>
              new Interval(beamData.get("interval").get).contains(eventTimestamp)
          }
      } match {
        // Beam metadata is old so we do not know the actual segment granularity for this beam as they can cover multiple segments ... continue as it used to happen in past
        case Some(x) if x._2.head.get("granularityId").isEmpty => eventBucket.start
        // A beam already exists for this event timestamp and has same granularity as the new one and is not a partial beam (see the last case for what is partial beam)
        case Some(x) if newSegGranMillis == DruidBeamMaker.getSegmentGranularity(x._2.head("granularityId").toString).get.bucket(eventTimestamp).toDurationMillis
          && new Interval(x._2.head("interval")).start.millis == tuning.segmentBucket(new Interval(x._2.head("interval")).start).start.millis  => new DateTime(x._1)

        case Some(x) => {
          // We found one beam however the beam granularity does not seem to be equal to the new segment granularity
          // so lets add it to our beam map if not present already and return the start of the beam interval. No need to update ZK
          // It can be because of two reasons -
          //  1. Granularity has been decreased or
          //  2. The event belongs to a past interval which covers partial segment interval because the segment granularity was increased in middle of that new segment bucket
          //     Basically it is a late event and might be dropped depending on the window period.

          allowGranularityChange = true

          beams.getOrElseUpdate(
            x._1,
            beamMergeFn(
              x._2.zipWithIndex map {
                case (beamDict, partitionNum) =>
                  // Note - Using segment granularity of existing beam not the one set in Beam Tuning as they might differ
                  // Although it might not matter as of now as the default decorate function is an identity function
                  val decorate = beamDecorateFn(DruidBeamMaker.getSegmentGranularity(x._2.head("granularityId").toString).get.bucket(eventTimestamp), partitionNum)
                  decorate(beamMaker.fromDict(beamDict, allowGranularityChange))
              }
            )
          )
          new DateTime(x._1)
        }

        case None => {
          if (eventBucket.start.millis >= latestBeamInterval.end.millis &&
            eventTimestamp.millis >= latestBeamInterval.end.millis) {
            // Create a new beam, latest beam is outdated
            tuning.segmentBucket(eventTimestamp).start
          } else if (newSegGranMillis < latestBeamInterval.millis) {
            // Control should never reach here as we will always find an existing beam if we decreased the granularity
            new DateTime(latestBeamInterval.start)
          } else {
            // The current event bucket overlaps with existing beam interval
            // Create a partial beam as segment granularity is increased such that the new segment bucket contains past active tasks thus
            // the beam will have interval start time as end time of latest Beam and end time as per the segment granularity
            allowGranularityChange = true
            Seq(latestBeamInterval.end, tuning.segmentBucket(eventTimestamp).start).maxBy(_.millis)
          }
        }
      }
    }
  }

  def propagate(events: Seq[EventType]) = {

    val grouped = events.groupBy(groupEvents(_, zkMetadataCache)).toSeq.sortBy(_._1.millis)
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
      tbwTimestamp <- toBeWarmed(latestEvent, latestEvent + tuning.warmingPeriod) if tbwTimestamp > latestEvent && !allowGranularityChange
      // Do not warm up beams until we are out of the weird state of having multiple active beams with different granularities
    ) yield {
      // Create beam asynchronously
      beam(tbwTimestamp, timekeeper.now)
    })
    // Propagate data
    val countFutures = for ((timestamp, eventGroup) <- grouped) yield {
      beam(timestamp, timekeeper.now) onFailure {
        e =>
          emitAlert(e, log, emitter, WARN, "Failed to create merged beam: %s" format identifier, alertMap)
      } flatMap {
        beam =>
          // We expect beams to handle retries, so if we get an exception here let's drop the batch
          beam.propagate(eventGroup) rescue {
            case e: DefunctBeamException =>
              // Just drop data until the next segment starts. At some point we should look at doing something
              // more intelligent.
              emitAlert(
                e, log, emitter, WARN, "Beam defunct: %s" format identifier,
                alertMap ++
                  Dict(
                    "eventCount" -> eventGroup.size,
                    "timestamp" -> timestamp.toString(),
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
                    beams.remove(timestamp.millis)
                  }
              } map (_ => 0)

            case e: Exception =>
              emitAlert(
                e, log, emitter, WARN, "Failed to propagate events: %s" format identifier,
                alertMap ++
                  Dict(
                    "eventCount" -> eventGroup.size,
                    "timestamp" -> timestamp.toString(),
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
      val closeFuture = Future.collect(beams.values.toList map (_.close())) map (_ => ())
      beams.clear()
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
      "latestTime" -> new DateTime((Seq(latestCloseTime.millis) ++ beamDictss.map(_._1)).max).toString(),
      "latestCloseTime" -> latestCloseTime.toString(),
      "beams" -> beamDictss.map(kv => (new DateTime(kv._1).toString(), kv._2))
    )
  )
}

object ClusteredBeamMeta
{
  def empty = ClusteredBeamMeta(new DateTime(0), Map.empty)

  def fromBytes[A](objectMapper: ObjectMapper, bytes: Array[Byte]): Either[Exception, ClusteredBeamMeta] = {
    try {
      val d = objectMapper.readValue(bytes, classOf[Dict])
      val beams: Map[Long, Seq[Dict]] = dict(d.getOrElse("beams", Dict())) map {
        case (k, vs) =>
          val ts = new DateTime(k)
          val beamDicts = list(vs) map (dict(_))
          (ts.millis, beamDicts)
      }
      val latestCloseTime = new DateTime(d.getOrElse("latestCloseTime", 0L))
      Right(ClusteredBeamMeta(latestCloseTime, beams))
    }
    catch {
      case e: Exception =>
        Left(e)
    }
  }
}
