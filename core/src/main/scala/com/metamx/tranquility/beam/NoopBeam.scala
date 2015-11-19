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

import com.twitter.util.Future
import org.joda.time.Interval
import org.joda.time.chrono.ISOChronology

class NoopBeam[A] extends Beam[A]
{
  def propagate(events: Seq[A]) = Future.value(0)

  def close() = Future.Done

  override def toString = "NoopBeam()"

  def getInterval() = Some(new Interval(0, 0, ISOChronology.getInstanceUTC))
}
