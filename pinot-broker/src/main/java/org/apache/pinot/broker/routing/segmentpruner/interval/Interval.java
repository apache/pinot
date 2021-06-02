/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.routing.segmentpruner.interval;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;


/**
 * The {@code Interval} class represents an one-dimensional closed interval which contains both ends.
 */
public class Interval implements Comparable<Interval> {
  public final long _min;
  public final long _max;

  public Interval(long min, long max) {
    Preconditions.checkState(min <= max, "invalid interval [%s, %s]", min, max);
    _min = min;
    _max = max;
  }

  public boolean intersects(Interval o) {
    return _max >= o._min && o._max >= _min;
  }

  @Override
  public int compareTo(Interval o) {
    if (_min < o._min) {
      return -1;
    } else if (_min > o._min) {
      return 1;
    } else {
      return Long.compare(_max, o._max);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Interval interval = (Interval) o;

    if (_min != interval._min) {
      return false;
    }
    return _max == interval._max;
  }

  @Override
  public int hashCode() {
    int result = (int) (_min ^ (_min >>> 32));
    result = 31 * result + (int) (_max ^ (_max >>> 32));
    return result;
  }

  @Nullable
  public static Interval getIntersection(Interval a, Interval b) {
    if (!a.intersects(b)) {
      return null;
    }
    return new Interval(Math.max(a._min, b._min), Math.min(a._max, b._max));
  }

  @Nullable
  public static Interval getUnion(Interval a, Interval b) {
    // Can only merge two intervals if they overlap
    if (!a.intersects(b)) {
      return null;
    }
    return new Interval(Math.min(a._min, b._min), Math.max(a._max, b._max));
  }

  @Override
  public String toString() {
    return "[" + _min + ", " + _max + "]";
  }
}
