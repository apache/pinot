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


/**
 * The {@code Interval} class represents an one-dimensional closed interval which contains both ends.
 */
public class Interval implements Comparable<Interval> {
  public final long _min;
  public final long _max;

  public Interval(long min, long max) {
    Preconditions.checkState(min <= max, "invalid interval [{}, {}]", min, max);
    _min = min;
    _max = max;
  }

  public boolean intersects(Interval o) {
    Preconditions.checkNotNull(o, "Invalid interval: null");
    return _max >= o._min && o._max >= _min;
  }

  @Override
  public int compareTo(Interval o) {
    Preconditions.checkNotNull(o, "Compare to invalid interval: null");
    if (_min < o._min) {
      return -1;
    } else if (_min > o._min) {
      return 1;
    } else if (_max < o._max) {
      return -1;
    } else if (_max > o._max) {
      return 1;
    }
    else return 0;
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

  public static Interval getIntersection(Interval a, Interval b) {
    Preconditions.checkNotNull(a, "Intersect invalid intervals {} and {}", a, b);
    Preconditions.checkNotNull(b, "Intersect invalid intervals {} and {}", a, b);
    if (!a.intersects(b)) {
      return null;
    }
    return new Interval(Math.max(a._min, b._min), Math.min(a._max, b._max));
  }

  public static Interval getUnion(Interval a, Interval b) {
    // Can only merge two intervals if they overlap
    Preconditions.checkNotNull(a, "Union invalid intervals {} and {}", a, b);
    Preconditions.checkNotNull(b, "Union invalid intervals {} and {}", a, b);
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
