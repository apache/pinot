/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.api;

import java.io.Serializable;
import java.sql.Timestamp;

public class TimeRange implements Comparable<TimeRange>, Serializable {
  private static final long serialVersionUID = -403250971215465050L;

  private Long start;
  private Long end;

  public TimeRange() {
  }

  public TimeRange(Long start, Long end) {
    this.start = start;
    this.end = end;

    if (start > end) {
      throw new IllegalArgumentException(
          "start must be less than or equal to end: start=" + start + ", end=" + end);
    }
  }

  public Long getStart() {
    return start;
  }

  public Long getEnd() {
    return end;
  }

  public boolean contains(Long time) {
    return time >= start && time <= end;
  }

  public boolean contains(TimeRange timeRange) {
    return start <= timeRange.getStart() && end >= timeRange.getEnd();
  }

  public boolean isDisjoint(TimeRange timeRange) {
    return end < timeRange.getStart() || start > timeRange.getEnd();
  }

  public int totalBuckets() {
    return (int) (end - start + 1);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TimeRange)) {
      return false;
    }
    TimeRange tr = (TimeRange) o;
    return start.equals(tr.getStart()) && end.equals(tr.getEnd());
  }

  @Override
  public int hashCode() {
    return (int) (start + 13 * end);
  }

  @Override
  public int compareTo(TimeRange timeRange) {
    return (int) (start - timeRange.getStart());
  }

  @Override
  public String toString() {
    return "[" + start + " TO " + end + "]" + "[" + new Timestamp(start) + " TO "
        + new Timestamp(end) + "]";
  }
}
