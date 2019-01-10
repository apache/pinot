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
package com.linkedin.thirdeye.hadoop.config;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * TimeGranularity class contains time unit and time size of the star tree time config
 *
 *  unit: the TimeUnit of the column
 *  size: the bucket size of the time column
 */
public class TimeGranularity {
  private static int DEFAULT_TIME_SIZE = 1;

  private int size = DEFAULT_TIME_SIZE;
  private TimeUnit unit;

  public TimeGranularity() {
  }

  public TimeGranularity(int size, TimeUnit unit) {
    this.size = size;
    this.unit = unit;
  }

  @JsonProperty
  public int getSize() {
    return size;
  }

  @JsonProperty
  public TimeUnit getUnit() {
    return unit;
  }

  public long toMillis() {
    return toMillis(1);
  }

  /**
   * Converts time in bucketed unit to millis
   *
   * @param time
   * @return
   */
  public long toMillis(long time) {
    return unit.toMillis(time * size);
  }

  /**
   * Converts millis to time unit
   *
   * e.g. If TimeGranularity is defined as 1 HOURS,
   * and we invoke convertToUnit(1458284400000) (i.e. 2016-03-18 00:00:00)
   * this method will return HOURS.convert(1458284400000, MILLISECONDS)/1 = 405079 hoursSinceEpoch
   *
   * If TimeGranularity is defined as 10 MINUTES,
   * and we invoke convertToUnit(1458284400000) (i.e. 2016-03-18 00:00:00)
   * this method will return MINUTES.convert(1458284400000, MILLISECONDS)/10 = 2430474 tenMinutesSinceEpoch
   * @param millis
   * @return
   */
  public long convertToUnit(long millis) {
    return unit.convert(millis, TimeUnit.MILLISECONDS) / size;
  }

  @Override
  public String toString() {
    return size + "-" + unit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, unit);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TimeGranularity)) {
      return false;
    }
    TimeGranularity other = (TimeGranularity) obj;
    return Objects.equals(other.size, this.size) && Objects.equals(other.unit, this.unit);
  }
}
