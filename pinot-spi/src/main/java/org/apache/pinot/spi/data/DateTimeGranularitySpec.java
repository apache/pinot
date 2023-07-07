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
package org.apache.pinot.spi.data;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.StringUtil;


/**
 * Class to represent granularity from {@link DateTimeFieldSpec}
 */
public class DateTimeGranularitySpec {
  // Colon format: 'size:timeUnit'
  private static final char COLON_SEPARATOR = ':';
  private static final int COLON_FORMAT_SIZE_POSITION = 0;
  private static final int COLON_FORMAT_TIME_UNIT_POSITION = 1;

  // Pipe format: 'timeUnit|size'
  private static final char PIPE_SEPARATOR = '|';
  private static final int PIPE_FORMAT_TIME_UNIT_POSITION = 0;
  private static final int PIPE_FORMAT_SIZE_POSITION = 1;
  private static final int PIPE_FORMAT_MIN_TOKENS = 1;
  private static final int PIPE_FORMAT_MAX_TOKENS = 2;

  private static final int NUM_TOKENS = 2;

  private final int _size;
  private final TimeUnit _timeUnit;

  /**
   * Constructs a dateTimeGranularitySpec granularity from a string
   */
  public DateTimeGranularitySpec(String granularity) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(granularity), "Must provide granularity");

    char separator;
    int sizePosition;
    int timeUnitPosition;
    int minTokens = NUM_TOKENS;
    int maxTokens = NUM_TOKENS;

    if (Character.isDigit(granularity.charAt(0))) {
      // Colon format: 'size:timeUnit'
      separator = COLON_SEPARATOR;
      sizePosition = COLON_FORMAT_SIZE_POSITION;
      timeUnitPosition = COLON_FORMAT_TIME_UNIT_POSITION;
    } else {
      // Pipe format: 'timeUnit(|size)'
      separator = PIPE_SEPARATOR;
      sizePosition = PIPE_FORMAT_SIZE_POSITION;
      timeUnitPosition = PIPE_FORMAT_TIME_UNIT_POSITION;

      minTokens = PIPE_FORMAT_MIN_TOKENS;
      maxTokens = PIPE_FORMAT_MAX_TOKENS;
    }

    String[] granularityTokens = StringUtil.split(granularity, separator, 2);
    Preconditions.checkArgument(granularityTokens.length >= minTokens && granularityTokens.length <= maxTokens,
            "Invalid granularity: %s, must be of format 'timeUnit(|size)' or 'size:timeUnit'", granularity);

    try {
      _timeUnit = TimeUnit.valueOf(granularityTokens[timeUnitPosition]);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Invalid time unit: %s in granularity: %s", granularityTokens[timeUnitPosition],
              granularity));
    }

    // New format without explicitly setting size - use default size = 1
    if (granularityTokens.length == 1) {
      _size = 1;
      return;
    }

    try {
      _size = Integer.parseInt(granularityTokens[sizePosition]);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Invalid size: %s in granularity: %s", granularityTokens[sizePosition], granularity));
    }
    Preconditions.checkArgument(_size > 0, "Invalid size: %s in granularity: %s, must be positive", _size, granularity);
  }

  /**
   * Constructs a dateTimeGranularitySpec granularity given the components of a granularity
   */
  public DateTimeGranularitySpec(int size, TimeUnit timeUnit) {
    Preconditions.checkArgument(size > 0, "Invalid size: %s, must be positive", size);
    Preconditions.checkArgument(timeUnit != null, "Must provide time unit");
    _size = size;
    _timeUnit = timeUnit;
  }

  public int getSize() {
    return _size;
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  /**
   * Converts a granularity to millis.
   *
   * Using new format:
   * <ul>
   *   <li>1) granularityToMillis(HOURS|1) = 3600000 (60*60*1000)</li>
   *   <li>2) granularityToMillis(MILLISECONDS|1) = 1</li>
   *   <li>3) granularityToMillis(MINUTES|15) = 900000 (15*60*1000)</li>
   * </ul>
   *
   * Using old format:
   * <ul>
   *   <li>1) granularityToMillis(1:HOURS) = 3600000 (60*60*1000)</li>
   *   <li>2) granularityToMillis(1:MILLISECONDS) = 1</li>
   *   <li>3) granularityToMillis(15:MINUTES) = 900000 (15*60*1000)</li>
   * </ul>
   */
  public long granularityToMillis() {
    return TimeUnit.MILLISECONDS.convert(_size, _timeUnit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DateTimeGranularitySpec that = (DateTimeGranularitySpec) o;
    return _size == that._size && _timeUnit == that._timeUnit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_size, _timeUnit);
  }
}
