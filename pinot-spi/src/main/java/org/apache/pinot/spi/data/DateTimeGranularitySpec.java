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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.EnumUtils;
import org.apache.pinot.spi.utils.EqualityUtils;


/**
 * Class to represent granularity from {@link DateTimeFieldSpec}
 */
public class DateTimeGranularitySpec {

  public static final String NUMBER_REGEX = "[1-9][0-9]*";

  public static final String COLON_SEPARATOR = ":";

  /* DateTimeFieldSpec granularity is of format size:timeUnit */
  public static final int GRANULARITY_SIZE_POSITION = 0;
  public static final int GRANULARITY_UNIT_POSITION = 1;
  public static final int MAX_GRANULARITY_TOKENS = 2;

  private final String _granularity;
  private final int _size;
  private final TimeUnit _timeUnit;

  /**
   * Constructs a dateTimeGranularitySpec granularity from a string
   */
  public DateTimeGranularitySpec(String granularity) {
    validateGranularity(granularity);
    _granularity = granularity;
    String[] granularityTokens = _granularity.split(COLON_SEPARATOR);
    _size = Integer.parseInt(granularityTokens[GRANULARITY_SIZE_POSITION]);
    _timeUnit = TimeUnit.valueOf(granularityTokens[GRANULARITY_UNIT_POSITION]);
  }

  /**
   * Constructs a dateTimeGranularitySpec granularity given the components of a granularity
   */
  public DateTimeGranularitySpec(int columnSize, TimeUnit columnUnit) {
    _granularity = Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit);
    validateGranularity(_granularity);
    _size = columnSize;
    _timeUnit = columnUnit;
  }

  public String getGranularity() {
    return _granularity;
  }

  public int getSize() {
    return _size;
  }

  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  /**
   * <ul>
   * <li>Convert a granularity to millis.
   * This method should not do validation of outputGranularity.
   * The validation should be handled by caller using {@link #validateGranularity}</li>
   * <ul>
   * <li>1) granularityToMillis(1:HOURS) = 3600000 (60*60*1000)</li>
   * <li>2) granularityToMillis(1:MILLISECONDS) = 1</li>
   * <li>3) granularityToMillis(15:MINUTES) = 900000 (15*60*1000)</li>
   * </ul>
   * </ul>
   */
  public Long granularityToMillis() {
    return TimeUnit.MILLISECONDS.convert(_size, _timeUnit);
  }

  /**
   * Check correctness of granularity of {@link DateTimeFieldSpec}
   */
  public static void validateGranularity(String granularity) {
    Preconditions.checkNotNull(granularity, "Granularity string in dateTimeFieldSpec must not be null");

    String[] granularityTokens = granularity.split(COLON_SEPARATOR);
    Preconditions.checkState(granularityTokens.length == MAX_GRANULARITY_TOKENS,
        "Incorrect granularity: %s. Must be of format 'size:timeunit'", granularity);
    Preconditions.checkState(granularityTokens[GRANULARITY_SIZE_POSITION].matches(NUMBER_REGEX),
        "Incorrect granularity size: %s. Must be of format '[0-9]+:<TimeUnit>'",
        granularityTokens[GRANULARITY_SIZE_POSITION]);
    Preconditions.checkState(EnumUtils.isValidEnum(TimeUnit.class, granularityTokens[GRANULARITY_UNIT_POSITION]),
        "Incorrect granularity size: %s. Must be of format '[0-9]+:<TimeUnit>'",
        granularityTokens[GRANULARITY_SIZE_POSITION]);
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    DateTimeGranularitySpec that = (DateTimeGranularitySpec) o;

    return EqualityUtils.isEqual(_granularity, that._granularity);
  }

  @Override
  public int hashCode() {
    return EqualityUtils.hashCodeOf(_granularity);
  }
}
