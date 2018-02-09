/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.data;

import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.EnumUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Class to represent granularity from {@link DateTimeFieldSpec}
 */
public class DateTimeGranularitySpec {

  public static final String GRANULARITY_TOKENS_ERROR_STR =
      "granularity must be of format size:timeunit";
  public static final String GRANULARITY_PATTERN_ERROR_STR =
      "granularity must be of format [0-9]+:<TimeUnit>";
  public static final String NUMBER_REGEX = "[1-9][0-9]*";

  public static final String COLON_SEPARATOR = ":";

  /* DateTimeFieldSpec granularity is of format size:timeUnit */
  public static final int GRANULARITY_SIZE_POSITION = 0;
  public static final int GRANULARITY_UNIT_POSITION = 1;
  public static final int MAX_GRANULARITY_TOKENS = 2;

  private String _granularity;

  public DateTimeGranularitySpec(String granularity) {
    _granularity = granularity;
    isValidGranularity(granularity);
  }

  /**
   * Constructs a dateTimeSpec granularity given the components of a granularity
   * @param columnSize
   * @param columnUnit
   * @return
   */
  public DateTimeGranularitySpec(int columnSize, TimeUnit columnUnit) {
    _granularity = Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit);
    isValidGranularity(_granularity);
  }

  public String getGranularity() {
    return _granularity;
  }


  /**
   * <ul>
   * <li>Convert a granularity to millis.
   * This method should not do validation of outputGranularity.
   * The validation should be handled by caller using {@link #isValidGranularity(String)}</li>
   * <ul>
   * <li>1) granularityToMillis(1:HOURS) = 3600000 (60*60*1000)</li>
   * <li>2) granularityToMillis(1:MILLISECONDS) = 1</li>
   * <li>3) granularityToMillis(15:MINUTES) = 900000 (15*60*1000)</li>
   * </ul>
   * </ul>
   * @return
   */
  public Long granularityToMillis() {

    long granularityInMillis = 0;

    String[] granularityTokens = _granularity.split(COLON_SEPARATOR);
    if (granularityTokens.length == MAX_GRANULARITY_TOKENS) {
      granularityInMillis =
          TimeUnit.MILLISECONDS.convert(
              Integer.valueOf(granularityTokens[GRANULARITY_SIZE_POSITION]),
              TimeUnit.valueOf(granularityTokens[GRANULARITY_UNIT_POSITION]));
    }
    return granularityInMillis;
  }


  /**
   * Check correctness of granularity of {@link DateTimeFieldSpec}
   * @param granularity
   * @return
   */
  public static boolean isValidGranularity(String granularity) {
    Preconditions.checkNotNull(granularity);
    String[] granularityTokens = granularity.split(COLON_SEPARATOR);
    Preconditions.checkArgument(granularityTokens.length == MAX_GRANULARITY_TOKENS,
        GRANULARITY_TOKENS_ERROR_STR);
    Preconditions.checkArgument(granularityTokens[GRANULARITY_SIZE_POSITION].matches(NUMBER_REGEX), GRANULARITY_PATTERN_ERROR_STR);
    Preconditions.checkArgument(EnumUtils.isValidEnum(TimeUnit.class, granularityTokens[GRANULARITY_UNIT_POSITION]),
        GRANULARITY_PATTERN_ERROR_STR);

    return true;
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
    int result = EqualityUtils.hashCodeOf(_granularity);
    return result;
  }
}
