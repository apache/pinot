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
package org.apache.pinot.common.request.context.predicate;

import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;


/**
 * Predicate for RANGE.
 * <p>Pinot uses RANGE to represent '>', '>=', '<', '<=', BETWEEN so that intersection of multiple ranges can be merged.
 */
public class RangePredicate extends BasePredicate {
  public static final char DELIMITER = Range.DELIMITER;
  public static final char LOWER_EXCLUSIVE = Range.LOWER_EXCLUSIVE;
  public static final char LOWER_INCLUSIVE = Range.LOWER_INCLUSIVE;
  public static final char UPPER_EXCLUSIVE = Range.UPPER_EXCLUSIVE;
  public static final char UPPER_INCLUSIVE = Range.UPPER_INCLUSIVE;
  public static final String UNBOUNDED = Range.UNBOUNDED;

  // For backward-compatibility
  private static final String LEGACY_DELIMITER = "\t\t";

  private final boolean _lowerInclusive;
  private final String _lowerBound;
  private final boolean _upperInclusive;
  private final String _upperBound;
  private final FieldSpec.DataType _rangeDataType;

  /**
   * The range is formatted as 5 parts:
   * <ul>
   *   <li>Lower inclusive '[' or exclusive '('</li>
   *   <li>Lower bound ('*' for unbounded)</li>
   *   <li>Delimiter ('\0')</li>
   *   <li>Upper bound ('*' for unbounded)</li>
   *   <li>Upper inclusive ']' or exclusive ')'</li>
   * </ul>
   */
  public RangePredicate(ExpressionContext lhs, String range) {
    super(lhs);
    String[] split = StringUtils.split(range, DELIMITER);
    if (split.length != 2) {
      split = StringUtils.split(range, LEGACY_DELIMITER);
    }
    String lower = split[0];
    String upper = split[1];
    _lowerInclusive = lower.charAt(0) == LOWER_INCLUSIVE;
    _lowerBound = lower.substring(1);
    int upperLength = upper.length();
    _upperInclusive = upper.charAt(upperLength - 1) == UPPER_INCLUSIVE;
    _upperBound = upper.substring(0, upperLength - 1);
    _rangeDataType = null;
  }

  public RangePredicate(ExpressionContext lhs, boolean lowerInclusive, String lowerBound, boolean upperInclusive,
      String upperBound, FieldSpec.DataType rangeDataType) {
    super(lhs);
    _lowerInclusive = lowerInclusive;
    _lowerBound = lowerBound;
    _upperInclusive = upperInclusive;
    _upperBound = upperBound;
    _rangeDataType = rangeDataType;
  }

  @Override
  public Type getType() {
    return Type.RANGE;
  }

  public boolean isLowerInclusive() {
    return _lowerInclusive;
  }

  public String getLowerBound() {
    return _lowerBound;
  }

  public boolean isUpperInclusive() {
    return _upperInclusive;
  }

  public String getUpperBound() {
    return _upperBound;
  }

  public FieldSpec.DataType getRangeDataType() {
    return _rangeDataType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RangePredicate)) {
      return false;
    }
    RangePredicate that = (RangePredicate) o;
    return _lowerInclusive == that._lowerInclusive && _upperInclusive == that._upperInclusive && Objects
        .equals(_lhs, that._lhs) && Objects.equals(_lowerBound, that._lowerBound) && Objects
        .equals(_upperBound, that._upperBound);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_lhs, _lowerInclusive, _lowerBound, _upperInclusive, _upperBound);
  }

  @Override
  public String toString() {
    if (_lowerBound.equals(UNBOUNDED)) {
      return _lhs + (_upperInclusive ? " <= '" : " < '") + _upperBound + '\'';
    }
    if (_upperBound.equals(UNBOUNDED)) {
      return _lhs + (_lowerInclusive ? " >= '" : " > '") + _lowerBound + '\'';
    }
    if (_lowerInclusive && _upperInclusive) {
      return _lhs + " BETWEEN '" + _lowerBound + "' AND '" + _upperBound + '\'';
    }
    return "(" + _lhs + (_lowerInclusive ? " >= '" : " > '") + _lowerBound + "' AND " + _lhs + (_upperInclusive
        ? " <= '" : " < '") + _upperBound + "')";
  }
}
