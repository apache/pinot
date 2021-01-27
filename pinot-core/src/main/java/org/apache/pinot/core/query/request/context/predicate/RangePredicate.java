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
package org.apache.pinot.core.query.request.context.predicate;

import java.math.BigDecimal;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.CommonConstants.Query.Range;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Predicate for RANGE.
 * <p>Pinot uses RANGE to represent '>', '>=', '<', '<=', BETWEEN so that intersection of multiple ranges can be merged.
 */
public class RangePredicate extends BasePredicate implements Predicate {
  private static BigDecimal INT_MIN_VALUE = BigDecimal.valueOf(Integer.MIN_VALUE);
  private static BigDecimal INT_MAX_VALUE = BigDecimal.valueOf(Integer.MAX_VALUE);

  private static BigDecimal LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE);
  private static BigDecimal LONG_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE);

  private static BigDecimal FLOAT_MIN_VALUE = BigDecimal.valueOf(-Float.MAX_VALUE);
  private static BigDecimal FLOAT_MAX_VALUE = BigDecimal.valueOf(Float.MAX_VALUE);

  private static BigDecimal DOUBLE_MIN_VALUE = BigDecimal.valueOf(-Double.MAX_VALUE);
  private static BigDecimal DOUBLE_MAX_VALUE = BigDecimal.valueOf(Double.MAX_VALUE);

  public static final char DELIMITER = Range.DELIMITER;
  public static final char LOWER_EXCLUSIVE = Range.LOWER_EXCLUSIVE;
  public static final char LOWER_INCLUSIVE = Range.LOWER_INCLUSIVE;
  public static final char UPPER_EXCLUSIVE = Range.UPPER_EXCLUSIVE;
  public static final char UPPER_INCLUSIVE = Range.UPPER_INCLUSIVE;
  public static final String UNBOUNDED = Range.UNBOUNDED;

  // For backward-compatibility
  private static final String LEGACY_DELIMITER = "\t\t";

  private final ExpressionContext _lhs;
  private boolean _lowerInclusive;
  private String _lowerBound;
  private boolean _upperInclusive;
  private String _upperBound;

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
    _lhs = lhs;
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
  }

  public RangePredicate(ExpressionContext lhs, boolean lowerInclusive, String lowerBound, boolean upperInclusive,
      String upperBound) {
    _lhs = lhs;
    _lowerInclusive = lowerInclusive;
    _lowerBound = lowerBound;
    _upperInclusive = upperInclusive;
    _upperBound = upperBound;
  }

  @Override
  public Type getType() {
    return Type.RANGE;
  }

  @Override
  public ExpressionContext getLhs() {
    return _lhs;
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

  /**
   * If the input value is greater than Integer.MAX_VALUE, return Integer.MAX_VALUE. If the input value is less than
   * Integer.MIN_VALUE return Integer.MIN_VALUE. Otherwise, convert the input value to an Integer value.
   */
  private static BigDecimal toInt(BigDecimal value) {
    if (value.compareTo(INT_MAX_VALUE) > 0) {
      return INT_MAX_VALUE;
    } else if (value.compareTo(INT_MIN_VALUE) < 0) {
      return INT_MIN_VALUE;
    } else {
      return new BigDecimal(value.intValue());
    }
  }

  /**
   * If the input value is greater than Long.MAX_VALUE, return Long.MAX_VALUE. If the input value is less than
   * Long.MIN_VALUE return Long.MIN_VALUE. Otherwise, convert the input value to an Integer value.
   */
  private static BigDecimal toLong(BigDecimal value) {
    if (value.compareTo(LONG_MAX_VALUE) > 0) {
      return LONG_MAX_VALUE;
    } else if (value.compareTo(LONG_MIN_VALUE) < 0) {
      return LONG_MIN_VALUE;
    } else {
      return new BigDecimal(value.longValue());
    }
  }

  /**
   * If the input value is greater than Float.MAX_VALUE, return Float.MAX_VALUE. If the input value is less than
   * -Float.MAX_VALUE return -Float.MAX_VALUE. Otherwise, convert the input value to an Integer value.
   */
  private static BigDecimal toFloat(BigDecimal value) {
    if (value.compareTo(FLOAT_MAX_VALUE) > 0) {
      return FLOAT_MAX_VALUE;
    } else if (value.compareTo(FLOAT_MIN_VALUE) < 0) {
      return FLOAT_MIN_VALUE;
    } else {
      return new BigDecimal(String.valueOf(value.floatValue()));
    }
  }

  /**
   * If the input value is greater than Double.MAX_VALUE, return Double.MAX_VALUE. If the input value is less than
   * -Double.MAX_VALUE return -Double.MAX_VALUE. Otherwise, convert the input value to an Integer value.
   */
  private static BigDecimal toDouble(BigDecimal value) {
    if (value.compareTo(DOUBLE_MAX_VALUE) > 0) {
      return DOUBLE_MAX_VALUE;
    } else if (value.compareTo(DOUBLE_MIN_VALUE) < 0) {
      return DOUBLE_MIN_VALUE;
    } else {
      return new BigDecimal(String.valueOf(value.doubleValue()));
    }
  }

  /**
   * Convert the input value to the specified datatype. If the input value is greater than the maximum value of the
   * data type, return the maximum value. If the input value is less than the minimum value of the data type, return
   * the minimum value. Otherwise, convert input value to the given data type.
   */
  private static BigDecimal convertValue(BigDecimal original, DataType dataType) {
    switch (dataType) {
      case INT:
        return toInt(original);
      case LONG:
        return toLong(original);
      case FLOAT:
        return toFloat(original);
      case DOUBLE:
        return toDouble(original);
      default:
        return original;
    }
  }

  @Override
  public void rewrite(DataType dataType) {
    // Consider a predicate where an integer column is being compared to a double literal. This predicate will be
    // rewritten as specified below.
    // RANGE PREDICATE
    //     intColumn > 12.1    rewritten to 	intColumn > 12
    //     intColumn >= 12.1   rewritten to   intColumn > 12
    //     intColumn > -12.1   rewritten to   intColumn >= -12
    //     intColumn >= -12.1  rewritten to   intColumn >= -12
    //
    //     intColumn < 12.1    rewritten to   intColumn <= 12
    //     intColumn <= 12.1   rewritten to   intColumn <= 12
    //     intColumn < -12.1   rewritten to   intColumn < -12
    //     intColumn <= -12.1  rewritten to   intColumn < -12
    //
    // The same logic applies to value of any numerical type.
    //
    if (!_lowerBound.equals(RangePredicate.UNBOUNDED)) {
      BigDecimal lowerBound = new BigDecimal(_lowerBound);
      BigDecimal lowerConvertedValue = convertValue(lowerBound, dataType);

      int compared = lowerBound.compareTo(lowerConvertedValue);
      if (compared != 0) {
        //If value after conversion is less than original value, upper bound has to be exclusive; otherwise,
        //upper bound has to be inclusive.
        _lowerInclusive = !(compared > 0);
      }

      _lowerBound = String.valueOf(lowerConvertedValue);
    }

    if (!_upperBound.equals(RangePredicate.UNBOUNDED)) {
      BigDecimal upperBound = new BigDecimal(_upperBound);
      BigDecimal upperConvertedValue = convertValue(upperBound, dataType);

      int compared = upperBound.compareTo(upperConvertedValue);
      if (compared != 0) {
        //If value after conversion is less than original value, upper bound has to be inclusive; otherwise,
        //upper bound has to be exclusive.
        _upperInclusive = compared > 0;
      }

      _upperBound = String.valueOf(upperConvertedValue);
    }
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
