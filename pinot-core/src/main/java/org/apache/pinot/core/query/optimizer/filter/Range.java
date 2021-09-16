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
package org.apache.pinot.core.query.optimizer.filter;

import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.request.context.predicate.RangePredicate;


/**
 * Helper class to represent a value range.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Range {
  private Comparable _lowerBound;
  private boolean _lowerInclusive;
  private Comparable _upperBound;
  private boolean _upperInclusive;

  public Range(@Nullable Comparable lowerBound, boolean lowerInclusive, @Nullable Comparable upperBound,
      boolean upperInclusive) {
    _lowerBound = lowerBound;
    _lowerInclusive = lowerInclusive;
    _upperBound = upperBound;
    _upperInclusive = upperInclusive;
  }

  /**
   * Intersects the current range with another range.
   */
  public void intersect(Range range) {
    if (range._lowerBound != null) {
      if (_lowerBound == null) {
        _lowerInclusive = range._lowerInclusive;
        _lowerBound = range._lowerBound;
      } else {
        int result = _lowerBound.compareTo(range._lowerBound);
        if (result < 0) {
          _lowerBound = range._lowerBound;
          _lowerInclusive = range._lowerInclusive;
        } else if (result == 0) {
          _lowerInclusive &= range._lowerInclusive;
        }
      }
    }
    if (range._upperBound != null) {
      if (_upperBound == null) {
        _upperInclusive = range._upperInclusive;
        _upperBound = range._upperBound;
      } else {
        int result = _upperBound.compareTo(range._upperBound);
        if (result > 0) {
          _upperBound = range._upperBound;
          _upperInclusive = range._upperInclusive;
        } else if (result == 0) {
          _upperInclusive &= range._upperInclusive;
        }
      }
    }
  }

  /**
   * Returns the string representation of the range. See {@link RangePredicate} for details.
   */
  public String getRangeString() {
    StringBuilder stringBuilder = new StringBuilder();
    if (_lowerBound == null) {
      stringBuilder.append(RangePredicate.LOWER_EXCLUSIVE).append(RangePredicate.UNBOUNDED);
    } else {
      stringBuilder.append(_lowerInclusive ? RangePredicate.LOWER_INCLUSIVE : RangePredicate.LOWER_EXCLUSIVE);
      stringBuilder.append(_lowerBound);
    }
    stringBuilder.append(RangePredicate.DELIMITER);
    if (_upperBound == null) {
      stringBuilder.append(RangePredicate.UNBOUNDED).append(RangePredicate.UPPER_EXCLUSIVE);
    } else {
      stringBuilder.append(_upperBound);
      stringBuilder.append(_upperInclusive ? RangePredicate.UPPER_INCLUSIVE : RangePredicate.UPPER_EXCLUSIVE);
    }
    return stringBuilder.toString();
  }

  /**
   * Creates a Range from the given string representation of the range and data type. See {@link RangePredicate} for
   * details.
   */
  public static Range getRange(String rangeString, DataType dataType) {
    String[] split = StringUtils.split(rangeString, RangePredicate.DELIMITER);
    String lower = split[0];
    boolean lowerInclusive = lower.charAt(0) == RangePredicate.LOWER_INCLUSIVE;
    String stringLowerBound = lower.substring(1);
    Comparable lowerBound =
        stringLowerBound.equals(RangePredicate.UNBOUNDED) ? null : dataType.convertInternal(stringLowerBound);
    String upper = split[1];
    int upperLength = upper.length();
    boolean upperInclusive = upper.charAt(upperLength - 1) == RangePredicate.UPPER_INCLUSIVE;
    String stringUpperBound = upper.substring(0, upperLength - 1);
    Comparable upperBound =
        stringUpperBound.equals(RangePredicate.UNBOUNDED) ? null : dataType.convertInternal(stringUpperBound);
    return new Range(lowerBound, lowerInclusive, upperBound, upperInclusive);
  }
}
