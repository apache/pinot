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
package com.linkedin.pinot.core.startree.v2;

import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import javax.annotation.Nonnull;


public class AggregationFunctionColumnPair {
  public static final String DELIMITER = "__";
  public static final String STAR = "*";
  public static final AggregationFunctionColumnPair COUNT_STAR =
      new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, STAR);
  public static final String COUNT_STAR_COLUMN_NAME = COUNT_STAR.toColumnName();

  private final AggregationFunctionType _functionType;
  private final String _column;

  public AggregationFunctionColumnPair(@Nonnull AggregationFunctionType functionType, @Nonnull String column) {
    _functionType = functionType;
    if (functionType == AggregationFunctionType.COUNT) {
      _column = STAR;
    } else {
      _column = column;
    }
  }

  @Nonnull
  public AggregationFunctionType getFunctionType() {
    return _functionType;
  }

  @Nonnull
  public String getColumn() {
    return _column;
  }

  @Nonnull
  public String toColumnName() {
    return _functionType.getName() + DELIMITER + _column;
  }

  @Nonnull
  public static AggregationFunctionColumnPair fromColumnName(@Nonnull String columnName) {
    String[] parts = columnName.split(DELIMITER, 2);
    AggregationFunctionType functionType = AggregationFunctionType.valueOf(parts[0].toUpperCase());
    if (functionType == AggregationFunctionType.COUNT) {
      return COUNT_STAR;
    } else {
      return new AggregationFunctionColumnPair(functionType, parts[1]);
    }
  }

  @Override
  public int hashCode() {
    return 31 * _functionType.hashCode() + _column.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof AggregationFunctionColumnPair) {
      AggregationFunctionColumnPair anotherPair = (AggregationFunctionColumnPair) obj;
      return _functionType == anotherPair._functionType && _column.equals(anotherPair._column);
    }
    return false;
  }
}
