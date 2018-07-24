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
  private final AggregationFunctionType _functionType;
  private final String _columnName;

  public AggregationFunctionColumnPair(@Nonnull AggregationFunctionType functionType, @Nonnull String columnName) {
    _functionType = functionType;
    _columnName = columnName;
  }

  @Nonnull
  public AggregationFunctionType getFunctionType() {
    return _functionType;
  }

  @Nonnull
  public String getColumnName() {
    return _columnName;
  }

  @Override
  public int hashCode() {
    return 31 * _functionType.hashCode() + _columnName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof AggregationFunctionColumnPair) {
      AggregationFunctionColumnPair anotherPair = (AggregationFunctionColumnPair) obj;
      return _functionType == anotherPair._functionType && _columnName.equals(anotherPair._columnName);
    }
    return false;
  }
}
