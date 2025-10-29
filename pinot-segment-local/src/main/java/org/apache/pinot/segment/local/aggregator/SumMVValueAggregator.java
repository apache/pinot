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
package org.apache.pinot.segment.local.aggregator;

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.AggregationFunctionType;

/**
 * Value aggregator for SUMMV aggregation function.
 * This aggregator handles multi-value columns by summing all values in all arrays.
 */
public class SumMVValueAggregator extends SumValueAggregator {

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.SUMMV;
  }

  @Override
  public Double getInitialAggregatedValue(@Nullable Object rawValue) {
    if (rawValue == null) {
      return 0.0;
    }
    return processMultiValueArray(rawValue);
  }

  @Override
  public Double applyRawValue(Double value, Object rawValue) {
    if (rawValue == null) {
      return value;
    }
    return value + processMultiValueArray(rawValue);
  }

  /**
   * Processes a multi-value array and returns the sum of all values.
   * The rawValue can be an Object[] array containing numeric values.
   */
  private Double processMultiValueArray(Object rawValue) {
    if (rawValue instanceof Object[]) {
      Object[] values = (Object[]) rawValue;
      double sum = 0.0;
      for (Object value : values) {
        if (value != null) {
          sum += ValueAggregatorUtils.toDouble(value);
        }
      }
      return sum;
    } else {
      return ValueAggregatorUtils.toDouble(rawValue);
    }
  }
}
