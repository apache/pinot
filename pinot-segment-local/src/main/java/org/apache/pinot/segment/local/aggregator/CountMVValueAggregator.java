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
 * Value aggregator for COUNTMV aggregation function.
 * This aggregator handles multi-value columns by counting all values in all arrays.
 */
public class CountMVValueAggregator extends CountValueAggregator {

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.COUNTMV;
  }

  @Override
  public Long getInitialAggregatedValue(@Nullable Object rawValue) {
    if (rawValue == null) {
      return 0L;
    }
    return processMultiValueArray(rawValue);
  }

  @Override
  public Long applyRawValue(Long value, Object rawValue) {
    if (rawValue == null) {
      return value;
    }
    return value + processMultiValueArray(rawValue);
  }

  /**
   * Processes a multi-value array and returns the count of non-null values.
   * The rawValue can be an Object[] array containing values.
   */
  private Long processMultiValueArray(Object rawValue) {
    if (rawValue instanceof Object[]) {
      Object[] values = (Object[]) rawValue;
      long count = 0;
      for (Object value : values) {
        if (value != null) {
          count++;
        }
      }
      return count;
    } else {
      return 1L;
    }
  }
}
