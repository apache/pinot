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
package org.apache.pinot.core.data.aggregator;

import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionType;
import org.apache.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.customobject.MinMaxRangePair;


public class MinMaxRangeValueAggregator implements ValueAggregator<Object, MinMaxRangePair> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.MINMAXRANGE;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public MinMaxRangePair getInitialAggregatedValue(Object rawValue) {
    if (rawValue instanceof byte[]) {
      // Use default value for zero-length byte array
      byte[] bytes = (byte[]) rawValue;
      if (bytes.length != 0) {
        return deserializeAggregatedValue(bytes);
      } else {
        return MinMaxRangeAggregationFunction.getDefaultMinMaxRangePair();
      }
    } else {
      double doubleValue = ((Number) rawValue).doubleValue();
      return new MinMaxRangePair(doubleValue, doubleValue);
    }
  }

  @Override
  public MinMaxRangePair applyRawValue(MinMaxRangePair value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      // Skip zero-length byte array
      byte[] bytes = (byte[]) rawValue;
      if (bytes.length != 0) {
        value.apply(deserializeAggregatedValue(bytes));
      }
    } else {
      double doubleValue = ((Number) rawValue).doubleValue();
      value.apply(doubleValue, doubleValue);
    }
    return value;
  }

  @Override
  public MinMaxRangePair applyAggregatedValue(MinMaxRangePair value, MinMaxRangePair aggregatedValue) {
    value.apply(aggregatedValue);
    return value;
  }

  @Override
  public MinMaxRangePair cloneAggregatedValue(MinMaxRangePair value) {
    return new MinMaxRangePair(value.getMin(), value.getMax());
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return Double.BYTES + Double.BYTES;
  }

  @Override
  public byte[] serializeAggregatedValue(MinMaxRangePair value) {
    return ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.serialize(value);
  }

  @Override
  public MinMaxRangePair deserializeAggregatedValue(byte[] bytes) {
    return ObjectSerDeUtils.MIN_MAX_RANGE_PAIR_SER_DE.deserialize(bytes);
  }
}
