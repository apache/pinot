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

import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class AvgValueAggregator implements ValueAggregator<Object, AvgPair> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.AVG;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public AvgPair getInitialAggregatedValue(Object rawValue) {
    if (rawValue instanceof byte[]) {
      return deserializeAggregatedValue((byte[]) rawValue);
    } else {
      return new AvgPair(((Number) rawValue).doubleValue(), 1L);
    }
  }

  @Override
  public AvgPair applyRawValue(AvgPair value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      value.apply(deserializeAggregatedValue((byte[]) rawValue));
    } else {
      value.apply(((Number) rawValue).doubleValue(), 1L);
    }
    return value;
  }

  @Override
  public AvgPair applyAggregatedValue(AvgPair value, AvgPair aggregatedValue) {
    value.apply(aggregatedValue);
    return value;
  }

  @Override
  public AvgPair cloneAggregatedValue(AvgPair value) {
    return new AvgPair(value.getSum(), value.getCount());
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return Double.BYTES + Long.BYTES;
  }

  @Override
  public byte[] serializeAggregatedValue(AvgPair value) {
    return CustomSerDeUtils.AVG_PAIR_SER_DE.serialize(value);
  }

  @Override
  public AvgPair deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.AVG_PAIR_SER_DE.deserialize(bytes);
  }
}
