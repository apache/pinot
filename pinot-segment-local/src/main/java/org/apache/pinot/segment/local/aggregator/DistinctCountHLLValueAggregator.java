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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountHLLValueAggregator implements ValueAggregator<Object, HyperLogLog> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;
  private static final int DEFAULT_LOG2M_BYTE_SIZE = 180;

  // Byte size won't change once we get the initial aggregated value
  private int _maxByteSize;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTHLL;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public HyperLogLog getInitialAggregatedValue(Object rawValue) {
    HyperLogLog initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else {
      // TODO: Handle configurable log2m for StarTreeBuilder
      initialValue = new HyperLogLog(CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M);
      initialValue.offer(rawValue);
      _maxByteSize = Math.max(_maxByteSize, DEFAULT_LOG2M_BYTE_SIZE);
    }
    return initialValue;
  }

  @Override
  public HyperLogLog applyRawValue(HyperLogLog value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      try {
        value.addAll(deserializeAggregatedValue((byte[]) rawValue));
      } catch (CardinalityMergeException e) {
        throw new RuntimeException(e);
      }
    } else {
      value.offer(rawValue);
    }
    return value;
  }

  @Override
  public HyperLogLog applyAggregatedValue(HyperLogLog value, HyperLogLog aggregatedValue) {
    try {
      value.addAll(aggregatedValue);
      return value;
    } catch (CardinalityMergeException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HyperLogLog cloneAggregatedValue(HyperLogLog value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(HyperLogLog value) {
    return CustomSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(value);
  }

  @Override
  public HyperLogLog deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytes);
  }
}
