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
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountHLLPlusPlusValueAggregator implements ValueAggregator<Object, HyperLogLogPlus> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

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
  public HyperLogLogPlus getInitialAggregatedValue(Object rawValue) {
    HyperLogLogPlus initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else {
      initialValue = new HyperLogLogPlus(CommonConstants.Helix.DEFAULT_HYPERLOGLOGPLUSPLUS_NORMAL_PRECISION,
          CommonConstants.Helix.DEFAULT_HYPERLOGLOGPLUSPLUS_SPARSE_PRECISION);
      initialValue.offer(rawValue);
      _maxByteSize = Math.max(_maxByteSize, initialValue.sizeof());
    }
    return initialValue;
  }

  @Override
  public HyperLogLogPlus applyRawValue(HyperLogLogPlus value, Object rawValue) {
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
  public HyperLogLogPlus applyAggregatedValue(HyperLogLogPlus value, HyperLogLogPlus aggregatedValue) {
    try {
      value.addAll(aggregatedValue);
      return value;
    } catch (CardinalityMergeException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HyperLogLogPlus cloneAggregatedValue(HyperLogLogPlus value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(HyperLogLogPlus value) {
    return CustomSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.serialize(value);
  }

  @Override
  public HyperLogLogPlus deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(bytes);
  }
}
