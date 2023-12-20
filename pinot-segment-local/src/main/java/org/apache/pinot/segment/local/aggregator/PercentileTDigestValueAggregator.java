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

import com.tdunning.math.stats.TDigest;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class PercentileTDigestValueAggregator implements ValueAggregator<Object, TDigest> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;
  public static final AggregationFunctionType AGGREGATION_FUNCTION_TYPE = AggregationFunctionType.PERCENTILETDIGEST;

  // TODO: This is copied from PercentileTDigestAggregationFunction.
  public static final int DEFAULT_TDIGEST_COMPRESSION = 100;

  private int _maxByteSize;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AGGREGATION_FUNCTION_TYPE;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public TDigest getInitialAggregatedValue(Object rawValue) {
    TDigest initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else {
      initialValue = TDigest.createMergingDigest(DEFAULT_TDIGEST_COMPRESSION);
      initialValue.add(((Number) rawValue).doubleValue());
      _maxByteSize = Math.max(_maxByteSize, initialValue.byteSize());
    }
    return initialValue;
  }

  @Override
  public TDigest applyRawValue(TDigest value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      value.add(deserializeAggregatedValue((byte[]) rawValue));
    } else {
      value.add(((Number) rawValue).doubleValue());
    }
    _maxByteSize = Math.max(_maxByteSize, value.byteSize());
    return value;
  }

  @Override
  public TDigest applyAggregatedValue(TDigest value, TDigest aggregatedValue) {
    value.add(aggregatedValue);
    _maxByteSize = Math.max(_maxByteSize, value.byteSize());
    return value;
  }

  @Override
  public TDigest cloneAggregatedValue(TDigest value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(TDigest value) {
    return CustomSerDeUtils.TDIGEST_SER_DE.serialize(value);
  }

  @Override
  public TDigest deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.TDIGEST_SER_DE.deserialize(bytes);
  }
}
