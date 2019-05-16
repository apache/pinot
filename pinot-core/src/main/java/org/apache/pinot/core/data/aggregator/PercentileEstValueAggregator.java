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
import org.apache.pinot.core.query.aggregation.function.PercentileEstAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.customobject.QuantileDigest;


public class PercentileEstValueAggregator implements ValueAggregator<Object, QuantileDigest> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private int _maxByteSize;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.PERCENTILEEST;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public QuantileDigest getInitialAggregatedValue(Object rawValue) {
    QuantileDigest initialValue;
    if (rawValue instanceof byte[]) {
      // Use default value for zero-length byte array
      byte[] bytes = (byte[]) rawValue;
      if (bytes.length != 0) {
        initialValue = deserializeAggregatedValue(bytes);
        _maxByteSize = Math.max(_maxByteSize, bytes.length);
      } else {
        initialValue = PercentileEstAggregationFunction.getDefaultQuantileDigest();
        _maxByteSize = Math.max(_maxByteSize, initialValue.getByteSize());
      }
    } else {
      initialValue = PercentileEstAggregationFunction.getDefaultQuantileDigest();
      initialValue.add(((Number) rawValue).longValue());
      _maxByteSize = Math.max(_maxByteSize, initialValue.getByteSize());
    }
    return initialValue;
  }

  @Override
  public QuantileDigest applyRawValue(QuantileDigest value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      // Skip zero-length byte array
      byte[] bytes = (byte[]) rawValue;
      if (bytes.length != 0) {
        value.merge(deserializeAggregatedValue(bytes));
        _maxByteSize = Math.max(_maxByteSize, value.getByteSize());
      }
    } else {
      value.add(((Number) rawValue).longValue());
      _maxByteSize = Math.max(_maxByteSize, value.getByteSize());
    }
    return value;
  }

  @Override
  public QuantileDigest applyAggregatedValue(QuantileDigest value, QuantileDigest aggregatedValue) {
    value.merge(aggregatedValue);
    _maxByteSize = Math.max(_maxByteSize, value.getByteSize());
    return value;
  }

  @Override
  public QuantileDigest cloneAggregatedValue(QuantileDigest value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(QuantileDigest value) {
    return ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.serialize(value);
  }

  @Override
  public QuantileDigest deserializeAggregatedValue(byte[] bytes) {
    return ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(bytes);
  }
}
