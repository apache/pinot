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

import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


public class DistinctCountBitmapValueAggregator implements ValueAggregator<Object, RoaringBitmap> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;
  public static final AggregationFunctionType AGGREGATION_FUNCTION_TYPE = AggregationFunctionType.DISTINCTCOUNTBITMAP;

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
  public RoaringBitmap getInitialAggregatedValue(Object rawValue) {
    RoaringBitmap initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else {
      initialValue = new RoaringBitmap();
      initialValue.add(rawValue.hashCode());
      _maxByteSize = Math.max(_maxByteSize, initialValue.serializedSizeInBytes());
    }
    return initialValue;
  }

  @Override
  public RoaringBitmap applyRawValue(RoaringBitmap value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      value.or(deserializeAggregatedValue((byte[]) rawValue));
    } else {
      value.add(rawValue.hashCode());
    }
    _maxByteSize = Math.max(_maxByteSize, value.serializedSizeInBytes());
    return value;
  }

  @Override
  public RoaringBitmap applyAggregatedValue(RoaringBitmap value, RoaringBitmap aggregatedValue) {
    value.or(aggregatedValue);
    _maxByteSize = Math.max(_maxByteSize, value.serializedSizeInBytes());
    return value;
  }

  @Override
  public RoaringBitmap cloneAggregatedValue(RoaringBitmap value) {
    return value.clone();
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(RoaringBitmap value) {
    return RoaringBitmapUtils.serialize(value);
  }

  @Override
  public RoaringBitmap deserializeAggregatedValue(byte[] bytes) {
    return RoaringBitmapUtils.deserialize(bytes);
  }
}
