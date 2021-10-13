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

import org.apache.datasketches.hll.HllSketch;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountHLLSketchValueAggregator implements ValueAggregator<Object, HllSketch> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  // Byte size won't change once we get the initial aggregated value
  private int _maxByteSize;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTHLLSKETCH;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public HllSketch getInitialAggregatedValue(Object rawValue) {
    HllSketch initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else {
      initialValue = new HllSketch(CommonConstants.Helix.DEFAULT_HYPERLOGLOG_SKETCH_LOG2K);
      extractFromRawValue(rawValue, initialValue);
      _maxByteSize = Math.max(_maxByteSize, initialValue.getUpdatableSerializationBytes());
    }
    return initialValue;
  }

  private void extractFromRawValue(Object rawValue, HllSketch hllSketch) {
    if (rawValue instanceof Long || rawValue instanceof Integer) {
      hllSketch.update((Long) rawValue);
    } else if (rawValue instanceof Double || rawValue instanceof Float) {
      hllSketch.update((Double) rawValue);
    } else if (rawValue instanceof String) {
      hllSketch.update((String) rawValue);
    } else {
      throw new IllegalStateException(
          "Illegal data type for DISTINCT_COUNT_HLL_SKETCH aggregation function: " + rawValue.getClass());
    }
  }

  @Override
  public HllSketch applyRawValue(HllSketch value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      value.update(deserializeAggregatedValue((byte[]) rawValue).toUpdatableByteArray());
    } else {
      extractFromRawValue(rawValue, value);
    }
    return value;
  }

  @Override
  public HllSketch applyAggregatedValue(HllSketch value, HllSketch aggregatedValue) {
    value.update(aggregatedValue.toUpdatableByteArray());
    return value;
  }

  @Override
  public HllSketch cloneAggregatedValue(HllSketch value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(HllSketch value) {
    return CustomSerDeUtils.HYPER_LOG_LOG_SKETCH_SER_DE.serialize(value);
  }

  @Override
  public HllSketch deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.HYPER_LOG_LOG_SKETCH_SER_DE.deserialize(bytes);
  }
}
