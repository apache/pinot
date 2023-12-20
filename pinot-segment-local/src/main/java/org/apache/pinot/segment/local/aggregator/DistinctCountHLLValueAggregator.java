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
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.local.utils.HyperLogLogUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountHLLValueAggregator implements ValueAggregator<Object, HyperLogLog> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;
  public static final AggregationFunctionType AGGREGATION_FUNCTION_TYPE = AggregationFunctionType.DISTINCTCOUNTHLL;

  private final int _log2m;

  // Byte size won't change once we get the initial aggregated value
  private int _maxByteSize;

  public DistinctCountHLLValueAggregator(List<ExpressionContext> arguments) {
    // length 1 means we use the default _log2m of 8
    if (arguments.size() <= 1) {
      _log2m = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M;
    } else {
      _log2m = arguments.get(1).getLiteral().getIntValue();
    }
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AGGREGATION_FUNCTION_TYPE;
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
      _maxByteSize = bytes.length;
    } else {
      initialValue = new HyperLogLog(_log2m);
      initialValue.offer(rawValue);
      _maxByteSize = HyperLogLogUtils.byteSize(initialValue);
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
    // NOTE: For aggregated metrics, initial aggregated value might have not been generated. Returns the byte size
    //       based on log2m.
    return _maxByteSize > 0 ? _maxByteSize : HyperLogLogUtils.byteSize(_log2m);
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
