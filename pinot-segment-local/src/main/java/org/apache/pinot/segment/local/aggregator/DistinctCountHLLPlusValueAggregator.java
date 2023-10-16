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
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.local.utils.HyperLogLogPlusUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountHLLPlusValueAggregator implements ValueAggregator<Object, HyperLogLogPlus> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private final int _p;
  private final int _sp;

  // Byte size won't change once we get the initial aggregated value
  private int _maxByteSize;

  public DistinctCountHLLPlusValueAggregator(List<ExpressionContext> arguments) {
    // length 1 means we use the default _p and _sp
    if (arguments.size() == 2) {
      _p = arguments.get(1).getLiteral().getIntValue();
      _sp = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_SP;
    } else if (arguments.size() == 3) {
      _p = arguments.get(1).getLiteral().getIntValue();
      _sp = arguments.get(2).getLiteral().getIntValue();
    } else {
      _p = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_P;
      _sp = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_SP;
    }
  }

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
      _maxByteSize = bytes.length;
    } else {
      initialValue = new HyperLogLogPlus(_p, _sp);
      initialValue.offer(rawValue);
      _maxByteSize = HyperLogLogPlusUtils.byteSize(_p, _sp);
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
    // NOTE: For aggregated metrics, initial aggregated value might have not been generated. Returns the byte size
    //       based on p and sp.
    return _maxByteSize > 0 ? _maxByteSize : HyperLogLogPlusUtils.byteSize(_p, _sp);
  }

  @Override
  public byte[] serializeAggregatedValue(HyperLogLogPlus value) {
    return CustomSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.serialize(value);
  }

  @Override
  public HyperLogLogPlus deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.deserialize(bytes);
  }
}
