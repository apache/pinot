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

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.local.utils.UltraLogLogUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountULLValueAggregator implements ValueAggregator<Object, UltraLogLog> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private final int _p;

  public DistinctCountULLValueAggregator(List<ExpressionContext> arguments) {
    // length 0 means we use the default _p
    if (arguments.size() == 1) {
      _p = arguments.get(0).getLiteral().getIntValue();
    } else {
      _p = CommonConstants.Helix.DEFAULT_ULTRALOGLOG_P;
    }
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTULL;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public UltraLogLog getInitialAggregatedValue(Object rawValue) {
    UltraLogLog initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
    } else {
      initialValue = UltraLogLog.create(_p);
      addObjectToSketch(rawValue, initialValue);
    }
    return initialValue;
  }

  @Override
  public UltraLogLog applyRawValue(UltraLogLog value, Object rawValue) {
    addObjectToSketch(rawValue, value);
    return value;
  }

  @Override
  public UltraLogLog applyAggregatedValue(UltraLogLog value, UltraLogLog aggregatedValue) {
    value.add(aggregatedValue);
    return value;
  }

  @Override
  public UltraLogLog cloneAggregatedValue(UltraLogLog value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return (1 << _p) + 1;
  }

  @Override
  public byte[] serializeAggregatedValue(UltraLogLog value) {
    return CustomSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.serialize(value);
  }

  @Override
  public UltraLogLog deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(bytes).downsize(_p); // downsize when we load
  }

  private void addObjectToSketch(Object rawValue, UltraLogLog sketch) {
    if (rawValue instanceof byte[]) {
      sketch.add(deserializeAggregatedValue((byte[]) rawValue));
    } else if (rawValue instanceof Object[]) {
      addObjectsToSketch((Object[]) rawValue, sketch);
    } else {
      UltraLogLogUtils.hashObject(rawValue).ifPresent(sketch::add);
    }
  }

  private void addObjectsToSketch(Object[] rawValues, UltraLogLog sketch) {
    for (Object rawValue : rawValues) {
      addObjectToSketch(rawValue, sketch);
    }
  }
}
