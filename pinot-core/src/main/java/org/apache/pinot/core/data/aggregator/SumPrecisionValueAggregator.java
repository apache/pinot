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

import java.math.BigDecimal;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;


public class SumPrecisionValueAggregator implements ValueAggregator<Object, BigDecimal> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private int _maxByteSize;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.SUMPRECISION;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public BigDecimal getInitialAggregatedValue(Object rawValue) {
    BigDecimal initialValue = toBigDecimal(rawValue);
    _maxByteSize = Math.max(_maxByteSize, BigDecimalUtils.byteSize(initialValue));
    return initialValue;
  }

  @Override
  public BigDecimal applyRawValue(BigDecimal value, Object rawValue) {
    value = value.add(toBigDecimal(rawValue));
    _maxByteSize = Math.max(_maxByteSize, BigDecimalUtils.byteSize(value));
    return value;
  }

  private static BigDecimal toBigDecimal(Object rawValue) {
    if (rawValue instanceof byte[]) {
      return BigDecimalUtils.deserialize((byte[]) rawValue);
    }
    if (rawValue instanceof Integer || rawValue instanceof Long) {
      return BigDecimal.valueOf(((Number) rawValue).longValue());
    }
    return new BigDecimal(rawValue.toString());
  }

  @Override
  public BigDecimal applyAggregatedValue(BigDecimal value, BigDecimal aggregatedValue) {
    value = value.add(aggregatedValue);
    _maxByteSize = Math.max(_maxByteSize, BigDecimalUtils.byteSize(value));
    return value;
  }

  @Override
  public BigDecimal cloneAggregatedValue(BigDecimal value) {
    // NOTE: No need to clone because BigDecimal is immutable
    return value;
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(BigDecimal value) {
    return BigDecimalUtils.serialize(value);
  }

  @Override
  public BigDecimal deserializeAggregatedValue(byte[] bytes) {
    return BigDecimalUtils.deserialize(bytes);
  }
}
