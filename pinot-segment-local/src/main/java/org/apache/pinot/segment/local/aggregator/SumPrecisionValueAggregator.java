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

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;


public class SumPrecisionValueAggregator implements ValueAggregator<Object, BigDecimal> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private int _maxByteSize;
  private int _fixedSize = -1;

  public SumPrecisionValueAggregator() {
  }

  /*
    Aggregate with a optimal maximum precision in mind. Scale is always only 1 32-bit
    int and the storing of the scale value does not affect the size of the big decimal.
    Given this, we won't care about scale in terms of the aggregations.
    During query time, the optional scale parameter can be provided, but during aggregation,
    we don't limit it.
   */
  public SumPrecisionValueAggregator(List<ExpressionContext> arguments) {
    // length 1 means we don't have any caps on maximum precision nor do we have a fixed size then
    if (arguments.size() <= 1) {
      return;
    }

    _fixedSize = BigDecimalUtils.byteSizeForFixedPrecision(arguments.get(1).getLiteral().getIntValue());
  }

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
    if (_fixedSize > 0) {
      return _fixedSize;
    }
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(BigDecimal value) {
    if (_fixedSize > 0) {
      return BigDecimalUtils.serializeWithSize(value, _fixedSize);
    }
    return BigDecimalUtils.serialize(value);
  }

  @Override
  public BigDecimal deserializeAggregatedValue(byte[] bytes) {
    return BigDecimalUtils.deserialize(bytes);
  }
}
