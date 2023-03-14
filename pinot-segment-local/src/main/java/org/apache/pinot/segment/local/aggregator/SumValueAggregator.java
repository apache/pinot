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

import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class SumValueAggregator implements ValueAggregator<Number, Number> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.DOUBLE;
  private final DataType _dataType;

  public SumValueAggregator(DataType dataType) {
    _dataType = dataType;
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.SUM;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public Number getInitialAggregatedValue(Number rawValue) {
    return rawValue;
  }

  @Override
  public Number applyRawValue(Number value, Number rawValue) {
    return sum(value, rawValue);
  }

  @Override
  public Number applyAggregatedValue(Number value, Number aggregatedValue) {
    return sum(value, aggregatedValue);
  }

  @Override
  public Number cloneAggregatedValue(Number value) {
    return value;
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return Double.BYTES;
  }

  @Override
  public byte[] serializeAggregatedValue(Number value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Double deserializeAggregatedValue(byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  private Number sum(Number value1, Number value2) {
    Number result;
    switch (_dataType) {
      case INT:
        result = value1.intValue() + value2.intValue();
        break;
      case LONG:
        result = value1.longValue() + value2.longValue();
        break;
      case FLOAT:
        result = value1.floatValue() + value2.floatValue();
        break;
      case DOUBLE:
        result = value1.doubleValue() + value2.doubleValue();
        break;
      default:
        throw new IllegalArgumentException("Unsupported metric type for SUM aggregator : " + AGGREGATED_VALUE_TYPE);
    }
    return result;
  }
}
