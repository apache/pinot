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


public class MinValueAggregator implements ValueAggregator<Number, Number> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.DOUBLE;
  private DataType _dataType;

  public MinValueAggregator(DataType dataType) {
    _dataType = dataType;
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.MIN;
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
    return getMinimum(value, rawValue);
  }

  @Override
  public Number applyAggregatedValue(Number value, Number aggregatedValue) {
    return getMinimum(value, aggregatedValue);
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

  private Number getMinimum(Number value1, Number value2) {
    Number result;
    switch (_dataType) {
      case INT:
        result = Math.min(value1.intValue(), value2.intValue());
        break;
      case LONG:
        result = Math.min(value1.longValue(), value2.longValue());
        break;
      case FLOAT:
        result = Math.min(value1.floatValue(), value2.floatValue());
        break;
      case DOUBLE:
        result = Math.min(value1.doubleValue(), value2.doubleValue());
        break;
      default:
        throw new IllegalArgumentException("Unsupported metric type : " + _dataType);
    }
    return result;
  }
}
