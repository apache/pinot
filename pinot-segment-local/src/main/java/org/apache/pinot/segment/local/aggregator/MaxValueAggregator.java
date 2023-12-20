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


public class MaxValueAggregator implements ValueAggregator<Number, Double> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.DOUBLE;
  public static final AggregationFunctionType AGGREGATION_FUNCTION_TYPE = AggregationFunctionType.MAX;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AGGREGATION_FUNCTION_TYPE;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public Double getInitialAggregatedValue(Number rawValue) {
    return rawValue.doubleValue();
  }

  @Override
  public Double applyRawValue(Double value, Number rawValue) {
    return Math.max(value, rawValue.doubleValue());
  }

  @Override
  public Double applyAggregatedValue(Double value, Double aggregatedValue) {
    return Math.max(value, aggregatedValue);
  }

  @Override
  public Double cloneAggregatedValue(Double value) {
    return value;
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return Double.BYTES;
  }

  @Override
  public byte[] serializeAggregatedValue(Double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Double deserializeAggregatedValue(byte[] bytes) {
    throw new UnsupportedOperationException();
  }
}
