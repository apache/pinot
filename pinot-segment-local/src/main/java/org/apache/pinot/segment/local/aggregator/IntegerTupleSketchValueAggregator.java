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

import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.Union;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class IntegerTupleSketchValueAggregator implements ValueAggregator<byte[], Sketch<IntegerSummary>> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;
  public static final AggregationFunctionType AGGREGATION_FUNCTION_TYPE =
      AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH;

  // This changes a lot similar to the Bitmap aggregator
  private int _maxByteSize;

  private final IntegerSummary.Mode _mode;

  public IntegerTupleSketchValueAggregator(IntegerSummary.Mode mode) {
    _mode = mode;
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AGGREGATION_FUNCTION_TYPE;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  // Utility method to merge two sketches
  private Sketch<IntegerSummary> union(Sketch<IntegerSummary> a, Sketch<IntegerSummary> b) {
    return new Union<>(new IntegerSummarySetOperations(_mode, _mode)).union(a, b);
  }

  @Override
  public Sketch<IntegerSummary> getInitialAggregatedValue(byte[] rawValue) {
    Sketch<IntegerSummary> initialValue = deserializeAggregatedValue(rawValue);
    _maxByteSize = Math.max(_maxByteSize, rawValue.length);
    return initialValue;
  }

  @Override
  public Sketch<IntegerSummary> applyRawValue(Sketch<IntegerSummary> value, byte[] rawValue) {
    Sketch<IntegerSummary> right = deserializeAggregatedValue(rawValue);
    Sketch<IntegerSummary> result = union(value, right).compact();
    _maxByteSize = Math.max(_maxByteSize, result.toByteArray().length);
    return result;
  }

  @Override
  public Sketch<IntegerSummary> applyAggregatedValue(Sketch<IntegerSummary> value,
      Sketch<IntegerSummary> aggregatedValue) {
    Sketch<IntegerSummary> result = union(value, aggregatedValue);
    _maxByteSize = Math.max(_maxByteSize, result.toByteArray().length);
    return result;
  }

  @Override
  public Sketch<IntegerSummary> cloneAggregatedValue(Sketch<IntegerSummary> value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(Sketch<IntegerSummary> value) {
    return CustomSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(value);
  }

  @Override
  public Sketch<IntegerSummary> deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(bytes);
  }
}
