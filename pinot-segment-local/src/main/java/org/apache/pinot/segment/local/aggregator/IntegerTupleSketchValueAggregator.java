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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.Union;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


@SuppressWarnings("unchecked")
public class IntegerTupleSketchValueAggregator implements ValueAggregator<byte[], Object> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private final int _nominalEntries;

  private final IntegerSummary.Mode _mode;

  public IntegerTupleSketchValueAggregator(List<ExpressionContext> arguments, IntegerSummary.Mode mode) {
    // No argument means we use the Helix default
    if (arguments.isEmpty()) {
      _nominalEntries = 1 << CommonConstants.Helix.DEFAULT_TUPLE_SKETCH_LGK;
    } else {
      _nominalEntries = arguments.get(0).getLiteral().getIntValue();
    }
    _mode = mode;
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public Object getInitialAggregatedValue(byte[] rawValue) {
    Sketch<IntegerSummary> initialValue = deserializeAggregatedValue(rawValue);
    Union tupleUnion = new Union<>(_nominalEntries, new IntegerSummarySetOperations(_mode, _mode));
    tupleUnion.union(initialValue);
    return tupleUnion;
  }

  private Union extractUnion(Object value) {
    if (value == null) {
      return new Union<>(_nominalEntries, new IntegerSummarySetOperations(_mode, _mode));
    } else if (value instanceof Union) {
      return (Union) value;
    } else if (value instanceof Sketch) {
      Sketch sketch = (Sketch) value;
      Union tupleUnion = new Union<>(_nominalEntries, new IntegerSummarySetOperations(_mode, _mode));
      tupleUnion.union(sketch);
      return tupleUnion;
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Integer Tuple Sketch aggregation: " + value.getClass().getSimpleName());
    }
  }

  private Sketch extractSketch(Object value) {
    if (value instanceof Union) {
      return ((Union) value).getResult();
    } else if (value instanceof Sketch) {
      return ((Sketch) value);
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Integer Tuple Sketch aggregation: " + value.getClass().getSimpleName());
    }
  }

  @Override
  public Object applyRawValue(Object aggregatedValue, byte[] rawValue) {
    Union tupleUnion = extractUnion(aggregatedValue);
    tupleUnion.union(deserializeAggregatedValue(rawValue));
    return tupleUnion;
  }

  @Override
  public Object applyAggregatedValue(Object value, Object aggregatedValue) {
    Union tupleUnion = extractUnion(aggregatedValue);
    Sketch sketch = extractSketch(value);
    tupleUnion.union(sketch);
    return tupleUnion;
  }

  @Override
  public Object cloneAggregatedValue(Object value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  /**
   * Returns the maximum number of storage bytes required for a Compact Integer Tuple Sketch with the given
   * number of actual entries. Note that this assumes the worst case of the sketch in
   * estimation mode, which requires storing theta and count.
   * @return the maximum number of storage bytes required for a Compact Integer Tuple Sketch with the given number
   * of entries.
   */
  @Override
  public int getMaxAggregatedValueByteSize() {
    if (_nominalEntries == 0) {
      return 8;
    }
    if (_nominalEntries == 1) {
      return 16;
    }
    int longSizeInBytes = Long.BYTES;
    int intSizeInBytes = Integer.BYTES;
    return (_nominalEntries * (longSizeInBytes + intSizeInBytes)) + 24;
  }

  @Override
  public byte[] serializeAggregatedValue(Object value) {
    Sketch sketch = extractSketch(value);
    return sketch.compact().toByteArray();
  }

  @Override
  public Sketch<IntegerSummary> deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(bytes);
  }

  @VisibleForTesting
  public int getNominalEntries() {
    return _nominalEntries;
  }
}
