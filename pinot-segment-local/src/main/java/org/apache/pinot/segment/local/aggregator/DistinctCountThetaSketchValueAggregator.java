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
import org.apache.datasketches.theta.ThetaSetOperationBuilder;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.ThetaUnion;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountThetaSketchValueAggregator implements ValueAggregator<Object, Object> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private final ThetaSetOperationBuilder _setOperationBuilder;
  private final int _nominalEntries;

  // This changes a lot similar to the Bitmap aggregator
  private int _maxByteSize;

  public DistinctCountThetaSketchValueAggregator(List<ExpressionContext> arguments) {
    // No argument means we use the Helix default
    if (arguments.isEmpty()) {
      _nominalEntries = CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES;
    } else {
      _nominalEntries = arguments.get(0).getLiteral().getIntValue();
    }
    _setOperationBuilder = ThetaUnion.builder().setNominalEntries(_nominalEntries);
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTTHETASKETCH;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @VisibleForTesting
  public int getNominalEntries() {
    return _nominalEntries;
  }

  private void singleItemUpdate(ThetaUnion thetaUnion, Object rawValue) {
    if (rawValue instanceof String) {
      thetaUnion.update((String) rawValue);
    } else if (rawValue instanceof Integer) {
      thetaUnion.update((Integer) rawValue);
    } else if (rawValue instanceof Long) {
      thetaUnion.update((Long) rawValue);
    } else if (rawValue instanceof Double) {
      thetaUnion.update((Double) rawValue);
    } else if (rawValue instanceof Float) {
      thetaUnion.update((Float) rawValue);
    } else if (rawValue instanceof Object[]) {
      multiItemUpdate(thetaUnion, (Object[]) rawValue);
    } else if (rawValue instanceof ThetaSketch) {
      thetaUnion.union((ThetaSketch) rawValue);
    } else if (rawValue instanceof ThetaUnion) {
      thetaUnion.union(((ThetaUnion) rawValue).getResult());
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Theta Sketch aggregation: " + rawValue.getClass().getSimpleName());
    }
  }

  private void multiItemUpdate(ThetaUnion thetaUnion, Object[] rawValues) {
    if (rawValues instanceof String[]) {
      for (String s : (String[]) rawValues) {
        thetaUnion.update(s);
      }
    } else if (rawValues instanceof Integer[]) {
      for (Integer i : (Integer[]) rawValues) {
        thetaUnion.update(i);
      }
    } else if (rawValues instanceof Long[]) {
      for (Long l : (Long[]) rawValues) {
        thetaUnion.update(l);
      }
    } else if (rawValues instanceof Double[]) {
      for (Double d : (Double[]) rawValues) {
        thetaUnion.update(d);
      }
    } else if (rawValues instanceof Float[]) {
      for (Float f : (Float[]) rawValues) {
        thetaUnion.update(f);
      }
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Theta Sketch aggregation: " + rawValues.getClass().getSimpleName());
    }
  }

  @Override
  public Object getInitialAggregatedValue(Object rawValue) {
    // NOTE: rawValue cannot be null because this aggregator can only be used for star-tree index.
    assert rawValue != null;
    ThetaUnion thetaUnion = _setOperationBuilder.buildUnion();
    if (rawValue instanceof byte[]) { // Serialized ThetaSketch
      byte[] bytes = (byte[]) rawValue;
      ThetaSketch sketch = deserializeAggregatedValue(bytes);
      thetaUnion.union(sketch);
    } else if (rawValue instanceof byte[][]) { // Multiple Serialized Sketches
      byte[][] serializedSketches = (byte[][]) rawValue;
      for (byte[] sketchBytes : serializedSketches) {
        thetaUnion.union(deserializeAggregatedValue(sketchBytes));
      }
    } else {
      singleItemUpdate(thetaUnion, rawValue);
    }
    _maxByteSize = Math.max(_maxByteSize, thetaUnion.getCurrentBytes());
    return thetaUnion;
  }

  private ThetaUnion extractUnion(Object value) {
    if (value == null) {
      return _setOperationBuilder.buildUnion();
    } else if (value instanceof ThetaUnion) {
      return (ThetaUnion) value;
    } else if (value instanceof ThetaSketch) {
      ThetaSketch sketch = (ThetaSketch) value;
      ThetaUnion thetaUnion = _setOperationBuilder.buildUnion();
      thetaUnion.union(sketch);
      return thetaUnion;
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Theta Sketch aggregation: " + value.getClass().getSimpleName());
    }
  }

  @Override
  public Object applyRawValue(Object aggregatedValue, Object rawValue) {
    ThetaUnion thetaUnion = extractUnion(aggregatedValue);
    if (rawValue instanceof byte[]) {
      ThetaSketch sketch = deserializeAggregatedValue((byte[]) rawValue);
      thetaUnion.union(sketch);
    } else {
      singleItemUpdate(thetaUnion, rawValue);
    }
    _maxByteSize = Math.max(_maxByteSize, thetaUnion.getCurrentBytes());
    return thetaUnion;
  }

  @Override
  public Object applyAggregatedValue(Object value, Object aggregatedValue) {
    ThetaUnion thetaUnion = extractUnion(aggregatedValue);
    singleItemUpdate(thetaUnion, value);
    _maxByteSize = Math.max(_maxByteSize, thetaUnion.getCurrentBytes());
    return thetaUnion;
  }

  @Override
  public Object cloneAggregatedValue(Object value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public boolean isAggregatedValueFixedSize() {
    return false;
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(Object value) {
    if (value instanceof ThetaUnion) {
      return CustomSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(((ThetaUnion) value).getResult());
    } else if (value instanceof ThetaSketch) {
      return CustomSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(((ThetaSketch) value));
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Theta Sketch aggregation: " + value.getClass().getSimpleName());
    }
  }

  @Override
  public ThetaSketch deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(bytes);
  }
}
