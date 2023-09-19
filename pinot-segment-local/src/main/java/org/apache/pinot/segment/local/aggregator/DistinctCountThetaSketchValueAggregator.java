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

import java.util.Arrays;
import java.util.stream.StreamSupport;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountThetaSketchValueAggregator implements ValueAggregator<Object, Sketch> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private final Union _union;

  // This changes a lot similar to the Bitmap aggregator
  private int _maxByteSize;

  public DistinctCountThetaSketchValueAggregator() {
    // TODO: Handle configurable nominal entries for StarTreeBuilder
    _union = Union.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTTHETASKETCH;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  // Utility method to create a theta sketch with one item in it
  private Sketch singleItemSketch(Object rawValue) {
    // TODO: Handle configurable nominal entries for StarTreeBuilder
    UpdateSketch sketch =
        Sketches.updateSketchBuilder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES)
            .build();
    if (rawValue instanceof String) {
      sketch.update((String) rawValue);
    } else if (rawValue instanceof Integer) {
      sketch.update((Integer) rawValue);
    } else if (rawValue instanceof Long) {
      sketch.update((Long) rawValue);
    } else if (rawValue instanceof Double) {
      sketch.update((Double) rawValue);
    } else if (rawValue instanceof Float) {
      sketch.update((Float) rawValue);
    } else if (rawValue instanceof Object[]) {
      addObjectsToSketch((Object[]) rawValue, sketch);
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Theta Sketch aggregation: " + rawValue.getClass().getSimpleName());
    }
    return sketch.compact();
  }

  private void addObjectsToSketch(Object[] rawValues, UpdateSketch updateSketch) {
    if (rawValues instanceof String[]) {
      for (String s : (String[]) rawValues) {
        updateSketch.update(s);
      }
    } else if (rawValues instanceof Integer[]) {
      for (Integer i : (Integer[]) rawValues) {
        updateSketch.update(i);
      }
    } else if (rawValues instanceof Long[]) {
      for (Long l : (Long[]) rawValues) {
        updateSketch.update(l);
      }
    } else if (rawValues instanceof Double[]) {
      for (Double d : (Double[]) rawValues) {
        updateSketch.update(d);
      }
    } else if (rawValues instanceof Float[]) {
      for (Float f : (Float[]) rawValues) {
        updateSketch.update(f);
      }
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Theta Sketch aggregation: " + rawValues.getClass().getSimpleName());
    }
  }

  // Utility method to merge two sketches
  private Sketch union(Sketch left, Sketch right) {
    return _union.union(left, right);
  }

  // Utility method to make an empty sketch
  private Sketch empty() {
    // TODO: Handle configurable nominal entries for StarTreeBuilder
    return Sketches.updateSketchBuilder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES)
        .build().compact();
  }

  @Override
  public Sketch getInitialAggregatedValue(Object rawValue) {
    Sketch initialValue;
    if (rawValue instanceof byte[]) { // Serialized Sketch
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else if (rawValue instanceof byte[][]) { // Multiple Serialized Sketches
      byte[][] serializedSketches = (byte[][]) rawValue;
      initialValue = StreamSupport.stream(Arrays.stream(serializedSketches).spliterator(), false)
          .map(this::deserializeAggregatedValue).reduce(this::union).orElseGet(this::empty);
      _maxByteSize = Math.max(_maxByteSize, initialValue.getCurrentBytes());
    } else {
      initialValue = singleItemSketch(rawValue);
      _maxByteSize = Math.max(_maxByteSize, initialValue.getCurrentBytes());
    }
    return initialValue;
  }

  @Override
  public Sketch applyRawValue(Sketch value, Object rawValue) {
    Sketch right;
    if (rawValue instanceof byte[]) {
      right = deserializeAggregatedValue((byte[]) rawValue);
    } else {
      right = singleItemSketch(rawValue);
    }
    Sketch result = union(value, right).compact();
    _maxByteSize = Math.max(_maxByteSize, result.getCurrentBytes());
    return result;
  }

  @Override
  public Sketch applyAggregatedValue(Sketch value, Sketch aggregatedValue) {
    Sketch result = union(value, aggregatedValue);
    _maxByteSize = Math.max(_maxByteSize, result.getCurrentBytes());
    return result;
  }

  @Override
  public Sketch cloneAggregatedValue(Sketch value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(Sketch value) {
    return CustomSerDeUtils.DATA_SKETCH_SER_DE.serialize(value);
  }

  @Override
  public Sketch deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.DATA_SKETCH_SER_DE.deserialize(bytes);
  }
}
