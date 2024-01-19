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

import java.util.List;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountCPCSketchValueAggregator implements ValueAggregator<Object, CpcSketch> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private final int _lgK;

  private int _maxByteSize;

  public DistinctCountCPCSketchValueAggregator(List<ExpressionContext> arguments) {
    // length 1 means we use the Helix default
    if (arguments.size() <= 1) {
      _lgK = CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK;
    } else {
      _lgK = arguments.get(1).getLiteral().getIntValue();
    }
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTCPCSKETCH;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public CpcSketch getInitialAggregatedValue(Object rawValue) {
    CpcSketch initialValue;
    if (rawValue instanceof byte[]) { // Serialized Sketch
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else if (rawValue instanceof byte[][]) { // Multiple Serialized Sketches
      byte[][] serializedSketches = (byte[][]) rawValue;
      CpcUnion union = new CpcUnion(_lgK);
      for (byte[] bytes : serializedSketches) {
        union.update(deserializeAggregatedValue(bytes));
      }
      initialValue = union.getResult();
      updateMaxByteSize(initialValue);
    } else {
      initialValue = empty();
      addObjectToSketch(rawValue, initialValue);
      updateMaxByteSize(initialValue);
    }
    return initialValue;
  }

  @Override
  public CpcSketch applyRawValue(CpcSketch value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      CpcSketch sketch = union(value, deserializeAggregatedValue(bytes));
      updateMaxByteSize(sketch);
      return sketch;
    } else {
      addObjectToSketch(rawValue, value);
      updateMaxByteSize(value);
      return value;
    }
  }

  @Override
  public CpcSketch applyAggregatedValue(CpcSketch value, CpcSketch aggregatedValue) {
    CpcSketch result = union(value, aggregatedValue);
    updateMaxByteSize(result);
    return result;
  }

  @Override
  public CpcSketch cloneAggregatedValue(CpcSketch value) {
    return deserializeAggregatedValue(serializeAggregatedValue(value));
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    // NOTE: For aggregated metrics, initial aggregated value might have not been generated. Returns the byte size
    //       based on lgK.
    return _maxByteSize > 0 ? _maxByteSize : CpcSketch.getMaxSerializedBytes(_lgK);
  }

  @Override
  public byte[] serializeAggregatedValue(CpcSketch value) {
    return CustomSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(value);
  }

  @Override
  public CpcSketch deserializeAggregatedValue(byte[] bytes) {
    return CustomSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(bytes);
  }

  private CpcSketch union(CpcSketch left, CpcSketch right) {
    if (left == null && right == null) {
      return empty();
    } else if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    }

    CpcUnion union = new CpcUnion(_lgK);
    union.update(left);
    union.update(right);
    return union.getResult();
  }

  private void addObjectToSketch(Object rawValue, CpcSketch sketch) {
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
          "Unsupported data type for CPC Sketch aggregation: " + rawValue.getClass().getSimpleName());
    }
  }

  private void addObjectsToSketch(Object[] rawValues, CpcSketch sketch) {
    if (rawValues instanceof String[]) {
      for (String s : (String[]) rawValues) {
        sketch.update(s);
      }
    } else if (rawValues instanceof Integer[]) {
      for (Integer i : (Integer[]) rawValues) {
        sketch.update(i);
      }
    } else if (rawValues instanceof Long[]) {
      for (Long l : (Long[]) rawValues) {
        sketch.update(l);
      }
    } else if (rawValues instanceof Double[]) {
      for (Double d : (Double[]) rawValues) {
        sketch.update(d);
      }
    } else if (rawValues instanceof Float[]) {
      for (Float f : (Float[]) rawValues) {
        sketch.update(f);
      }
    } else {
      throw new IllegalStateException(
          "Unsupported data type for CPC Sketch aggregation: " + rawValues.getClass().getSimpleName());
    }
  }

  private void updateMaxByteSize(CpcSketch sketch) {
    if (sketch != null) {
      _maxByteSize = Math.max(_maxByteSize, sketch.toByteArray().length);
    }
  }

  private CpcSketch empty() {
    return new CpcSketch(_lgK);
  }
}
