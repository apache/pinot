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

import org.apache.datasketches.theta.SingleItemSketch;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountThetaSketchValueAggregator implements ValueAggregator<Object, Sketch> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTHLL;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  // This changes a lot similar to the Bitmap aggregator
  private int _maxByteSize;

  // Utility method to create a theta sketch with one item in it
  private Sketch singleItemSketch(Object rawValue) {
    return SingleItemSketch.create(rawValue.hashCode()).compact();
  }

  // Utility method to merge two sketches
  private Sketch union(Sketch left, Sketch right) {
    // TODO: Handle configurable nominal entries for StarTreeBuilder
    Union u = Union.builder()
      .setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES)
      .buildUnion();
    u.update(left);
    u.update(right);
    return u.getResult().compact();
  }

  @Override
  public Sketch getInitialAggregatedValue(Object rawValue) {
    Sketch initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else {
      initialValue = singleItemSketch(rawValue);
      _maxByteSize = Math.max(_maxByteSize, initialValue.getCurrentBytes(true));
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
    _maxByteSize = Math.max(_maxByteSize, result.getCurrentBytes(true));
    return result;
  }


  @Override
  public Sketch applyAggregatedValue(Sketch value, Sketch aggregatedValue) {
    Sketch result = union(value, aggregatedValue);
    _maxByteSize = Math.max(_maxByteSize, result.getCurrentBytes(true));
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
