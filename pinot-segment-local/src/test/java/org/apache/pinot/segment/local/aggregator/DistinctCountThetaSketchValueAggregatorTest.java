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

import java.util.Collections;
import java.util.stream.IntStream;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class DistinctCountThetaSketchValueAggregatorTest {

  @Test
  public void initialShouldCreateSingleItemSketch() {
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    assertEquals(toSketch(agg.getInitialAggregatedValue("hello world")).getEstimate(), 1.0);
  }

  @Test
  public void initialShouldParseASketch() {
    UpdateSketch input = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input::update);
    Sketch result = input.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    byte[] bytes = agg.serializeAggregatedValue(result);
    Sketch initSketch = toSketch(agg.getInitialAggregatedValue(bytes));
    Union union =
        Union.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
    union.union(initSketch);
    assertEquals(initSketch.getEstimate(), result.getEstimate());
    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), union.getCurrentBytes());
  }

  @Test
  public void initialShouldParseMultiValueSketches() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    input1.update("hello");
    UpdateSketch input2 = Sketches.updateSketchBuilder().build();
    input2.update("world");
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    byte[][] bytes = {agg.serializeAggregatedValue(input1), agg.serializeAggregatedValue(input2)};
    assertEquals(toSketch(agg.getInitialAggregatedValue(bytes)).getEstimate(), 2.0);
  }

  @Test
  public void applyAggregatedValueShouldUnion() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    Sketch result1 = input1.compact();
    UpdateSketch input2 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input2::update);
    Sketch result2 = input2.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    Sketch result = toSketch(agg.applyAggregatedValue(result1, result2));
    Union union =
        Union.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
    union.union(result1);
    union.union(result2);
    Sketch merged = union.getResult();
    assertEquals(result.getEstimate(), merged.getEstimate());
    assertEquals(agg.getMaxAggregatedValueByteSize(), union.getCurrentBytes());
  }

  @Test
  public void applyRawValueShouldUnion() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    Sketch result1 = input1.compact();
    UpdateSketch input2 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input2::update);
    Sketch result2 = input2.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    byte[] result2bytes = agg.serializeAggregatedValue(result2);
    Sketch result = toSketch(agg.applyRawValue(result1, result2bytes));
    Union union =
        Union.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
    union.union(result1);
    union.union(result2);
    Sketch merged = union.getResult();
    assertEquals(result.getEstimate(), merged.getEstimate());
    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), union.getCurrentBytes());
  }

  @Test
  public void applyRawValueShouldAdd() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    input1.update("hello".hashCode());
    Sketch result1 = input1.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    Sketch result = toSketch(agg.applyRawValue(result1, "world"));
    Union union =
        Union.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
    union.union(result);
    assertEquals(result.getEstimate(), 2.0);
    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), union.getCurrentBytes());
  }

  @Test
  public void applyRawValueShouldSupportMultiValue() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    input1.update("hello");
    Sketch result1 = input1.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    String[] strings = {"hello", "world", "this", "is", "some", "strings"};
    Sketch result = toSketch(agg.applyRawValue(result1, (Object) strings));
    Union union =
        Union.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
    union.union(result);
    assertEquals(result.getEstimate(), 6.0);
    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), union.getCurrentBytes());
  }

  @Test
  public void getInitialValueShouldSupportDifferentTypes() {
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    assertEquals(toSketch(agg.getInitialAggregatedValue(12345)).getEstimate(), 1.0);
    assertEquals(toSketch(agg.getInitialAggregatedValue(12345L)).getEstimate(), 1.0);
    assertEquals(toSketch(agg.getInitialAggregatedValue(12.345f)).getEstimate(), 1.0);
    assertEquals(toSketch(agg.getInitialAggregatedValue(12.345d)).getEstimate(), 1.0);
    assertThrows(() -> agg.getInitialAggregatedValue(new Object()));
  }

  @Test
  public void getInitialValueShouldSupportMultiValueTypes() {
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    Integer[] ints = {12345};
    assertEquals(toSketch(agg.getInitialAggregatedValue(ints)).getEstimate(), 1.0);
    Long[] longs = {12345L};
    assertEquals(toSketch(agg.getInitialAggregatedValue(longs)).getEstimate(), 1.0);
    Float[] floats = {12.345f};
    assertEquals(toSketch(agg.getInitialAggregatedValue(floats)).getEstimate(), 1.0);
    Double[] doubles = {12.345d};
    assertEquals(toSketch(agg.getInitialAggregatedValue(doubles)).getEstimate(), 1.0);
    Object[] objects = {new Object()};
    assertThrows(() -> agg.getInitialAggregatedValue(objects));
    byte[][] zeroSketches = {};
    assertEquals(toSketch(agg.getInitialAggregatedValue(zeroSketches)).getEstimate(), 0.0);
  }

  @Test
  public void shouldRetainSketchOrdering() {
    UpdateSketch input = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 10).forEach(input::update);
    Sketch unordered = input.compact(false, null);
    Sketch ordered = input.compact(true, null);
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(Collections.emptyList());
    assertTrue(toSketch(agg.cloneAggregatedValue(ordered)).isOrdered());
    assertFalse(toSketch(agg.cloneAggregatedValue(unordered)).isOrdered());
  }

  private Sketch toSketch(Object value) {
    if (value instanceof Union) {
      return ((Union) value).getResult();
    } else if (value instanceof Sketch) {
      return (Sketch) value;
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Theta Sketch aggregation: " + value.getClass().getSimpleName());
    }
  }
}
