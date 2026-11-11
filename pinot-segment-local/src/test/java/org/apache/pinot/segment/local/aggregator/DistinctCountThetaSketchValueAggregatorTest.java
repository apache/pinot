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
import java.util.stream.IntStream;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.ThetaUnion;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class DistinctCountThetaSketchValueAggregatorTest {

  @Test
  public void initialShouldCreateSingleItemSketch() {
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(List.of());
    assertEquals(toSketch(agg.getInitialAggregatedValue("hello world")).getEstimate(), 1.0);
  }

  @Test
  public void initialShouldParseASketch() {
    UpdatableThetaSketch input = UpdatableThetaSketch.builder().build();
    IntStream.range(0, 1000).forEach(input::update);
    ThetaSketch result = input.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(List.of());
    byte[] bytes = agg.serializeAggregatedValue(result);
    ThetaSketch initSketch = toSketch(agg.getInitialAggregatedValue(bytes));
    ThetaUnion union =
        ThetaUnion.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
    union.union(initSketch);
    assertEquals(initSketch.getEstimate(), result.getEstimate());
    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), union.getCurrentBytes());
  }

  @Test
  public void initialShouldParseMultiValueSketches() {
    UpdatableThetaSketch input1 = UpdatableThetaSketch.builder().build();
    input1.update("hello");
    UpdatableThetaSketch input2 = UpdatableThetaSketch.builder().build();
    input2.update("world");
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(List.of());
    byte[][] bytes = {agg.serializeAggregatedValue(input1), agg.serializeAggregatedValue(input2)};
    assertEquals(toSketch(agg.getInitialAggregatedValue(bytes)).getEstimate(), 2.0);
  }

  @Test
  public void applyAggregatedValueShouldUnion() {
    UpdatableThetaSketch input1 = UpdatableThetaSketch.builder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    ThetaSketch result1 = input1.compact();
    UpdatableThetaSketch input2 = UpdatableThetaSketch.builder().build();
    IntStream.range(0, 1000).forEach(input2::update);
    ThetaSketch result2 = input2.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(List.of());
    ThetaSketch result = toSketch(agg.applyAggregatedValue(result1, result2));
    ThetaUnion union =
        ThetaUnion.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
    union.union(result1);
    union.union(result2);
    ThetaSketch merged = union.getResult();
    assertEquals(result.getEstimate(), merged.getEstimate());
    assertEquals(agg.getMaxAggregatedValueByteSize(), union.getCurrentBytes());
  }

  @Test
  public void applyRawValueShouldUnion() {
    UpdatableThetaSketch input1 = UpdatableThetaSketch.builder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    ThetaSketch result1 = input1.compact();
    UpdatableThetaSketch input2 = UpdatableThetaSketch.builder().build();
    IntStream.range(0, 1000).forEach(input2::update);
    ThetaSketch result2 = input2.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(List.of());
    byte[] result2bytes = agg.serializeAggregatedValue(result2);
    ThetaSketch result = toSketch(agg.applyRawValue(result1, result2bytes));
    ThetaUnion union =
        ThetaUnion.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
    union.union(result1);
    union.union(result2);
    ThetaSketch merged = union.getResult();
    assertEquals(result.getEstimate(), merged.getEstimate());
    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), union.getCurrentBytes());
  }

  @Test
  public void applyRawValueShouldAdd() {
    UpdatableThetaSketch input1 = UpdatableThetaSketch.builder().build();
    input1.update("hello".hashCode());
    ThetaSketch result1 = input1.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(List.of());
    ThetaSketch result = toSketch(agg.applyRawValue(result1, "world"));
    ThetaUnion union =
        ThetaUnion.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
    union.union(result);
    assertEquals(result.getEstimate(), 2.0);
    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), union.getCurrentBytes());
  }

  @Test
  public void applyRawValueShouldSupportMultiValue() {
    UpdatableThetaSketch input1 = UpdatableThetaSketch.builder().build();
    input1.update("hello");
    ThetaSketch result1 = input1.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(List.of());
    String[] strings = {"hello", "world", "this", "is", "some", "strings"};
    ThetaSketch result = toSketch(agg.applyRawValue(result1, (Object) strings));
    ThetaUnion union =
        ThetaUnion.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();
    union.union(result);
    assertEquals(result.getEstimate(), 6.0);
    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), union.getCurrentBytes());
  }

  @Test
  public void getInitialValueShouldSupportDifferentTypes() {
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(List.of());
    assertEquals(toSketch(agg.getInitialAggregatedValue(12345)).getEstimate(), 1.0);
    assertEquals(toSketch(agg.getInitialAggregatedValue(12345L)).getEstimate(), 1.0);
    assertEquals(toSketch(agg.getInitialAggregatedValue(12.345f)).getEstimate(), 1.0);
    assertEquals(toSketch(agg.getInitialAggregatedValue(12.345d)).getEstimate(), 1.0);
    assertThrows(() -> agg.getInitialAggregatedValue(new Object()));
  }

  @Test
  public void getInitialValueShouldSupportMultiValueTypes() {
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(List.of());
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
    UpdatableThetaSketch input = UpdatableThetaSketch.builder().build();
    IntStream.range(0, 10).forEach(input::update);
    ThetaSketch unordered = input.compact(false, null);
    ThetaSketch ordered = input.compact(true, null);
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator(List.of());
    assertTrue(toSketch(agg.cloneAggregatedValue(ordered)).isOrdered());
    assertFalse(toSketch(agg.cloneAggregatedValue(unordered)).isOrdered());
  }

  private ThetaSketch toSketch(Object value) {
    if (value instanceof ThetaUnion) {
      return ((ThetaUnion) value).getResult();
    } else if (value instanceof ThetaSketch) {
      return (ThetaSketch) value;
    } else {
      throw new IllegalStateException(
          "Unsupported data type for Theta Sketch aggregation: " + value.getClass().getSimpleName());
    }
  }
}
