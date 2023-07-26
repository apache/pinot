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

import java.util.stream.IntStream;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class DistinctCountThetaSketchValueAggregatorTest {

  @Test
  public void initialShouldCreateSingleItemSketch() {
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator();
    assertEquals(agg.getInitialAggregatedValue("hello world").getEstimate(), 1.0);
  }

  @Test
  public void initialShouldParseASketch() {
    UpdateSketch input = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input::update);
    Sketch result = input.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator();
    byte[] bytes = agg.serializeAggregatedValue(result);
    assertEquals(agg.getInitialAggregatedValue(bytes).getEstimate(), result.getEstimate());

    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), result.getCurrentBytes());
  }

  @Test
  public void initialShouldParseMultiValueSketches() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    input1.update("hello");
    UpdateSketch input2 = Sketches.updateSketchBuilder().build();
    input2.update("world");
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator();
    byte[][] bytes = {agg.serializeAggregatedValue(input1), agg.serializeAggregatedValue(input2)};
    assertEquals(agg.getInitialAggregatedValue(bytes).getEstimate(), 2.0);
  }

  @Test
  public void applyAggregatedValueShouldUnion() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    Sketch result1 = input1.compact();
    UpdateSketch input2 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input2::update);
    Sketch result2 = input2.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator();
    Sketch result = agg.applyAggregatedValue(result1, result2);
    Union union =
        Union.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();

    Sketch merged = union.union(result1, result2);

    assertEquals(result.getEstimate(), merged.getEstimate());

    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), merged.getCurrentBytes());
  }

  @Test
  public void applyRawValueShouldUnion() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    Sketch result1 = input1.compact();
    UpdateSketch input2 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input2::update);
    Sketch result2 = input2.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator();
    byte[] result2bytes = agg.serializeAggregatedValue(result2);
    Sketch result = agg.applyRawValue(result1, result2bytes);
    Union union =
        Union.builder().setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES).buildUnion();

    Sketch merged = union.union(result1, result2);

    assertEquals(result.getEstimate(), merged.getEstimate());

    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), merged.getCurrentBytes());
  }

  @Test
  public void applyRawValueShouldAdd() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    input1.update("hello".hashCode());
    Sketch result1 = input1.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator();
    Sketch result = agg.applyRawValue(result1, "world");

    assertEquals(result.getEstimate(), 2.0);

    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), 32 // may change in future versions of datasketches
    );
  }

  @Test
  public void applyRawValueShouldSupportMultiValue() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    input1.update("hello");
    Sketch result1 = input1.compact();
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator();
    String[] strings = {"hello", "world", "this", "is", "some", "strings"};
    Sketch result = agg.applyRawValue(result1, (Object) strings);

    assertEquals(result.getEstimate(), 6.0);

    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), 64 // may change in future versions of datasketches
    );
  }

  @Test
  public void getInitialValueShouldSupportDifferentTypes() {
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator();
    assertEquals(agg.getInitialAggregatedValue(12345).getEstimate(), 1.0);
    assertEquals(agg.getInitialAggregatedValue(12345L).getEstimate(), 1.0);
    assertEquals(agg.getInitialAggregatedValue(12.345f).getEstimate(), 1.0);
    assertEquals(agg.getInitialAggregatedValue(12.345d).getEstimate(), 1.0);
    assertThrows(() -> agg.getInitialAggregatedValue(new Object()));
  }

  @Test
  public void getInitialValueShouldSupportMultiValueTypes() {
    DistinctCountThetaSketchValueAggregator agg = new DistinctCountThetaSketchValueAggregator();
    Integer[] ints = {12345};
    assertEquals(agg.getInitialAggregatedValue(ints).getEstimate(), 1.0);
    Long[] longs = {12345L};
    assertEquals(agg.getInitialAggregatedValue(longs).getEstimate(), 1.0);
    Float[] floats = {12.345f};
    assertEquals(agg.getInitialAggregatedValue(floats).getEstimate(), 1.0);
    Double[] doubles = {12.345d};
    assertEquals(agg.getInitialAggregatedValue(doubles).getEstimate(), 1.0);
    Object[] objects = {new Object()};
    assertThrows(() -> agg.getInitialAggregatedValue(objects));
    byte[][] zeroSketches = {};
    assertEquals(agg.getInitialAggregatedValue(zeroSketches).getEstimate(), 0.0);
  }
}
