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
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class DistinctCountCPCSketchValueAggregatorTest {

  @Test
  public void initialShouldCreateSingleItemSketch() {
    DistinctCountCPCSketchValueAggregator agg = new DistinctCountCPCSketchValueAggregator(Collections.emptyList());
    assertEquals(agg.getInitialAggregatedValue("hello world").getEstimate(), 1.0);
  }

  @Test
  public void initialShouldParseASketch() {
    CpcSketch input = new CpcSketch();
    IntStream.range(0, 1000).forEach(input::update);
    DistinctCountCPCSketchValueAggregator agg = new DistinctCountCPCSketchValueAggregator(Collections.emptyList());
    byte[] bytes = agg.serializeAggregatedValue(input);
    assertEquals(agg.getInitialAggregatedValue(bytes).getEstimate(), input.getEstimate());

    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), input.toByteArray().length);
  }

  @Test
  public void initialShouldParseMultiValueSketches() {
    CpcSketch input1 = new CpcSketch();
    input1.update("hello");
    CpcSketch input2 = new CpcSketch();
    input2.update("world");
    DistinctCountCPCSketchValueAggregator agg = new DistinctCountCPCSketchValueAggregator(Collections.emptyList());
    byte[][] bytes = {agg.serializeAggregatedValue(input1), agg.serializeAggregatedValue(input2)};
    assertEquals(Math.round(agg.getInitialAggregatedValue(bytes).getEstimate()), 2);
  }

  @Test
  public void applyAggregatedValueShouldUnion() {
    CpcSketch input1 = new CpcSketch();
    IntStream.range(0, 1000).forEach(input1::update);
    CpcSketch input2 = new CpcSketch();
    IntStream.range(0, 1000).forEach(input2::update);
    DistinctCountCPCSketchValueAggregator agg = new DistinctCountCPCSketchValueAggregator(Collections.emptyList());
    CpcSketch result = agg.applyAggregatedValue(input1, input2);

    CpcUnion union = new CpcUnion(CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK);
    union.update(input1);
    union.update(input2);
    CpcSketch merged = union.getResult();

    assertEquals(result.getEstimate(), merged.getEstimate());

    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), merged.toByteArray().length);
  }

  @Test
  public void applyRawValueShouldUnion() {
    CpcSketch input1 = new CpcSketch();
    IntStream.range(0, 1000).forEach(input1::update);
    CpcSketch input2 = new CpcSketch();
    IntStream.range(0, 1000).forEach(input2::update);
    DistinctCountCPCSketchValueAggregator agg = new DistinctCountCPCSketchValueAggregator(Collections.emptyList());
    byte[] result2bytes = agg.serializeAggregatedValue(input2);
    CpcSketch result = agg.applyRawValue(input1, result2bytes);

    CpcUnion union = new CpcUnion(CommonConstants.Helix.DEFAULT_CPC_SKETCH_LGK);
    union.update(input1);
    union.update(input2);
    CpcSketch merged = union.getResult();

    assertEquals(result.getEstimate(), merged.getEstimate());

    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), merged.toByteArray().length);
  }

  @Test
  public void applyRawValueShouldAdd() {
    CpcSketch input1 = new CpcSketch();
    input1.update("hello".hashCode());
    DistinctCountCPCSketchValueAggregator agg = new DistinctCountCPCSketchValueAggregator(Collections.emptyList());
    CpcSketch result = agg.applyRawValue(input1, "world");
    assertEquals(Math.round(result.getEstimate()), 2);

    CpcSketch pristine = new CpcSketch();
    pristine.update("hello");
    pristine.update("world");
    assertEquals(agg.getMaxAggregatedValueByteSize(), pristine.toByteArray().length);
  }

  @Test
  public void applyRawValueShouldSupportMultiValue() {
    CpcSketch input1 = new CpcSketch();
    input1.update("hello");
    DistinctCountCPCSketchValueAggregator agg = new DistinctCountCPCSketchValueAggregator(Collections.emptyList());
    String[] strings = {"hello", "world", "this", "is", "some", "strings"};
    CpcSketch result = agg.applyRawValue(input1, strings);

    assertEquals(Math.round(result.getEstimate()), 6);

    CpcSketch pristine = new CpcSketch();
    for (String value : strings) {
      pristine.update(value);
    }
    assertEquals(agg.getMaxAggregatedValueByteSize(), pristine.toByteArray().length);
  }

  @Test
  public void getInitialValueShouldSupportDifferentTypes() {
    DistinctCountCPCSketchValueAggregator agg = new DistinctCountCPCSketchValueAggregator(Collections.emptyList());
    assertEquals(agg.getInitialAggregatedValue(12345).getEstimate(), 1.0);
    assertEquals(agg.getInitialAggregatedValue(12345L).getEstimate(), 1.0);
    assertEquals(agg.getInitialAggregatedValue(12.345f).getEstimate(), 1.0);
    assertEquals(agg.getInitialAggregatedValue(12.345d).getEstimate(), 1.0);
    assertThrows(() -> agg.getInitialAggregatedValue(new Object()));
  }

  @Test
  public void getInitialValueShouldSupportMultiValueTypes() {
    DistinctCountCPCSketchValueAggregator agg = new DistinctCountCPCSketchValueAggregator(Collections.emptyList());
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
