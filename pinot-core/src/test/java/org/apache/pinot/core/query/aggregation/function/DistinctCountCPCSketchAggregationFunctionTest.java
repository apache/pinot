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
package org.apache.pinot.core.query.aggregation.function;

import java.lang.foreign.MemorySegment;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.segment.local.customobject.CpcSketchAccumulator;
import org.apache.pinot.segment.local.customobject.SerializedCPCSketch;
import org.apache.pinot.segment.spi.Constants;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DistinctCountCPCSketchAggregationFunctionTest {

  @Test
  public void testCanUseStarTreeDefaultLgK() {
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);

    assertTrue(function.canUseStarTree(Map.of()));
    assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, "12")));
    assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, 12)));
    assertFalse(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, 11)));

    function = new DistinctCountCPCSketchAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"), ExpressionContext.forLiteral(Literal.intValue(12))), false);

    assertTrue(function.canUseStarTree(Map.of()));
    assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, "12")));
    assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, 12)));
    assertFalse(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, 11)));
  }

  @Test
  public void testCanUseCustomLgK() {
    DistinctCountCPCSketchAggregationFunction function = new DistinctCountCPCSketchAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.stringValue("nominalEntries=8192"))), false);

    // Default lgK = 12 / K=4096
    assertFalse(function.canUseStarTree(Map.of()));
    assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, "14")));
    assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, 13)));
    assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, "13")));
  }

  /// Tests the empty result path used by BROKER_EVALUATE when all segments are pruned.
  /// The sequence createAggregationResultHolder → extractAggregationResult → extractFinalResult
  /// must produce a value that is convertible to the declared final result column type.
  @Test
  public void testEmptyResultProducesConvertibleFinalResult() {
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);

    AggregationResultHolder holder = function.createAggregationResultHolder();
    CpcSketchAccumulator accumulator = function.extractAggregationResult(holder);
    assertTrue(accumulator.isEmpty());

    // extractFinalResult should return 0 for an empty accumulator
    Comparable result = function.extractFinalResult(accumulator);
    assertEquals(result, 0L);

    // The result must be convertible via the declared column type
    Object converted = function.getFinalResultColumnType().convert(result);
    assertEquals(converted, 0L);
  }

  @Test
  public void testEmptyResultProducesConvertibleRawFinalResult() {
    DistinctCountRawCPCSketchAggregationFunction function =
        new DistinctCountRawCPCSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);

    AggregationResultHolder holder = function.createAggregationResultHolder();
    CpcSketchAccumulator accumulator = function.extractAggregationResult(holder);
    assertTrue(accumulator.isEmpty());

    // extractFinalResult should return a SerializedCPCSketch
    SerializedCPCSketch rawResult = function.extractFinalResult(accumulator);
    assertNotNull(rawResult);

    // The declared column type is STRING; convert() must produce a String
    assertEquals(function.getFinalResultColumnType(), ColumnDataType.STRING);
    Object converted = function.getFinalResultColumnType().convert(rawResult);
    assertTrue(converted instanceof String);

    // The string should be a valid Base64-encoded CPC sketch that round-trips
    String base64 = (String) converted;
    byte[] bytes = Base64.getDecoder().decode(base64);
    CpcSketch deserialized = CpcSketch.heapify(MemorySegment.ofArray(bytes));
    assertEquals(deserialized.getEstimate(), 0.0);
  }

  @Test
  public void testMergeWithEmptyAccumulators() {
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);

    CpcSketchAccumulator empty1 = new CpcSketchAccumulator(12, 2);
    CpcSketchAccumulator empty2 = new CpcSketchAccumulator(12, 2);
    CpcSketchAccumulator nonEmpty = new CpcSketchAccumulator(12, 2);
    CpcSketch sketch = new CpcSketch(12);
    sketch.update("hello");
    nonEmpty.apply(sketch);

    // merge(empty, non-empty) should return non-empty
    CpcSketchAccumulator result = function.merge(empty1, nonEmpty);
    assertSame(result, nonEmpty);

    // merge(non-empty, empty) should return non-empty
    result = function.merge(nonEmpty, empty2);
    assertSame(result, nonEmpty);

    // merge(empty, empty) should return empty
    result = function.merge(new CpcSketchAccumulator(12, 2), new CpcSketchAccumulator(12, 2));
    assertTrue(result.isEmpty());
  }

  @Test
  public void testMergeRawFinalResults() {
    DistinctCountRawCPCSketchAggregationFunction function =
        new DistinctCountRawCPCSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);
    CpcSketch sketch1 = new CpcSketch(12);
    sketch1.update("a");
    sketch1.update("b");
    CpcSketch sketch2 = new CpcSketch(12);
    sketch2.update("c");

    String merged = function.mergeFinalResult(new SerializedCPCSketch(sketch1).toString(),
        new SerializedCPCSketch(sketch2).toString());
    CpcSketch mergedSketch = CpcSketch.heapify(MemorySegment.ofArray(Base64.getDecoder().decode(merged)));
    assertEquals(Math.round(mergedSketch.getEstimate()), 3L);
  }
}
