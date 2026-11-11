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
import org.apache.datasketches.tuple.TupleSketch;
import org.apache.datasketches.tuple.TupleSketchIterator;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummaryDeserializer;
import org.apache.datasketches.tuple.aninteger.IntegerTupleSketch;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.Constants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class IntegerTupleSketchAggregationFunctionTest {

  @Test
  public void testCanUseStarTreeDefaultK() {
    IntegerTupleSketchAggregationFunction function =
        new IntegerTupleSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")),
            IntegerSummary.Mode.Sum, false);

    assertTrue(function.canUseStarTree(Map.of()));
    assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "16384")));
    assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 16384)));
    assertFalse(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 8192)));
  }

  @Test
  public void testCanUseCustomK() {
    IntegerTupleSketchAggregationFunction function = new IntegerTupleSketchAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"), ExpressionContext.forLiteral(Literal.intValue(32768))),
        IntegerSummary.Mode.Sum, false);

    // Default StarTree lgK = 14 / K=16384
    assertFalse(function.canUseStarTree(Map.of()));
    assertFalse(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "16384")));
    assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "65536")));
    assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 32768)));
    assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "32768")));
  }

  @Test
  public void testMergeRawFinalResults() {
    IntegerTupleSketchAggregationFunction function =
        new IntegerTupleSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")),
            IntegerSummary.Mode.Sum, false);
    IntegerTupleSketch sketch1 = new IntegerTupleSketch(4, IntegerSummary.Mode.Sum);
    sketch1.update(1L, 1);
    sketch1.update(2L, 2);
    IntegerTupleSketch sketch2 = new IntegerTupleSketch(4, IntegerSummary.Mode.Sum);
    sketch2.update(3L, 3);

    String merged = (String) function.mergeFinalResult(serialize(sketch1), serialize(sketch2));
    TupleSketch<IntegerSummary> mergedSketch = TupleSketch.heapifySketch(
        MemorySegment.ofArray(Base64.getDecoder().decode(merged)), new IntegerSummaryDeserializer());
    assertEquals(mergedSketch.getEstimate(), 3.0);
    int sum = 0;
    TupleSketchIterator<IntegerSummary> iterator = mergedSketch.iterator();
    while (iterator.next()) {
      sum += iterator.getSummary().getValue();
    }
    assertEquals(sum, 6);
  }

  @Test
  public void testMergeAvgValueFinalResults() {
    AvgValueIntegerTupleSketchAggregationFunction function =
        new AvgValueIntegerTupleSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")),
            IntegerSummary.Mode.Sum, false);
    assertThrows(UnsupportedOperationException.class, () -> function.mergeFinalResult(1L, 2L));
  }

  private static String serialize(IntegerTupleSketch sketch) {
    return Base64.getEncoder().encodeToString(sketch.compact().toByteArray());
  }
}
