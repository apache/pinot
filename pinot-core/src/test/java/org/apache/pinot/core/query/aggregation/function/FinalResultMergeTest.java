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

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.util.Base64;
import java.util.List;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.FrequentItemsSketch;
import org.apache.datasketches.frequencies.FrequentLongsSketch;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggBigDecimalFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctIntFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggIntFunction;
import org.apache.pinot.core.query.aggregation.function.array.SumArrayDoubleAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.array.SumArrayLongAggregationFunction;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.segment.local.customobject.SerializedFrequentLongsSketch;
import org.apache.pinot.segment.local.customobject.SerializedFrequentStringsSketch;
import org.apache.pinot.segment.local.customobject.SerializedKLL;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Merges the final results that servers or leaf stage workers return for partitions of the aggregated values, in the
/// form they are transported in.
public class FinalResultMergeTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("column");

  @Test
  public void testHistogram() {
    HistogramAggregationFunction function = new HistogramAggregationFunction(List.of(COLUMN,
        ExpressionContext.forLiteral(Literal.doubleValue(0)), ExpressionContext.forLiteral(Literal.doubleValue(10)),
        ExpressionContext.forLiteral(Literal.intValue(2))), false);
    double[] finalResult1 = {1, 2};
    double[] finalResult2 = {3, 4};
    assertEquals(function.mergeFinalResult(finalResult1, finalResult2), new double[]{4, 6});
    assertEquals(finalResult1, new double[]{1, 2});
    assertEquals(finalResult2, new double[]{3, 4});
  }

  @Test
  public void testSumArrayLong() {
    SumArrayLongAggregationFunction function = new SumArrayLongAggregationFunction(List.of(COLUMN), false);
    long[] longer = {1, 2, 3};
    long[] shorter = {4};
    assertEquals(function.mergeFinalResult(longer, shorter), new long[]{5, 2, 3});
    assertEquals(function.mergeFinalResult(shorter, longer), new long[]{5, 2, 3});
    assertEquals(longer, new long[]{1, 2, 3});
    assertEquals(shorter, new long[]{4});
  }

  @Test
  public void testSumArrayDouble() {
    SumArrayDoubleAggregationFunction function = new SumArrayDoubleAggregationFunction(List.of(COLUMN), false);
    double[] longer = {1, 2};
    double[] shorter = {0.5};
    assertEquals(function.mergeFinalResult(longer, shorter), new double[]{1.5, 2});
    assertEquals(function.mergeFinalResult(shorter, longer), new double[]{1.5, 2});
    assertEquals(longer, new double[]{1, 2});
    assertEquals(shorter, new double[]{0.5});
  }

  @Test
  public void testArrayAgg() {
    ArrayAggIntFunction function = new ArrayAggIntFunction(COLUMN, DataType.INT, false);
    assertEquals(function.mergeFinalResult(new int[]{1, 2}, new int[]{2}), new int[]{1, 2, 2});
    assertEquals(function.mergeFinalResult(new int[0], new int[]{3}), new int[]{3});
    assertEquals(function.mergeFinalResult(new int[]{3}, new int[0]), new int[]{3});

    ArrayAggDistinctIntFunction distinctFunction = new ArrayAggDistinctIntFunction(COLUMN, DataType.INT, false);
    assertEquals(distinctFunction.mergeFinalResult(new int[]{1, 2}, new int[]{3}), new int[]{1, 2, 3});

    ArrayAggBigDecimalFunction bigDecimalFunction = new ArrayAggBigDecimalFunction(COLUMN, false);
    BigDecimal[] merged =
        bigDecimalFunction.mergeFinalResult(new BigDecimal[]{BigDecimal.ONE}, new BigDecimal[]{BigDecimal.TEN});
    assertEquals(merged, new BigDecimal[]{BigDecimal.ONE, BigDecimal.TEN});
  }

  @Test
  public void testPercentileRawKLL() {
    List<ExpressionContext> arguments = List.of(COLUMN, ExpressionContext.forLiteral(Literal.doubleValue(50)));
    for (PercentileKLLAggregationFunction function : List.of(new PercentileRawKLLAggregationFunction(arguments, false),
        new PercentileRawKLLMVAggregationFunction(arguments, false))) {
      KllDoublesSketch sketch1 = KllDoublesSketch.newHeapInstance();
      sketch1.update(1);
      sketch1.update(2);
      KllDoublesSketch sketch2 = KllDoublesSketch.newHeapInstance();
      sketch2.update(3);
      String merged = (String) function.mergeFinalResult(new SerializedKLL(sketch1, 50).toString(),
          new SerializedKLL(sketch2, 50).toString());
      KllDoublesSketch mergedSketch =
          KllDoublesSketch.heapify(MemorySegment.ofArray(Base64.getDecoder().decode(merged)));
      assertEquals(mergedSketch.getN(), 3);
      assertEquals(mergedSketch.getMinItem(), 1.0);
      assertEquals(mergedSketch.getMaxItem(), 3.0);
    }
  }

  @Test
  public void testFrequentLongsSketch() {
    FrequentLongsSketchAggregationFunction function =
        new FrequentLongsSketchAggregationFunction(List.of(COLUMN), false);
    FrequentLongsSketch sketch1 = new FrequentLongsSketch(8);
    sketch1.update(1, 3);
    sketch1.update(2);
    FrequentLongsSketch sketch2 = new FrequentLongsSketch(8);
    sketch2.update(3, 2);
    String merged = function.mergeFinalResult(new SerializedFrequentLongsSketch(sketch1).toString(),
        new SerializedFrequentLongsSketch(sketch2).toString());
    FrequentLongsSketch mergedSketch =
        FrequentLongsSketch.getInstance(MemorySegment.ofArray(Base64.getDecoder().decode(merged)));
    assertEquals(mergedSketch.getStreamLength(), 6);
    assertEquals(mergedSketch.getEstimate(1), 3);
    assertEquals(mergedSketch.getEstimate(2), 1);
    assertEquals(mergedSketch.getEstimate(3), 2);
  }

  @Test
  public void testFrequentStringsSketch() {
    FrequentStringsSketchAggregationFunction function =
        new FrequentStringsSketchAggregationFunction(List.of(COLUMN), false);
    FrequentItemsSketch<String> sketch1 = new FrequentItemsSketch<>(8);
    sketch1.update("a", 3);
    sketch1.update("b");
    FrequentItemsSketch<String> sketch2 = new FrequentItemsSketch<>(8);
    sketch2.update("c", 2);
    String merged = function.mergeFinalResult(new SerializedFrequentStringsSketch(sketch1).toString(),
        new SerializedFrequentStringsSketch(sketch2).toString());
    FrequentItemsSketch<String> mergedSketch = FrequentItemsSketch.getInstance(
        MemorySegment.ofArray(Base64.getDecoder().decode(merged)), new ArrayOfStringsSerDe());
    assertEquals(mergedSketch.getStreamLength(), 6);
    assertEquals(mergedSketch.getEstimate("a"), 3);
    assertEquals(mergedSketch.getEstimate("b"), 1);
    assertEquals(mergedSketch.getEstimate("c"), 2);
  }

  @Test
  public void testIdSet()
      throws IOException {
    IdSetAggregationFunction function = new IdSetAggregationFunction(List.of(COLUMN), false);
    IdSet idSet1 = IdSets.create(DataType.INT);
    idSet1.add(1);
    IdSet idSet2 = IdSets.create(DataType.INT);
    idSet2.add(2);
    IdSet merged = IdSets.fromBase64String(function.mergeFinalResult(idSet1.toBase64String(), idSet2.toBase64String()));
    assertTrue(merged.contains(1));
    assertTrue(merged.contains(2));
    assertFalse(merged.contains(3));
  }
}
