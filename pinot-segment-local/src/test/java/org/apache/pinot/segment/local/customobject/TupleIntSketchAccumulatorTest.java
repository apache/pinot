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

package org.apache.pinot.segment.local.customobject;

import java.util.stream.IntStream;
import org.apache.datasketches.tuple.CompactTupleSketch;
import org.apache.datasketches.tuple.TupleSketch;
import org.apache.datasketches.tuple.TupleSketchIterator;
import org.apache.datasketches.tuple.TupleUnion;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.datasketches.tuple.aninteger.IntegerTupleSketch;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TupleIntSketchAccumulatorTest {
  private IntegerSummarySetOperations _setOps;
  private final int _lgK = 12;
  private final int _nominalEntries = (int) Math.pow(2, _lgK);

  @BeforeMethod
  public void setUp() {
    _setOps = new IntegerSummarySetOperations(IntegerSummary.Mode.Sum, IntegerSummary.Mode.Sum);
  }

  @Test
  public void testEmptyAccumulator() {
    TupleIntSketchAccumulator accumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, 2);
    Assert.assertTrue(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), 0.0);
  }

  @Test
  public void testAccumulatorWithSingleSketch() {
    IntegerTupleSketch input = new IntegerTupleSketch(_lgK, IntegerSummary.Mode.Sum);
    IntStream.range(0, 1000).forEach(i -> input.update(i, 1));
    CompactTupleSketch<IntegerSummary> sketch = input.compact();

    TupleIntSketchAccumulator accumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, 2);
    accumulator.apply(sketch);

    Assert.assertFalse(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), sketch.getEstimate());
  }

  @Test
  public void testAccumulatorMerge() {
    IntegerTupleSketch input1 = new IntegerTupleSketch(_lgK, IntegerSummary.Mode.Sum);
    IntStream.range(0, 1000).forEach(i -> input1.update(i, 1));
    CompactTupleSketch<IntegerSummary> sketch1 = input1.compact();
    IntegerTupleSketch input2 = new IntegerTupleSketch(_lgK, IntegerSummary.Mode.Sum);
    IntStream.range(1000, 2000).forEach(i -> input2.update(i, 1));
    CompactTupleSketch<IntegerSummary> sketch2 = input2.compact();

    TupleIntSketchAccumulator accumulator1 = new TupleIntSketchAccumulator(_setOps, _nominalEntries, 3);
    accumulator1.apply(sketch1);
    TupleIntSketchAccumulator accumulator2 = new TupleIntSketchAccumulator(_setOps, _nominalEntries, 3);
    accumulator2.apply(sketch2);
    accumulator1.merge(accumulator2);

    Assert.assertEquals(accumulator1.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate());
  }

  @Test
  public void testThresholdBehavior() {
    IntegerTupleSketch input1 = new IntegerTupleSketch(_lgK, IntegerSummary.Mode.Sum);
    IntStream.range(0, 1000).forEach(i -> input1.update(i, 1));
    CompactTupleSketch<IntegerSummary> sketch1 = input1.compact();
    IntegerTupleSketch input2 = new IntegerTupleSketch(_lgK, IntegerSummary.Mode.Sum);
    IntStream.range(1000, 2000).forEach(i -> input2.update(i, 1));
    CompactTupleSketch<IntegerSummary> sketch2 = input2.compact();

    TupleIntSketchAccumulator accumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, 3);
    accumulator.apply(sketch1);
    accumulator.apply(sketch2);

    Assert.assertEquals(accumulator.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate());
  }

  // Summaries are summed, so a sketch merged twice doubles a summary while leaving the estimate right.
  @Test
  public void testInputsSurviveRepeatedFlushes() {
    for (int threshold = 1; threshold <= 4; threshold++) {
      TupleIntSketchAccumulator accumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, threshold);
      TupleUnion<IntegerSummary> expected = new TupleUnion<>(_nominalEntries, _setOps);
      for (int i = 0; i < 7; i++) {
        IntegerTupleSketch input = new IntegerTupleSketch(_lgK, IntegerSummary.Mode.Sum);
        int base = i * 1000;
        IntStream.range(base, base + 1000).forEach(k -> input.update(k, 1));
        CompactTupleSketch<IntegerSummary> sketch = input.compact();
        accumulator.apply(sketch);
        expected.union(sketch);
      }
      TupleSketch<IntegerSummary> actual = accumulator.getResult();
      TupleSketch<IntegerSummary> want = expected.getResult();
      Assert.assertEquals(actual.getEstimate(), want.getEstimate(), 0.0, "threshold " + threshold);
      Assert.assertEquals(summarySum(actual), summarySum(want), "threshold " + threshold);
    }
  }

  @Test
  public void testRepeatedGetResultIsStable() {
    TupleIntSketchAccumulator accumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, 2);
    for (int i = 0; i < 5; i++) {
      IntegerTupleSketch input = new IntegerTupleSketch(_lgK, IntegerSummary.Mode.Sum);
      int base = i * 1000;
      IntStream.range(base, base + 1000).forEach(k -> input.update(k, 1));
      accumulator.apply(input.compact());
    }
    long first = summarySum(accumulator.getResult());
    Assert.assertEquals(summarySum(accumulator.getResult()), first);
    Assert.assertEquals(summarySum(accumulator.getResult()), first);
  }

  private static long summarySum(TupleSketch<IntegerSummary> sketch) {
    long sum = 0;
    TupleSketchIterator<IntegerSummary> it = sketch.iterator();
    while (it.next()) {
      sum += it.getSummary().getValue();
    }
    return sum;
  }

  @Test
  public void testUnionWithEmptyInput() {
    TupleIntSketchAccumulator accumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, 3);
    TupleIntSketchAccumulator emptyAccumulator = new TupleIntSketchAccumulator(_setOps, _nominalEntries, 3);

    accumulator.merge(emptyAccumulator);

    Assert.assertTrue(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), 0.0);
  }
}
