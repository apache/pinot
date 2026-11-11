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
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class CpcSketchAccumulatorTest {
  private final int _lgNominalEntries = 20;
  private final double _epsilon = 0.5;

  @Test
  public void testEmptyAccumulator() {
    CpcSketchAccumulator accumulator = new CpcSketchAccumulator(_lgNominalEntries, 2);
    assertTrue(accumulator.isEmpty());
    assertEquals(accumulator.getResult().getEstimate(), 0.0);
  }

  @Test
  public void testAccumulatorWithSingleSketch() {
    CpcSketch sketch = new CpcSketch(_lgNominalEntries);
    IntStream.range(0, 1000).forEach(sketch::update);

    CpcSketchAccumulator accumulator = new CpcSketchAccumulator(_lgNominalEntries, 2);
    accumulator.apply(sketch);

    assertFalse(accumulator.isEmpty());
    assertEquals(accumulator.getResult().getEstimate(), sketch.getEstimate());
  }

  @Test
  public void testSeededSketchIsReturnedUnchanged() {
    for (int numEntries : new int[]{0, 1000}) {
      CpcSketch sketch = new CpcSketch(12);
      IntStream.range(0, numEntries).forEach(sketch::update);
      for (int threshold = 1; threshold <= 3; threshold++) {
        CpcSketchAccumulator accumulator = new CpcSketchAccumulator(10, threshold, sketch);

        assertFalse(accumulator.isEmpty());
        assertEquals(accumulator.getNumInputs(), 1);
        assertSame(accumulator.getResult(), sketch);
        assertSame(accumulator.getResult(), sketch);
      }
    }
  }

  @Test
  public void testMergeAfterReadingSeededSketch() {
    CpcSketch input1 = new CpcSketch(12);
    IntStream.range(0, 1000).forEach(input1::update);
    byte[] bytes = input1.toByteArray();
    CpcSketch sketch2 = new CpcSketch(12);
    IntStream.range(500, 1500).forEach(sketch2::update);
    for (int threshold = 1; threshold <= 3; threshold++) {
      CpcSketch sketch1 = CpcSketch.heapify(bytes);
      CpcSketchAccumulator accumulator = new CpcSketchAccumulator(10, threshold, sketch1);
      assertEquals(accumulator.getResult().toByteArray(), bytes);
      accumulator.merge(new CpcSketchAccumulator(10, threshold, sketch2));

      CpcUnion expected = new CpcUnion(10);
      expected.update(sketch1);
      expected.update(sketch2);
      CpcSketch result = accumulator.getResult();
      assertEquals(accumulator.getNumInputs(), 2);
      assertEquals(result.getLgK(), 10);
      assertEquals(result.getEstimate(), expected.getResult().getEstimate(), 0.0);
    }
  }

  @Test
  public void testAccumulatorMerge() {
    CpcSketch sketch1 = new CpcSketch(_lgNominalEntries);
    IntStream.range(0, 1000).forEach(sketch1::update);
    CpcSketch sketch2 = new CpcSketch(_lgNominalEntries);
    IntStream.range(1000, 2000).forEach(sketch2::update);

    CpcSketchAccumulator accumulator1 = new CpcSketchAccumulator(_lgNominalEntries, 3);
    accumulator1.apply(sketch1);
    CpcSketchAccumulator accumulator2 = new CpcSketchAccumulator(_lgNominalEntries, 3);
    accumulator2.apply(sketch2);
    accumulator1.merge(accumulator2);

    assertEquals(accumulator1.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate(),
        _epsilon);
  }

  @Test
  public void testThresholdBehavior() {
    CpcSketch sketch1 = new CpcSketch(_lgNominalEntries);
    IntStream.range(0, 1000).forEach(sketch1::update);
    CpcSketch sketch2 = new CpcSketch(_lgNominalEntries);
    IntStream.range(1000, 2000).forEach(sketch2::update);

    CpcSketchAccumulator accumulator = new CpcSketchAccumulator(_lgNominalEntries, 3);
    accumulator.apply(sketch1);
    accumulator.apply(sketch2);

    assertEquals(accumulator.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate(), _epsilon);
  }

  @Test
  public void testInputsSurviveRepeatedFlushes() {
    for (int threshold = 1; threshold <= 4; threshold++) {
      CpcSketchAccumulator accumulator = new CpcSketchAccumulator(_lgNominalEntries, threshold);
      CpcUnion expected = new CpcUnion(_lgNominalEntries);
      for (int i = 0; i < 7; i++) {
        CpcSketch sketch = new CpcSketch(_lgNominalEntries);
        int base = i * 1000;
        IntStream.range(base, base + 1000).forEach(sketch::update);
        accumulator.apply(sketch);
        expected.update(sketch);
      }
      assertEquals(accumulator.getResult().getEstimate(), expected.getResult().getEstimate(), _epsilon,
          "threshold " + threshold);
    }
  }

  @Test
  public void testRepeatedGetResultIsStable() {
    CpcSketchAccumulator accumulator = new CpcSketchAccumulator(_lgNominalEntries, 2);
    for (int i = 0; i < 5; i++) {
      CpcSketch sketch = new CpcSketch(_lgNominalEntries);
      int base = i * 1000;
      IntStream.range(base, base + 1000).forEach(sketch::update);
      accumulator.apply(sketch);
    }
    double first = accumulator.getResult().getEstimate();
    assertEquals(accumulator.getResult().getEstimate(), first, 0.0);
    assertEquals(accumulator.getResult().getEstimate(), first, 0.0);
  }

  @Test
  public void testUnionWithEmptyInput() {
    CpcSketchAccumulator accumulator = new CpcSketchAccumulator(_lgNominalEntries, 3);
    CpcSketchAccumulator emptyAccumulator = new CpcSketchAccumulator(_lgNominalEntries, 3);

    accumulator.merge(emptyAccumulator);

    assertTrue(accumulator.isEmpty());
    assertEquals(accumulator.getResult().getEstimate(), 0.0);
  }
}
