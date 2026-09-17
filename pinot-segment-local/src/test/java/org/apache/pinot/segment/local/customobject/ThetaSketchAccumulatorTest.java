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
import org.apache.datasketches.theta.ThetaSetOperationBuilder;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.ThetaUnion;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ThetaSketchAccumulatorTest {
  private ThetaSetOperationBuilder _setOperationBuilder;

  @BeforeMethod
  public void setUp() {
    _setOperationBuilder = new ThetaSetOperationBuilder();
  }

  @Test
  public void testEmptyAccumulator() {
    ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(_setOperationBuilder, 2);
    Assert.assertTrue(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), 0.0);
  }

  @Test
  public void testAccumulatorWithSingleSketch() {
    UpdatableThetaSketch input = UpdatableThetaSketch.builder().build();
    IntStream.range(0, 1000).forEach(input::update);
    ThetaSketch sketch = input.compact();

    ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(_setOperationBuilder, 2);
    accumulator.apply(sketch);

    Assert.assertFalse(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), sketch.getEstimate());
  }

  @Test
  public void testAccumulatorMerge() {
    UpdatableThetaSketch input1 = UpdatableThetaSketch.builder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    ThetaSketch sketch1 = input1.compact();
    UpdatableThetaSketch input2 = UpdatableThetaSketch.builder().build();
    IntStream.range(1000, 2000).forEach(input2::update);
    ThetaSketch sketch2 = input2.compact();

    ThetaSketchAccumulator accumulator1 = new ThetaSketchAccumulator(_setOperationBuilder, 3);
    accumulator1.apply(sketch1);
    ThetaSketchAccumulator accumulator2 = new ThetaSketchAccumulator(_setOperationBuilder, 3);
    accumulator2.apply(sketch2);
    accumulator1.merge(accumulator2);

    Assert.assertEquals(accumulator1.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate());
  }

  @Test
  public void testThresholdBehavior() {
    UpdatableThetaSketch input1 = UpdatableThetaSketch.builder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    ThetaSketch sketch1 = input1.compact();
    UpdatableThetaSketch input2 = UpdatableThetaSketch.builder().build();
    IntStream.range(1000, 2000).forEach(input2::update);
    ThetaSketch sketch2 = input2.compact();

    ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(_setOperationBuilder, 3);
    accumulator.apply(sketch1);
    accumulator.apply(sketch2);

    Assert.assertEquals(accumulator.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate());
  }

  @Test
  public void testInputsSurviveRepeatedFlushes() {
    for (int threshold = 1; threshold <= 4; threshold++) {
      ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(_setOperationBuilder, threshold);
      ThetaUnion expected = _setOperationBuilder.buildUnion();
      for (int i = 0; i < 7; i++) {
        UpdatableThetaSketch input = UpdatableThetaSketch.builder().build();
        int base = i * 1000;
        IntStream.range(base, base + 1000).forEach(input::update);
        ThetaSketch sketch = input.compact();
        accumulator.apply(sketch);
        expected.union(sketch);
      }
      Assert.assertEquals(accumulator.getResult().getEstimate(), expected.getResult().getEstimate(), 0.0,
          "threshold " + threshold);
    }
  }

  @Test
  public void testRepeatedGetResultIsStable() {
    ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(_setOperationBuilder, 2);
    for (int i = 0; i < 5; i++) {
      UpdatableThetaSketch input = UpdatableThetaSketch.builder().build();
      int base = i * 1000;
      IntStream.range(base, base + 1000).forEach(input::update);
      accumulator.apply(input.compact());
    }
    double first = accumulator.getResult().getEstimate();
    Assert.assertEquals(accumulator.getResult().getEstimate(), first, 0.0);
    Assert.assertEquals(accumulator.getResult().getEstimate(), first, 0.0);
  }

  @Test
  public void testUnionWithEmptyInput() {
    ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(_setOperationBuilder, 3);
    ThetaSketchAccumulator emptyAccumulator = new ThetaSketchAccumulator(_setOperationBuilder, 3);

    accumulator.merge(emptyAccumulator);

    Assert.assertTrue(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), 0.0);
  }
}
