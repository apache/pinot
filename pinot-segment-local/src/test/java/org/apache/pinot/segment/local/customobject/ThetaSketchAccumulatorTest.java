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
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ThetaSketchAccumulatorTest {
  private SetOperationBuilder setOperationBuilder;

  @BeforeMethod
  public void setUp() {
    setOperationBuilder = new SetOperationBuilder();
  }

  @Test
  public void testEmptyAccumulator() {
    ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(setOperationBuilder, false, 2);
    Assert.assertTrue(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), 0.0);
  }

  @Test
  public void testAccumulatorWithSingleSketch() {
    UpdateSketch input = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input::update);
    Sketch sketch = input.compact();

    ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(setOperationBuilder, false, 2);
    accumulator.apply(sketch);

    Assert.assertFalse(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), sketch.getEstimate());
  }

  @Test
  public void testAccumulatorMerge() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    Sketch sketch1 = input1.compact();
    UpdateSketch input2 = Sketches.updateSketchBuilder().build();
    IntStream.range(1000, 2000).forEach(input2::update);
    Sketch sketch2 = input2.compact();

    ThetaSketchAccumulator accumulator1 = new ThetaSketchAccumulator(setOperationBuilder, true, 3);
    accumulator1.apply(sketch1);
    ThetaSketchAccumulator accumulator2 = new ThetaSketchAccumulator(setOperationBuilder, true, 3);
    accumulator2.apply(sketch2);
    accumulator1.merge(accumulator2);

    Assert.assertEquals(accumulator1.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate());
  }

  @Test
  public void testThresholdBehavior() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    Sketch sketch1 = input1.compact();
    UpdateSketch input2 = Sketches.updateSketchBuilder().build();
    IntStream.range(1000, 2000).forEach(input2::update);
    Sketch sketch2 = input2.compact();

    ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(setOperationBuilder, true, 3);
    accumulator.apply(sketch1);
    accumulator.apply(sketch2);

    Assert.assertEquals(accumulator.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate());
  }

  @Test
  public void testUnionWithEmptyInput() {
    ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(setOperationBuilder, true, 3);
    ThetaSketchAccumulator emptyAccumulator = new ThetaSketchAccumulator(setOperationBuilder, true, 3);

    accumulator.merge(emptyAccumulator);

    Assert.assertTrue(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), 0.0);
  }
}
