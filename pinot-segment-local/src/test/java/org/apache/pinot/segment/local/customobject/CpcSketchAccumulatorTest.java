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
import org.testng.Assert;
import org.testng.annotations.Test;


public class CpcSketchAccumulatorTest {
  private final int _lgNominalEntries = 20;
  private final double _epsilon = 0.5;

  @Test
  public void testEmptyAccumulator() {
    CpcSketchAccumulator accumulator = new CpcSketchAccumulator(_lgNominalEntries, 2);
    Assert.assertTrue(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), 0.0);
  }

  @Test
  public void testAccumulatorWithSingleSketch() {
    CpcSketch sketch = new CpcSketch(_lgNominalEntries);
    IntStream.range(0, 1000).forEach(sketch::update);

    CpcSketchAccumulator accumulator = new CpcSketchAccumulator(_lgNominalEntries, 2);
    accumulator.apply(sketch);

    Assert.assertFalse(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), sketch.getEstimate());
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

    Assert.assertEquals(accumulator1.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate(),
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

    Assert.assertEquals(accumulator.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate(), _epsilon);
  }

  @Test
  public void testUnionWithEmptyInput() {
    CpcSketchAccumulator accumulator = new CpcSketchAccumulator(_lgNominalEntries, 3);
    CpcSketchAccumulator emptyAccumulator = new CpcSketchAccumulator(_lgNominalEntries, 3);

    accumulator.merge(emptyAccumulator);

    Assert.assertTrue(accumulator.isEmpty());
    Assert.assertEquals(accumulator.getResult().getEstimate(), 0.0);
  }
}
