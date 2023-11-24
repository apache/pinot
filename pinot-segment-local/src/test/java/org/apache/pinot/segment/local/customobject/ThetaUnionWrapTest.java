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


public class ThetaUnionWrapTest {
  private SetOperationBuilder _setOperationBuilder;

  @BeforeMethod
  public void setUp() {
    _setOperationBuilder = new SetOperationBuilder();
  }

  @Test
  public void testEmptyUnionWrap() {
    ThetaUnionWrap thetaUnionWrap = new ThetaUnionWrap(_setOperationBuilder, false);
    Assert.assertTrue(thetaUnionWrap.isEmpty());
    Assert.assertEquals(thetaUnionWrap.getResult().getEstimate(), 0.0);
  }

  @Test
  public void testUnionWrapWithSingleSketch() {
    UpdateSketch input = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input::update);
    Sketch sketch = input.compact();

    ThetaUnionWrap thetaUnionWrap = new ThetaUnionWrap(_setOperationBuilder, false);
    thetaUnionWrap.apply(sketch);

    Assert.assertFalse(thetaUnionWrap.isEmpty());
    Assert.assertEquals(thetaUnionWrap.getResult().getEstimate(), sketch.getEstimate());
  }

  @Test
  public void testUnionWrapMerge() {
    UpdateSketch input1 = Sketches.updateSketchBuilder().build();
    IntStream.range(0, 1000).forEach(input1::update);
    Sketch sketch1 = input1.compact();
    UpdateSketch input2 = Sketches.updateSketchBuilder().build();
    IntStream.range(1000, 2000).forEach(input2::update);
    Sketch sketch2 = input2.compact();

    ThetaUnionWrap thetaUnionWrap1 = new ThetaUnionWrap(_setOperationBuilder, true);
    thetaUnionWrap1.apply(sketch1);
    ThetaUnionWrap thetaUnionWrap2 = new ThetaUnionWrap(_setOperationBuilder, true);
    thetaUnionWrap2.apply(sketch2);
    thetaUnionWrap1.merge(thetaUnionWrap2);

    Assert.assertEquals(thetaUnionWrap1.getResult().getEstimate(), sketch1.getEstimate() + sketch2.getEstimate());
  }

  @Test
  public void testUnionWithEmptyInput() {
    ThetaUnionWrap thetaUnionWrap = new ThetaUnionWrap(_setOperationBuilder, true);
    ThetaUnionWrap emptyUnionWrap = new ThetaUnionWrap(_setOperationBuilder, true);

    thetaUnionWrap.merge(emptyUnionWrap);

    Assert.assertTrue(thetaUnionWrap.isEmpty());
    Assert.assertEquals(thetaUnionWrap.getResult().getEstimate(), 0.0);
  }
}
