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
package org.apache.pinot.core.function.scalar;

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import java.math.BigDecimal;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.utils.UltraLogLogUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SketchFunctionsTest {

  private double thetaEstimate(byte[] bytes) {
    return ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(bytes).getEstimate();
  }

  byte[] _bytes = {1, 2, 3};

  Object[] _inputs = {
      "string", 1, 1L, 1.0f, 1.0d, BigDecimal.valueOf(1), _bytes
  };

  @Test
  public void testThetaSketchCreation() {
    for (Object i : _inputs) {
      Assert.assertEquals(thetaEstimate(SketchFunctions.toThetaSketch(i)), 1.0);
      Assert.assertEquals(thetaEstimate(SketchFunctions.toThetaSketch(i, 1024)), 1.0);
    }
    Assert.assertEquals(thetaEstimate(SketchFunctions.toThetaSketch(null)), 0.0);
    Assert.assertEquals(thetaEstimate(SketchFunctions.toThetaSketch(null, 1024)), 0.0);
    Assert.assertThrows(IllegalArgumentException.class, () -> SketchFunctions.toThetaSketch(new Object()));
    Assert.assertThrows(IllegalArgumentException.class, () -> SketchFunctions.toThetaSketch(new Object(), 1024));
  }

  @Test
  public void thetaThetaSketchSummary() {
    for (Object i : _inputs) {
      Sketch sketch = Sketches.wrapSketch(Memory.wrap(SketchFunctions.toThetaSketch(i)));
      Assert.assertEquals(SketchFunctions.thetaSketchToString(sketch), sketch.toString());
    }
    Assert.assertThrows(RuntimeException.class, () -> SketchFunctions.thetaSketchToString(new Object()));
  }

  private long hllEstimate(byte[] bytes) {
    return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(bytes).cardinality();
  }

  @Test
  public void hllCreation() {
    for (Object i : _inputs) {
      Assert.assertEquals(hllEstimate(SketchFunctions.toHLL(i)), 1);
      Assert.assertEquals(hllEstimate(SketchFunctions.toHLL(i, 8)), 1);
    }
    Assert.assertEquals(hllEstimate(SketchFunctions.toHLL(null)), 0);
    Assert.assertEquals(hllEstimate(SketchFunctions.toHLL(null, 8)), 0);
  }

  private double intTupleEstimate(byte[] bytes) {
    return ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(bytes).getEstimate();
  }

  @Test
  public void intTupleSumCreation() {
    for (Object i : _inputs) {
      Assert.assertEquals(intTupleEstimate(SketchFunctions.toIntegerSumTupleSketch(i, 1)), 1.0d);
      Assert.assertEquals(intTupleEstimate(SketchFunctions.toIntegerSumTupleSketch(i, 1, 16)), 1.0d);
    }
    Assert.assertEquals(intTupleEstimate(SketchFunctions.toIntegerSumTupleSketch(null, 1)), 0.0d);
    Assert.assertEquals(intTupleEstimate(SketchFunctions.toIntegerSumTupleSketch(null, 1, 16)), 0.0d);
    Assert.assertThrows(IllegalArgumentException.class, () -> SketchFunctions.toIntegerSumTupleSketch(new Object(), 1));
    Assert.assertThrows(IllegalArgumentException.class,
        () -> SketchFunctions.toIntegerSumTupleSketch(new Object(), 1, 1024));
  }

  private double cpcEstimate(byte[] bytes) {
    return ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(bytes).getEstimate();
  }

  @Test
  public void testCpcCreation() {
    for (Object i : _inputs) {
      Assert.assertEquals(cpcEstimate(SketchFunctions.toCpcSketch(i)), 1.0);
      Assert.assertEquals(cpcEstimate(SketchFunctions.toCpcSketch(i, 11)), 1.0);
    }
    Assert.assertEquals(cpcEstimate(SketchFunctions.toCpcSketch(null)), 0.0);
    Assert.assertEquals(cpcEstimate(SketchFunctions.toCpcSketch(null, 11)), 0.0);
    Assert.assertThrows(IllegalArgumentException.class, () -> SketchFunctions.toCpcSketch(new Object()));
    Assert.assertThrows(IllegalArgumentException.class, () -> SketchFunctions.toCpcSketch(new Object(), 11));
  }

  @Test
  public void thetaCpcSketchToString() {
    for (Object i : _inputs) {
      CpcSketch sketch = CpcSketch.heapify(Memory.wrap(SketchFunctions.toCpcSketch(i)));
      Assert.assertEquals(SketchFunctions.cpcSketchToString(sketch), sketch.toString());
    }
    Assert.assertThrows(RuntimeException.class, () -> SketchFunctions.cpcSketchToString(new Object()));
  }

  private long ullEstimate(byte[] bytes) {
    // round it to a long to make it easier to assert on
    return Math.round(ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(bytes).getDistinctCountEstimate());
  }

  @Test
  public void testULLCreation() {
    for (Object i : _inputs) {
      Assert.assertEquals(ullEstimate(SketchFunctions.toULL(i)), 1);
      Assert.assertEquals(ullEstimate(SketchFunctions.toULL(i, 11)), 1);
    }
    Assert.assertEquals(ullEstimate(SketchFunctions.toULL(null)), 0);
    Assert.assertEquals(ullEstimate(SketchFunctions.toULL(null, 11)), 0);
    Assert.assertThrows(IllegalArgumentException.class, () -> SketchFunctions.toULL(new Object()));
    Assert.assertThrows(IllegalArgumentException.class, () -> SketchFunctions.toULL(new Object(), 11));
  }

  @Test
  public void testULLLoading() {
    for (Object i : _inputs) {
      UltraLogLog ull = UltraLogLog.create(12);
      UltraLogLogUtils.hashObject(i).ifPresent(ull::add);
      byte[] loaded = SketchFunctions.fromULL(ull.getState());
      UltraLogLog deserialized = ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.deserialize(loaded);
      Assert.assertEquals(deserialized.getState(), ull.getState());
    }
  }
}
