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
package org.apache.pinot.core.common;

import java.nio.ByteBuffer;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.tuple.aninteger.IntegerSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.segment.local.customobject.CpcSketchAccumulator;
import org.apache.pinot.segment.local.customobject.ThetaSketchAccumulator;
import org.apache.pinot.segment.local.customobject.TupleIntSketchAccumulator;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Asserts that {@code deserialize(ByteBuffer)} on the sketch serdes returns a sketch equivalent to
 * {@code deserialize(byte[])}, before and after the commit that switches the buffer path from
 * "allocate byte[] + memcpy + Memory.wrap(bytes)" to "Memory.wrap(buffer, LITTLE_ENDIAN)".
 *
 * <p>Each test also exercises a buffer whose position is non-zero in the underlying array (an inner
 * slice), reproducing the layout produced by
 * {@code DataTableImplV4.getCustomObject} on the broker reduce path.
 */
public class ObjectSerDeUtilsBufferParityTest {
  private static final int DISTINCT_VALUES = 10_000;

  @Test
  public void thetaSketchByteBufferParity() {
    Sketch original = buildThetaSketch();
    byte[] bytes = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(original);

    Sketch fromBytes = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(bytes);
    Sketch fromBuffer = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(ByteBuffer.wrap(bytes));
    Sketch fromInnerSlice = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(innerSlice(bytes));

    assertEquals(fromBuffer.getEstimate(), fromBytes.getEstimate());
    assertEquals(fromInnerSlice.getEstimate(), fromBytes.getEstimate());
  }

  @Test
  public void tupleSketchByteBufferParity() {
    IntegerSketch original = new IntegerSketch(12, IntegerSummary.Mode.Sum);
    for (int i = 0; i < DISTINCT_VALUES; i++) {
      original.update(i, 1);
    }
    byte[] bytes = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(original.compact());

    org.apache.datasketches.tuple.Sketch<IntegerSummary> fromBytes =
        ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(bytes);
    org.apache.datasketches.tuple.Sketch<IntegerSummary> fromBuffer =
        ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(ByteBuffer.wrap(bytes));
    org.apache.datasketches.tuple.Sketch<IntegerSummary> fromInnerSlice =
        ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(innerSlice(bytes));

    assertEquals(fromBuffer.getEstimate(), fromBytes.getEstimate());
    assertEquals(fromInnerSlice.getEstimate(), fromBytes.getEstimate());
  }

  @Test
  public void cpcSketchByteBufferParity() {
    CpcSketch original = new CpcSketch();
    for (int i = 0; i < DISTINCT_VALUES; i++) {
      original.update(i);
    }
    byte[] bytes = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(original);

    CpcSketch fromBytes = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(bytes);
    CpcSketch fromBuffer = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(ByteBuffer.wrap(bytes));
    CpcSketch fromInnerSlice = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(innerSlice(bytes));

    assertEquals(fromBuffer.getEstimate(), fromBytes.getEstimate());
    assertEquals(fromInnerSlice.getEstimate(), fromBytes.getEstimate());
  }

  @Test
  public void thetaSketchAccumulatorByteBufferParity() {
    Sketch original = buildThetaSketch();
    byte[] bytes = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(original);

    ThetaSketchAccumulator fromBytes = ObjectSerDeUtils.DATA_SKETCH_THETA_ACCUMULATOR_SER_DE.deserialize(bytes);
    ThetaSketchAccumulator fromBuffer =
        ObjectSerDeUtils.DATA_SKETCH_THETA_ACCUMULATOR_SER_DE.deserialize(ByteBuffer.wrap(bytes));
    ThetaSketchAccumulator fromInnerSlice =
        ObjectSerDeUtils.DATA_SKETCH_THETA_ACCUMULATOR_SER_DE.deserialize(innerSlice(bytes));

    assertEquals(fromBuffer.getResult().getEstimate(), fromBytes.getResult().getEstimate());
    assertEquals(fromInnerSlice.getResult().getEstimate(), fromBytes.getResult().getEstimate());
  }

  @Test
  public void tupleSketchAccumulatorByteBufferParity() {
    IntegerSketch original = new IntegerSketch(12, IntegerSummary.Mode.Sum);
    for (int i = 0; i < DISTINCT_VALUES; i++) {
      original.update(i, 1);
    }
    byte[] bytes = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(original.compact());

    TupleIntSketchAccumulator fromBytes = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_ACCUMULATOR_SER_DE.deserialize(bytes);
    TupleIntSketchAccumulator fromBuffer =
        ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_ACCUMULATOR_SER_DE.deserialize(ByteBuffer.wrap(bytes));
    TupleIntSketchAccumulator fromInnerSlice =
        ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_ACCUMULATOR_SER_DE.deserialize(innerSlice(bytes));

    assertEquals(fromBuffer.getResult().getEstimate(), fromBytes.getResult().getEstimate());
    assertEquals(fromInnerSlice.getResult().getEstimate(), fromBytes.getResult().getEstimate());
  }

  @Test
  public void cpcSketchAccumulatorByteBufferParity() {
    CpcSketch original = new CpcSketch();
    for (int i = 0; i < DISTINCT_VALUES; i++) {
      original.update(i);
    }
    byte[] bytes = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(original);

    CpcSketchAccumulator fromBytes = ObjectSerDeUtils.DATA_SKETCH_CPC_ACCUMULATOR_SER_DE.deserialize(bytes);
    CpcSketchAccumulator fromBuffer =
        ObjectSerDeUtils.DATA_SKETCH_CPC_ACCUMULATOR_SER_DE.deserialize(ByteBuffer.wrap(bytes));
    CpcSketchAccumulator fromInnerSlice =
        ObjectSerDeUtils.DATA_SKETCH_CPC_ACCUMULATOR_SER_DE.deserialize(innerSlice(bytes));

    assertEquals(fromBuffer.getResult().getEstimate(), fromBytes.getResult().getEstimate());
    assertEquals(fromInnerSlice.getResult().getEstimate(), fromBytes.getResult().getEstimate());
  }

  private static Sketch buildThetaSketch() {
    UpdateSketch sketch = new UpdateSketchBuilder().build();
    for (int i = 0; i < DISTINCT_VALUES; i++) {
      sketch.update(i);
    }
    return sketch.compact();
  }

  /**
   * Returns a buffer whose {@code position()} is non-zero in the backing array. This mirrors the
   * layout {@code DataTableImplV4.getCustomObject} produces, which is the actual production input
   * to the broker reduce path: {@code _variableSizeData.slice()} after the outer buffer has been
   * advanced past previous columns.
   */
  private static ByteBuffer innerSlice(byte[] bytes) {
    int padding = 7;
    byte[] padded = new byte[padding + bytes.length + padding];
    System.arraycopy(bytes, 0, padded, padding, bytes.length);
    ByteBuffer outer = ByteBuffer.wrap(padded);
    outer.position(padding);
    ByteBuffer inner = outer.slice();
    inner.limit(bytes.length);
    return inner;
  }
}
