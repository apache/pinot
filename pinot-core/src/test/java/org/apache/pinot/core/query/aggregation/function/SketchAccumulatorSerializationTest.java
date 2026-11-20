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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.datasketches.theta.ThetaSetOperationBuilder;
import org.apache.datasketches.theta.ThetaUnion;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.tuple.TupleUnion;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.datasketches.tuple.aninteger.IntegerTupleSketch;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Checks that deserialized sketches retain their bytes and use the receiving query's settings when merged.
/// All mutable state belongs to individual test invocations, so the tests can run concurrently.
public class SketchAccumulatorSerializationTest {
  @DataProvider
  public Object[][] thresholds() {
    return new Object[][]{{1}, {3}};
  }

  @Test(dataProvider = "thresholds")
  public void testTuple(int threshold) {
    IntegerTupleSketch left = new IntegerTupleSketch(12, IntegerSummary.Mode.Sum);
    IntegerTupleSketch right = new IntegerTupleSketch(12, IntegerSummary.Mode.Sum);
    for (int i = 0; i < 1000; i++) {
      left.update(i, 1);
      right.update(i + 500, 2);
    }
    TupleUnion<IntegerSummary> expected =
        new TupleUnion<>(128, new IntegerSummarySetOperations(IntegerSummary.Mode.Max, IntegerSummary.Mode.Max));
    expected.union(left.compact());
    expected.union(right.compact());
    assertLifecycle(new IntegerTupleSketchAggregationFunction(arguments(threshold), IntegerSummary.Mode.Max, false),
        48, left.compact().toByteArray(), right.compact().toByteArray(), expected.getResult().toByteArray());
  }

  @Test(dataProvider = "thresholds")
  public void testCpc(int threshold) {
    CpcSketch left = new CpcSketch(12);
    CpcSketch right = new CpcSketch(12);
    for (int i = 0; i < 1000; i++) {
      left.update(i);
      right.update(i + 500);
    }
    CpcUnion expected = new CpcUnion(7);
    expected.update(left);
    expected.update(right);
    assertLifecycle(new DistinctCountCPCSketchAggregationFunction(arguments(threshold), false), 49,
        left.toByteArray(), right.toByteArray(), expected.getResult().toByteArray());
  }

  @Test(dataProvider = "thresholds")
  public void testTheta(int threshold) {
    UpdatableThetaSketch left = UpdatableThetaSketch.builder().build();
    UpdatableThetaSketch right = UpdatableThetaSketch.builder().build();
    for (int i = 0; i < 1000; i++) {
      left.update(i);
      right.update(i + 500);
    }
    ThetaUnion expected = new ThetaSetOperationBuilder().setNominalEntries(128).buildUnion();
    expected.union(left.compact());
    expected.union(right.compact());
    assertLifecycle(new DistinctCountThetaSketchAggregationFunction(arguments(threshold), false), 21,
        thetaList(left.compact().toByteArray()), thetaList(right.compact().toByteArray()),
        thetaList(expected.getResult(false, null).toByteArray()));
  }

  private static List<ExpressionContext> arguments(int threshold) {
    return List.of(ExpressionContext.forIdentifier("column"), ExpressionContext.forLiteral(
        Literal.stringValue("nominalEntries=128;accumulatorThreshold=" + threshold)));
  }

  private static byte[] thetaList(byte[] sketch) {
    return ByteBuffer.allocate(3 * Integer.BYTES + sketch.length).putInt(1).putInt(47).putInt(sketch.length)
        .put(sketch).array();
  }

  private static <I> void assertLifecycle(AggregationFunction<I, ?> function, int type, byte[] leftBytes,
      byte[] rightBytes, byte[] expectedBytes) {
    I left = function.deserializeIntermediateResult(new CustomObject(type, ByteBuffer.wrap(leftBytes)));
    // Receiving one sketch must not downsample it, including when the accumulator threshold is one.
    AggregationFunction.SerializedResult serialized = function.serializeIntermediateResult(left);
    assertEquals(serialized.getType(), type);
    assertEquals(serialized.getBytes(), leftBytes);
    I right = function.deserializeIntermediateResult(new CustomObject(type, ByteBuffer.wrap(rightBytes)));
    I merged = function.merge(left, right);
    assertEquals(function.serializeIntermediateResult(merged).getBytes(), expectedBytes);
  }
}
