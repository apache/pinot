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

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class PercentileSmartTDigestAggregationFunctionTest {
  private static final ExpressionContext EXPRESSION = ExpressionContext.forIdentifier("col");

  /// Exercises the complete capacity-preserving intermediate lifecycle: pass-through serde, materialization by
  /// merging another digest, and raw-list merging in both argument orders.
  @Test
  public void testCapacityPreservingIntermediateLifecycle() {
    int numCentroids = 51;
    byte[] small = PercentileRawTDigestAggregationFunctionTest.createSmallUnitCentroidDigest(numCentroids, 20.0, 60,
        100);
    PercentileSmartTDigestAggregationFunction function = createFunction();

    Object intermediateResult = function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small)));
    assertTrue(intermediateResult instanceof PercentileTDigestAccumulator);

    AggregationFunction.SerializedIntermediateResult passThrough =
        function.serializeIntermediateResult(intermediateResult);
    assertEquals(passThrough.getType(), ObjectSerDeUtils.ObjectType.TDigest.getValue());
    // Before the fix this was a verbose encoding with 51 centroids, which a plain
    // MergingDigest.fromBytes reader rejects with ArrayIndexOutOfBoundsException.
    assertEquals(MergingDigest.fromBytes(ByteBuffer.wrap(passThrough.getBytes())).size(), numCentroids);

    byte[] empty = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(TDigest.createMergingDigest(20.0));
    Object merged = function.merge(
        intermediateResult,
        function.deserializeIntermediateResult(
            new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(empty))));
    byte[] serialized = function.serializeIntermediateResult(merged).getBytes();
    assertEquals(ByteBuffer.wrap(serialized).getInt(), 2, "Expected small (capacity-preserving) encoding");
    assertEquals(((TDigest) merged).byteSize(), serialized.length);
    TDigest roundTripped = MergingDigest.fromBytes(ByteBuffer.wrap(serialized));
    assertEquals(roundTripped.size(), numCentroids);
    assertEquals(roundTripped.quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);

    DoubleArrayList values = new DoubleArrayList(new double[]{0.0, 25.0, 50.0});
    assertEquals(((TDigest) function.merge(merged, values)).size(), numCentroids + 3L);
    Object reverseOrder = function.merge(new DoubleArrayList(values), function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small))));
    assertTrue(reverseOrder instanceof PercentileTDigestAccumulator);
    assertEquals(((TDigest) reverseOrder).size(), numCentroids + 3L);
  }

  @Test
  public void testMergeWithNullIntermediateResult() {
    PercentileSmartTDigestAggregationFunction function = createFunction();
    DoubleArrayList valueList = new DoubleArrayList(new double[]{1.0, 2.0});
    assertEquals(function.merge(null, valueList), valueList);
    assertEquals(function.merge(valueList, null), valueList);
    assertEquals(function.merge(null, null), null);
  }

  private static PercentileSmartTDigestAggregationFunction createFunction() {
    return new PercentileSmartTDigestAggregationFunction(
        List.of(EXPRESSION, ExpressionContext.forLiteral(Literal.doubleValue(50.0)),
            ExpressionContext.forLiteral(Literal.stringValue("THRESHOLD=1;COMPRESSION=20"))), false);
  }

  public static class WithHighThreshold extends AbstractPercentileAggregationFunctionTest {
    @Override
    public String callStr(String column, int percent) {
      return "PERCENTILESMARTTDIGEST(" + column + ", " + percent + ", 'THRESHOLD=10000')";
    }
  }

  public static class WithSmallThreshold extends AbstractPercentileAggregationFunctionTest {
    @Override
    public String callStr(String column, int percent) {
      return "PERCENTILESMARTTDIGEST(" + column + ", " + percent + ", 'THRESHOLD=1')";
    }

    @Override
    String expectedAggrWithNull10(Scenario scenario) {
      return "0.5";
    }

    @Override
    String expectedAggrWithNull30(Scenario scenario) {
      return "2.5";
    }

    @Override
    String expectedAggrWithNull50(Scenario scenario) {
      return "4.5";
    }

    @Override
    String expectedAggrWithNull70(Scenario scenario) {
      return "6.5";
    }

    @Override
    String expectedAggrWithoutNull55(Scenario scenario) {
      switch (scenario.getDataType()) {
        case INT:
          return "-6.442450943999939E8";
        case LONG:
          return "-2.7670116110564065E18";
        case FLOAT:
        case DOUBLE:
          return "-Infinity";
        default:
          throw new IllegalArgumentException("Unsupported datatype " + scenario.getDataType());
      }
    }

    @Override
    String expectedAggrWithoutNull75(Scenario scenario) {
      return "4.0";
    }

    @Override
    String expectedAggrWithoutNull90(Scenario scenario) {
      return "7.100000000000001";
    }
  }
}
