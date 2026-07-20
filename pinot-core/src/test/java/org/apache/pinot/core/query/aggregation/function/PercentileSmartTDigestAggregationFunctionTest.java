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
import org.testng.Assert;
import org.testng.annotations.Test;


public class PercentileSmartTDigestAggregationFunctionTest {
  private static final ExpressionContext EXPRESSION = ExpressionContext.forIdentifier("col");

  /// The intermediate result must round-trip through the [PercentileTDigestAccumulator] so that
  /// capacity-preserving digests (more centroids than a fresh [MergingDigest] of the same
  /// compression can hold) are not re-encoded into bytes a receiving server cannot read.
  @Test
  public void testIntermediateResultRoundTripPreservesCapacityPreservingState() {
    int numCentroids = 51;
    double compression = 20.0;
    byte[] small =
        PercentileRawTDigestAggregationFunctionTest.createSmallUnitCentroidDigest(numCentroids, compression, 60, 100);
    PercentileSmartTDigestAggregationFunction function = new PercentileSmartTDigestAggregationFunction(
        List.of(EXPRESSION, ExpressionContext.forLiteral(Literal.doubleValue(50.0)),
            ExpressionContext.forLiteral(Literal.stringValue("THRESHOLD=1;COMPRESSION=20"))), false);

    Object intermediateResult = function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small)));
    Assert.assertTrue(intermediateResult instanceof PercentileTDigestAccumulator);

    AggregationFunction.SerializedIntermediateResult serialized =
        function.serializeIntermediateResult(intermediateResult);
    Assert.assertEquals(serialized.getType(), ObjectSerDeUtils.ObjectType.TDigest.getValue());
    // Before the fix this was a verbose encoding with 51 centroids, which a plain
    // MergingDigest.fromBytes reader rejects with ArrayIndexOutOfBoundsException.
    TDigest roundTripped = MergingDigest.fromBytes(ByteBuffer.wrap(serialized.getBytes()));
    Assert.assertEquals(roundTripped.size(), numCentroids);
    Assert.assertEquals(roundTripped.quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);

    Object merged = function.merge(
        function.deserializeIntermediateResult(
            new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes()))),
        function.deserializeIntermediateResult(
            new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes()))));
    Assert.assertTrue(merged instanceof PercentileTDigestAccumulator);
    Assert.assertEquals(((TDigest) merged).size(), 2L * numCentroids);

    // The materialized (merged) accumulator must also serialize into plain-reader-compatible bytes.
    TDigest mergedRoundTripped = MergingDigest.fromBytes(
        ByteBuffer.wrap(function.serializeIntermediateResult(merged).getBytes()));
    Assert.assertEquals(mergedRoundTripped.size(), 2L * numCentroids);
    Assert.assertEquals(mergedRoundTripped.quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);
  }

  /// Materialized (non-pass-through) capacity-preserving state must also serialize into the small encoding through
  /// the generic serde: merging with an empty digest materializes the oversized centroids without recompression.
  @Test
  public void testMaterializedCapacityPreservingStateUsesSmallEncoding() {
    int numCentroids = 51;
    byte[] small =
        PercentileRawTDigestAggregationFunctionTest.createSmallUnitCentroidDigest(numCentroids, 20.0, 60, 100);
    byte[] empty = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(TDigest.createMergingDigest(20.0));
    PercentileSmartTDigestAggregationFunction function = new PercentileSmartTDigestAggregationFunction(
        List.of(EXPRESSION, ExpressionContext.forLiteral(Literal.doubleValue(50.0)),
            ExpressionContext.forLiteral(Literal.stringValue("THRESHOLD=1;COMPRESSION=20"))), false);

    Object merged = function.merge(
        function.deserializeIntermediateResult(
            new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small))),
        function.deserializeIntermediateResult(
            new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(empty))));
    byte[] serialized = function.serializeIntermediateResult(merged).getBytes();
    Assert.assertEquals(ByteBuffer.wrap(serialized).getInt(), 2, "Expected small (capacity-preserving) encoding");
    Assert.assertEquals(((TDigest) merged).byteSize(), serialized.length);
    TDigest roundTripped = MergingDigest.fromBytes(ByteBuffer.wrap(serialized));
    Assert.assertEquals(roundTripped.size(), numCentroids);
    Assert.assertEquals(roundTripped.quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);
  }

  /// Broker reduce can merge a server that produced a t-digest with a server that stayed below the
  /// threshold and produced a raw value list; both argument orders must route into the accumulator.
  @Test
  public void testMergeAccumulatorWithValueList() {
    int numCentroids = 51;
    byte[] small = PercentileRawTDigestAggregationFunctionTest.createSmallUnitCentroidDigest(numCentroids, 20.0, 60,
        100);
    PercentileSmartTDigestAggregationFunction function = new PercentileSmartTDigestAggregationFunction(
        List.of(EXPRESSION, ExpressionContext.forLiteral(Literal.doubleValue(50.0)),
            ExpressionContext.forLiteral(Literal.stringValue("THRESHOLD=1;COMPRESSION=20"))), false);

    for (boolean accumulatorFirst : new boolean[]{true, false}) {
      Object accumulator = function.deserializeIntermediateResult(
          new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small)));
      DoubleArrayList valueList = new DoubleArrayList(new double[]{0.0, 25.0, 50.0});
      Object merged = accumulatorFirst ? function.merge(accumulator, valueList)
          : function.merge(valueList, accumulator);
      Assert.assertTrue(merged instanceof PercentileTDigestAccumulator);
      Assert.assertEquals(((TDigest) merged).size(), numCentroids + 3L);
      Assert.assertEquals(((TDigest) merged).quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);
      TDigest roundTripped = MergingDigest.fromBytes(
          ByteBuffer.wrap(function.serializeIntermediateResult(merged).getBytes()));
      Assert.assertEquals(roundTripped.size(), numCentroids + 3L);
    }
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
