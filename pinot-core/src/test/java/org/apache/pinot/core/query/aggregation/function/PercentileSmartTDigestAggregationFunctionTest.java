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

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PercentileSmartTDigestAggregationFunctionTest {
  private static final ExpressionContext EXPRESSION = ExpressionContext.forIdentifier("col");

  @Test
  public void testAccumulatorIntermediateSerializationIsLegacyCompatibleAtLowCompression() {
    PercentileSmartTDigestAggregationFunction function = newFunction();
    byte[] verbose = PercentileRawTDigestAggregationFunctionTest.createVerboseUnitCentroidDigest(51, 20.0);
    Object intermediateResult = function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(verbose)));
    assertTrue(intermediateResult instanceof PercentileTDigestAccumulator);

    AggregationFunction.SerializedIntermediateResult serialized =
        function.serializeIntermediateResult(intermediateResult);
    assertEquals(serialized.getType(), ObjectSerDeUtils.ObjectType.TDigest.getValue());
    assertLegacyCompatibleShape(serialized.getBytes(), 20.0);

    Object roundTripped = function.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
    assertTrue(roundTripped instanceof PercentileTDigestAccumulator);
    assertUnitCentroidDigest((TDigest) roundTripped, 51, 20.0);
    assertLegacyCompatibleShape(function.serializeIntermediateResult(roundTripped).getBytes(), 20.0);
  }

  /// The intermediate result must round-trip through the [PercentileTDigestAccumulator] so that
  /// capacity-preserving digests (more centroids than a fresh legacy [MergingDigest] of the same
  /// compression can hold) are not re-encoded into bytes a receiving server cannot read.
  @Test
  public void testIntermediateResultRoundTripPreservesCapacityPreservingState() {
    int numCentroids = 51;
    byte[] small =
        PercentileRawTDigestAggregationFunctionTest.createSmallUnitCentroidDigest(numCentroids, 20.0, 60, 100);
    PercentileSmartTDigestAggregationFunction function = newFunction();

    Object intermediateResult = function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small)));
    assertTrue(intermediateResult instanceof PercentileTDigestAccumulator);

    AggregationFunction.SerializedIntermediateResult serialized =
        function.serializeIntermediateResult(intermediateResult);
    assertEquals(serialized.getType(), ObjectSerDeUtils.ObjectType.TDigest.getValue());
    // A verbose re-encoding with 51 centroids would be rejected by a plain legacy MergingDigest.fromBytes reader
    // with ArrayIndexOutOfBoundsException.
    TDigest roundTripped = MergingDigest.fromBytes(ByteBuffer.wrap(serialized.getBytes()));
    assertEquals(roundTripped.size(), numCentroids);
    assertEquals(roundTripped.quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);

    Object merged = function.merge(
        function.deserializeIntermediateResult(
            new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes()))),
        function.deserializeIntermediateResult(
            new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes()))));
    assertTrue(merged instanceof PercentileTDigestAccumulator);
    assertEquals(((TDigest) merged).size(), 2L * numCentroids);

    // The materialized (merged) accumulator must also serialize into plain-reader-compatible bytes.
    TDigest mergedRoundTripped = MergingDigest.fromBytes(
        ByteBuffer.wrap(function.serializeIntermediateResult(merged).getBytes()));
    assertEquals(mergedRoundTripped.size(), 2L * numCentroids);
    assertEquals(mergedRoundTripped.quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);
  }

  /// Materialized (non-pass-through) capacity-preserving state must also serialize into legacy-compatible bytes
  /// through the generic serde, with `byteSize()` agreeing with the serialized length.
  @Test
  public void testMaterializedCapacityPreservingStateSerializesLegacyCompatible() {
    int numCentroids = 51;
    byte[] small =
        PercentileRawTDigestAggregationFunctionTest.createSmallUnitCentroidDigest(numCentroids, 20.0, 60, 100);
    byte[] empty = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(TDigest.createMergingDigest(20.0));
    PercentileSmartTDigestAggregationFunction function = newFunction();

    Object merged = function.merge(
        function.deserializeIntermediateResult(
            new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small))),
        function.deserializeIntermediateResult(
            new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(empty))));
    byte[] serialized = function.serializeIntermediateResult(merged).getBytes();
    assertLegacyCompatibleShape(serialized, 20.0);
    assertEquals(((TDigest) merged).byteSize(), serialized.length);
    TDigest roundTripped = MergingDigest.fromBytes(ByteBuffer.wrap(serialized));
    assertEquals(roundTripped.size(), numCentroids);
    assertEquals(roundTripped.quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);
  }

  @Test
  public void testMergeWithNullIntermediateResult() {
    PercentileSmartTDigestAggregationFunction function = newFunction();
    DoubleArrayList valueList = new DoubleArrayList(new double[]{1.0, 2.0});
    assertEquals(function.merge(null, valueList), valueList);
    assertEquals(function.merge(valueList, null), valueList);
    assertEquals(function.merge(null, null), null);
  }

  /// Broker reduce can merge a server that produced a t-digest with a server that stayed below the
  /// threshold and produced a raw value list; both argument orders must route into the accumulator.
  @Test
  public void testMergeAccumulatorWithValueList() {
    int numCentroids = 51;
    byte[] small =
        PercentileRawTDigestAggregationFunctionTest.createSmallUnitCentroidDigest(numCentroids, 20.0, 60, 100);
    PercentileSmartTDigestAggregationFunction function = newFunction();

    for (boolean accumulatorFirst : new boolean[]{true, false}) {
      Object accumulator = function.deserializeIntermediateResult(
          new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small)));
      DoubleArrayList valueList = new DoubleArrayList(new double[]{0.0, 25.0, 50.0});
      Object merged = accumulatorFirst ? function.merge(accumulator, valueList)
          : function.merge(valueList, accumulator);
      assertTrue(merged instanceof PercentileTDigestAccumulator);
      assertEquals(((TDigest) merged).size(), numCentroids + 3L);
      assertEquals(((TDigest) merged).quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);
      TDigest roundTripped = MergingDigest.fromBytes(
          ByteBuffer.wrap(function.serializeIntermediateResult(merged).getBytes()));
      assertEquals(roundTripped.size(), numCentroids + 3L);
    }
  }

  @Test
  public void testThresholdConversionPreservesDuplicateInfiniteValues() {
    int repetitions = 1_000;
    double[] values = new double[3 * repetitions];
    Arrays.fill(values, 0, repetitions, Double.NEGATIVE_INFINITY);
    Arrays.fill(values, repetitions, 2 * repetitions, 0.0);
    Arrays.fill(values, 2 * repetitions, values.length, Double.POSITIVE_INFINITY);
    PercentileSmartTDigestAggregationFunction function = newFunction();

    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(values.length, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, values)));
    Object intermediateResult = function.extractAggregationResult(resultHolder);
    assertTrue(intermediateResult instanceof PercentileTDigestAccumulator);
    TDigest result = (TDigest) intermediateResult;
    assertDuplicateInfinityResult(result, values.length);
    TDigest roundTripped = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(
        function.serializeIntermediateResult(intermediateResult).getBytes());
    assertDuplicateInfinityResult(roundTripped, values.length);

    AggregationFunction.SerializedIntermediateResult serialized =
        function.serializeIntermediateResult(intermediateResult);
    Object first = function.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
    Object second = function.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
    Object merged = function.merge(first, second);
    assertTrue(merged instanceof PercentileTDigestAccumulator);
    assertDuplicateInfinityResult((TDigest) merged, 2L * values.length);
  }

  private static PercentileSmartTDigestAggregationFunction newFunction() {
    return new PercentileSmartTDigestAggregationFunction(
        List.of(EXPRESSION, ExpressionContext.forLiteral(Literal.doubleValue(50.0)),
            ExpressionContext.forLiteral(Literal.stringValue("THRESHOLD=1;COMPRESSION=20"))), false);
  }

  private static void assertDuplicateInfinityResult(TDigest result, long expectedSize) {
    assertEquals(result.size(), expectedSize);
    assertEquals(result.getMin(), Double.NEGATIVE_INFINITY);
    assertEquals(result.getMax(), Double.POSITIVE_INFINITY);
    for (Centroid centroid : result.centroids()) {
      assertFalse(Double.isNaN(centroid.mean()));
    }
    assertEquals(result.quantile(0.0), Double.NEGATIVE_INFINITY);
    assertEquals(result.quantile(0.5), 0.0);
    assertEquals(result.quantile(1.0), Double.POSITIVE_INFINITY);
  }

  private static void assertLegacyCompatibleShape(byte[] bytes, double compression) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int encoding = buffer.getInt();
    if (encoding == 1) {
      assertEquals(buffer.getDouble(Integer.BYTES + 2 * Double.BYTES), compression);
      int centroidCount = buffer.getInt(Integer.BYTES + 3 * Double.BYTES);
      assertTrue(centroidCount <= 2 * Math.ceil(compression) + 10,
          "Verbose encoding exceeds the t-digest 3.2 centroid capacity: " + centroidCount);
    } else {
      assertEquals(encoding, 2, "Unexpected t-digest encoding");
    }
  }

  private static void assertUnitCentroidDigest(TDigest digest, int expectedSize, double compression) {
    assertEquals(digest.size(), expectedSize);
    assertEquals(digest.compression(), compression);
    assertEquals(digest.quantile(0.0), 0.0);
    assertEquals(digest.quantile(0.5), (expectedSize - 1.0) / 2.0, 1.0);
    assertEquals(digest.quantile(1.0), expectedSize - 1.0);
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
      return "1.0";
    }

    @Override
    String expectedAggrWithNull30(Scenario scenario) {
      return "3.0";
    }

    @Override
    String expectedAggrWithNull50(Scenario scenario) {
      return "5.0";
    }

    @Override
    String expectedAggrWithNull70(Scenario scenario) {
      return "7.0";
    }

    @Override
    String expectedAggrWithoutNull55(Scenario scenario) {
      return "0.0";
    }

    @Override
    String expectedAggrWithoutNull75(Scenario scenario) {
      return "4.0";
    }

    @Override
    String expectedAggrWithoutNull90(Scenario scenario) {
      return "7.0";
    }
  }
}
