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
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.customobject.PercentileTDigestAccumulator;
import org.apache.pinot.segment.local.customobject.SerializedTDigest;
import org.apache.pinot.segment.local.customobject.TDigest;
import org.apache.pinot.segment.local.utils.TDigestUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests that `percentileRawTDigest` retains the legacy byte format and reader capacity contract.
/// Capacity-preserving inputs can contain more centroids than a default legacy reader allocates.
public class PercentileRawTDigestAggregationFunctionTest {
  private static final ExpressionContext EXPRESSION = ExpressionContext.forIdentifier("col");

  @Test
  public void testFinalResultPreservesLegacyCapacityState() {
    int numCentroids = 51;
    double compression = 20.0;
    byte[] small = createSmallUnitCentroidDigest(numCentroids, compression, 60, 100);

    // Sanity: the supplied legacy input has the expected weight.
    TDigest direct = TDigestUtils.deserialize(ByteBuffer.wrap(small));
    assertEquals(direct.size(), numCentroids);

    PercentileRawTDigestAggregationFunction function =
        new PercentileRawTDigestAggregationFunction(EXPRESSION, 50.0, (int) compression, false);
    TDigest intermediateResult = function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small)));
    assertTrue(intermediateResult instanceof PercentileTDigestAccumulator);

    SerializedTDigest finalResult = function.extractFinalResult(intermediateResult);
    byte[] serialized = BytesUtils.toBytes(finalResult.toString());
    assertLegacyCompatibleShape(serialized, compression);
    assertEquals(serialized, small, "Untouched legacy small state must retain its original bytes");

    // Before the capacity fix, verbose output exceeded the allocation of a legacy reader.
    TDigest roundTripped = TDigestUtils.deserialize(ByteBuffer.wrap(serialized));
    assertUnitCentroidDigest(roundTripped, numCentroids);

    // Also cover the materialized (merged) accumulator state, not just the serialized pass-through.
    TDigest merged = function.merge(intermediateResult, function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small))));
    byte[] mergedSerialized = BytesUtils.toBytes(function.extractFinalResult(merged).toString());
    assertLegacyCompatibleShape(mergedSerialized, compression);
    TDigest mergedRoundTripped = TDigestUtils.deserialize(ByteBuffer.wrap(mergedSerialized));
    assertEquals(mergedRoundTripped.size(), 2L * numCentroids);
    assertEquals(mergedRoundTripped.quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);
  }

  @Test
  public void testFinalResultRoundTripForRegularDigest() {
    double compression = 100.0;
    TDigest input = TDigest.createMergingDigest(compression);
    for (int i = 0; i < 1000; i++) {
      input.add(i);
    }
    byte[] bytes = new byte[input.byteSize()];
    input.asBytes(ByteBuffer.wrap(bytes));

    PercentileRawTDigestAggregationFunction function =
        new PercentileRawTDigestAggregationFunction(EXPRESSION, 50.0, (int) compression, false);
    TDigest intermediateResult = function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(bytes)));

    SerializedTDigest finalResult = function.extractFinalResult(intermediateResult);
    byte[] serialized = BytesUtils.toBytes(finalResult.toString());
    TDigest roundTripped = TDigestUtils.deserialize(ByteBuffer.wrap(serialized));
    assertEquals(roundTripped.size(), 1000);
    assertEquals(roundTripped.quantile(0.5), input.quantile(0.5), 1e-6);
  }

  /// A verbose digest with more centroids than a t-digest 3.2 reader of the same compression allocates is accepted
  /// since the 3.3 upgrade, but the emitted final result must stay readable by legacy readers.
  @Test
  public void testAccumulatorFinalSerializationIsLegacyCompatibleAtLowCompression() {
    PercentileRawTDigestAggregationFunction function =
        new PercentileRawTDigestAggregationFunction(EXPRESSION, 50.0, 20, false);
    byte[] verbose = createVerboseUnitCentroidDigest(51, 20.0);
    TDigest intermediateResult = function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(verbose)));
    assertTrue(intermediateResult instanceof PercentileTDigestAccumulator);

    SerializedTDigest finalResult = function.extractFinalResult(intermediateResult);
    byte[] serialized = BytesUtils.toBytes(finalResult.toString());
    assertLegacyCompatibleShape(serialized, 20.0);

    TDigest roundTripped = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(serialized);
    assertEquals(roundTripped.compression(), 20.0);
    assertUnitCentroidDigest(roundTripped, 51);
  }

  /// Verbose-encoded digest (encoding 1) with unit-weight centroids at means 0..numCentroids-1. Package-private:
  /// shared with [PercentileSmartTDigestAggregationFunctionTest].
  static byte[] createVerboseUnitCentroidDigest(int numCentroids, double compression) {
    ByteBuffer buffer = ByteBuffer.allocate(4 * Integer.BYTES + 2 * Double.BYTES + 2 * Double.BYTES * numCentroids);
    buffer.putInt(1);
    buffer.putDouble(0.0);
    buffer.putDouble(numCentroids - 1.0);
    buffer.putDouble(compression);
    buffer.putInt(numCentroids);
    for (int i = 0; i < numCentroids; i++) {
      buffer.putDouble(1.0);
      buffer.putDouble(i);
    }
    return buffer.array();
  }

  /// SMALL-encoded digest (encoding 2) with explicit main/buffer capacities, unit-weight centroids
  /// at means 0..numCentroids-1. This is the layout t-digest's `asSmallBytes` produces and
  /// legacy readers accept using the encoded capacity rather than the default for the compression.
  /// Package-private: shared with [PercentileSmartTDigestAggregationFunctionTest].
  static byte[] createSmallUnitCentroidDigest(int numCentroids, double compression, int mainCapacity,
      int bufferCapacity) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + 2 * Double.BYTES + Float.BYTES + 3 * Short.BYTES
        + 2 * Float.BYTES * numCentroids);
    buffer.putInt(2);
    buffer.putDouble(0.0);
    buffer.putDouble(numCentroids - 1.0);
    buffer.putFloat((float) compression);
    buffer.putShort((short) mainCapacity);
    buffer.putShort((short) bufferCapacity);
    buffer.putShort((short) numCentroids);
    for (int i = 0; i < numCentroids; i++) {
      buffer.putFloat(1.0f);
      buffer.putFloat(i);
    }
    return buffer.array();
  }

  static void assertLegacyCompatibleShape(byte[] bytes, double compression) {
    // Decode the documented legacy header independently of the production codec. The centroid count must fit
    // the allocation that a tdunning reader makes before it consumes any centroid payload.
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int encoding = buffer.getInt();
    buffer.position(Integer.BYTES + 2 * Double.BYTES);
    int centroidCount;
    int centroidSize;
    if (encoding == 1) {
      assertEquals(buffer.getDouble(), compression);
      centroidCount = buffer.getInt();
      centroidSize = 2 * Double.BYTES;
      assertTrue(centroidCount <= 2 * Math.ceil(compression) + 10,
          "Verbose encoding exceeds the legacy reader centroid capacity: " + centroidCount);
    } else {
      assertEquals(encoding, 2, "Unexpected t-digest encoding");
      assertEquals(buffer.getFloat(), (float) compression);
      int mainCapacity = buffer.getShort();
      int bufferCapacity = buffer.getShort();
      centroidCount = buffer.getShort();
      centroidSize = 2 * Float.BYTES;
      assertTrue(mainCapacity >= centroidCount);
      assertTrue(bufferCapacity >= mainCapacity);
    }
    assertTrue(centroidCount >= 0);
    assertEquals(buffer.remaining(), centroidCount * centroidSize);
  }

  private static void assertUnitCentroidDigest(TDigest digest, int expectedSize) {
    assertEquals(digest.size(), expectedSize);
    assertEquals(digest.quantile(0.0), 0.0);
    assertEquals(digest.quantile(0.5), (expectedSize - 1.0) / 2.0, 1.0);
    assertEquals(digest.quantile(1.0), expectedSize - 1.0);
  }
}
