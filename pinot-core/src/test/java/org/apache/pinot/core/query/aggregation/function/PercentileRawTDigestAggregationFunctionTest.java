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
import java.nio.ByteBuffer;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests that the final result of `percentileRawTDigest` stays readable by a plain t-digest
/// [MergingDigest#fromBytes] reader when the intermediate [PercentileTDigestAccumulator] holds
/// capacity-preserving state (a digest with more centroids than a freshly allocated
/// [MergingDigest] of the same compression can hold).
public class PercentileRawTDigestAggregationFunctionTest {
  private static final ExpressionContext EXPRESSION = ExpressionContext.forIdentifier("col");

  @Test
  public void testFinalResultReadableByPlainReaderForCapacityPreservingState() {
    int numCentroids = 51;
    double compression = 20.0;
    byte[] small = createSmallUnitCentroidDigest(numCentroids, compression, 60, 100);

    // Sanity: the input itself is readable by a plain t-digest reader.
    TDigest direct = MergingDigest.fromBytes(ByteBuffer.wrap(small));
    assertEquals(direct.size(), numCentroids);

    PercentileRawTDigestAggregationFunction function =
        new PercentileRawTDigestAggregationFunction(EXPRESSION, 50.0, (int) compression, false);
    TDigest intermediateResult = function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(small)));
    assertTrue(intermediateResult instanceof PercentileTDigestAccumulator);

    byte[] serialized = BytesUtils.toBytes(function.extractFinalResult(intermediateResult).toString());

    // Client side: a plain t-digest reader must be able to read the emitted bytes without losing
    // state. Before the fix this threw ArrayIndexOutOfBoundsException because the final result was
    // re-encoded as a verbose digest with more centroids than the reader allocates.
    TDigest roundTripped = MergingDigest.fromBytes(ByteBuffer.wrap(serialized));
    assertEquals(roundTripped.size(), numCentroids);
    assertEquals(roundTripped.quantile(0.0), 0.0);
    assertEquals(roundTripped.quantile(0.5), (numCentroids - 1.0) / 2.0, 1.0);
    assertEquals(roundTripped.quantile(1.0), numCentroids - 1.0);
  }

  /// SMALL-encoded digest (encoding 2) with explicit main/buffer capacities, unit-weight centroids
  /// at means 0..numCentroids-1. This is the layout t-digest's `asSmallBytes` produces and
  /// `MergingDigest.fromBytes` accepts regardless of the default capacity for the compression.
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
}
