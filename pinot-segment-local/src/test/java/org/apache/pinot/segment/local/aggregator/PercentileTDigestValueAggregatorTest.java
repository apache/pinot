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
package org.apache.pinot.segment.local.aggregator;

import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/// Tests the serialization-size contract and deferred compression used by [PercentileTDigestValueAggregator].
public class PercentileTDigestValueAggregatorTest {

  @Test(dataProvider = "compressionBounds")
  public void testRawValuesTrackMaxVerboseSizeWithoutCompression(int compression, int numValues,
      int expectedMaxByteSize) {
    PercentileTDigestValueAggregator aggregator = newAggregator(compression);
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), 0);

    TDigest digest = aggregator.getInitialAggregatedValue(0.0);
    for (int i = 1; i < numValues; i++) {
      aggregator.applyRawValue(digest, i);
    }
    assertEquals(digest.centroidCount(), 0);
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), expectedMaxByteSize);
    byte[] serialized = aggregator.serializeAggregatedValue(digest);
    assertTrue(serialized.length <= expectedMaxByteSize);
  }

  @DataProvider
  public static Object[][] compressionBounds() {
    return new Object[][]{
        {10, 30, 512},
        {100, 210, 3_392},
        {1_000, 2_010, 32_192}
    };
  }

  @Test
  public void testRawUpdatesDoNotForceCompression() {
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest digest = aggregator.getInitialAggregatedValue(0.0);
    for (int i = 1; i < 256; i++) {
      aggregator.applyRawValue(digest, i);
    }

    assertEquals(digest.size(), 256L);
    assertEquals(digest.centroidCount(), 0);
    int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
    byte[] serialized = aggregator.serializeAggregatedValue(digest);
    assertTrue(digest.centroidCount() > 0);
    assertTrue(serialized.length <= maxByteSize);
  }

  @Test
  public void testPreAggregatedCompressionExpandsRegisteredBound() {
    TDigest input = TDigest.createMergingDigest(200);
    input.add(42.0);
    byte[] inputBytes = CustomSerDeUtils.TDIGEST_SER_DE.serialize(input);

    PercentileTDigestValueAggregator aggregator = newAggregator(10);
    TDigest result = aggregator.getInitialAggregatedValue(inputBytes);
    for (int i = 0; i < 409; i++) {
      aggregator.applyRawValue(result, i);
    }

    assertEquals(result.compression(), 200.0);
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), 6_592);
    int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
    assertTrue(aggregator.serializeAggregatedValue(result).length <= maxByteSize);
  }

  @Test
  public void testOversizedSmallEncodedDigestRemainsReadable() {
    int centroidCount = 300;
    byte[] smallEncoding = createSmallEncoding(10, 400, 500, centroidCount);

    PercentileTDigestValueAggregator aggregator = newAggregator(10);
    TDigest result = aggregator.getInitialAggregatedValue(smallEncoding);
    int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
    byte[] serialized = aggregator.serializeAggregatedValue(result);
    TDigest clone = aggregator.cloneAggregatedValue(result);

    assertEquals(result.centroidCount(), centroidCount);
    assertEquals(maxByteSize, 4_832);
    assertEquals(ByteBuffer.wrap(serialized).getInt(), 2);
    assertEquals(serialized.length, smallEncoding.length);
    assertTrue(serialized.length <= maxByteSize);
    assertEquals(clone.size(), result.size());
    assertEquals(clone.centroidCount(), result.centroidCount());
    assertEquals(clone.quantile(0.75), result.quantile(0.75));
  }

  @Test
  public void testAggregatedUpdateDoesNotForceDestinationCompression() {
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest destination = aggregator.getInitialAggregatedValue(1.0);
    TDigest source = TDigest.createMergingDigest(100);
    source.add(2.0);
    source.add(3.0);

    aggregator.applyAggregatedValue(destination, source);

    assertEquals(destination.size(), 3L);
    assertEquals(destination.centroidCount(), 0);
    int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
    byte[] serialized = aggregator.serializeAggregatedValue(destination);
    assertTrue(serialized.length <= maxByteSize);
  }

  private static PercentileTDigestValueAggregator newAggregator(int compression) {
    return new PercentileTDigestValueAggregator(
        List.of(ExpressionContext.forLiteral(Literal.intValue(compression))));
  }

  private static byte[] createSmallEncoding(int compression, int centroidCapacity, int bufferSize,
      int centroidCount) {
    ByteBuffer buffer = ByteBuffer.allocate(30 + centroidCount * 2 * Float.BYTES);
    buffer.putInt(2);
    buffer.putDouble(0.0);
    buffer.putDouble(centroidCount - 1.0);
    buffer.putFloat(compression);
    buffer.putShort((short) centroidCapacity);
    buffer.putShort((short) bufferSize);
    buffer.putShort((short) centroidCount);
    for (int i = 0; i < centroidCount; i++) {
      buffer.putFloat(1.0F);
      buffer.putFloat(i);
    }
    return buffer.array();
  }
}
