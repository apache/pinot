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
package org.apache.pinot.segment.local.utils;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.pinot.segment.local.customobject.PercentileTDigestAccumulator;
import org.apache.pinot.segment.local.customobject.TDigest;
import org.apache.pinot.segment.local.customobject.TDigest.Centroid;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TDigestUtilsTest {
  private static final int VERBOSE_ENCODING = 1;
  private static final int SMALL_ENCODING = 2;
  private static final int VERBOSE_HEADER_SIZE = 32;

  @Test
  public void testPinotDigestRetainsQuantileAccuracyAcrossLegacyBytes() {
    TDigest digest = TDigestUtils.createMergingDigestWithLegacyBuffer(100.0);
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }

    TDigest roundTripped = TDigestUtils.deserializeFiniteWithLegacyBuffer(TDigestUtils.serialize(digest));
    assertEquals(roundTripped.size(), digest.size());
    assertEquals(roundTripped.getMin(), 0.0);
    assertEquals(roundTripped.getMax(), 999.0);
    assertEquals(roundTripped.quantile(0.75), 749.5, 2.0);
  }

  @Test
  public void testBufferedRawQuantilesRetainLegacyTwoLevelCompression() {
    SplittableRandom random = new SplittableRandom(10);
    double[] values = new double[94];
    TDigest raw = TDigestUtils.createMergingDigestWithLegacyBuffer(100.0);
    TDigest merged = TDigestUtils.createMergingDigestWithLegacyBuffer(100.0);
    for (int start = 0; start < values.length; start += 10) {
      TDigest partial = TDigestUtils.createMergingDigestWithLegacyBuffer(100.0);
      for (int i = start; i < Math.min(start + 10, values.length); i++) {
        values[i] = random.nextDouble() * 10_000;
        raw.add(values[i]);
        partial.add(values[i]);
      }
      merged.add(partial);
    }
    Arrays.sort(values);
    for (double quantile : new double[]{0.1, 0.4, 0.5, 0.75, 0.86, 0.95}) {
      // At this size legacy working compression retains unit centroids, whose quantiles are order statistics.
      double expected = values[(int) (quantile * values.length)];
      assertEquals(raw.quantile(quantile), expected, 1e-9);
      assertEquals(merged.quantile(quantile), expected, 1e-9);
    }
  }

  @Test
  public void testSerializeReusesCallerScratchWithoutSharingOutput() {
    TDigest digest = TDigestUtils.createMergingDigestWithLegacyBuffer(100.0);
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }
    ByteBuffer scratch = ByteBuffer.allocate(4_096);

    byte[] bytes = TDigestUtils.serialize(digest, scratch);
    assertTrue(scratch.position() >= bytes.length);
    scratch.put(0, (byte) 0);
    assertEquals(ByteBuffer.wrap(bytes).getInt(), VERBOSE_ENCODING);
    assertEquals(TDigestUtils.deserialize(bytes).size(), digest.size());
  }

  @Test
  public void testSerializeUsesNetworkOrderWithoutChangingScratchOrder() {
    TDigest digest = TDigestUtils.createMergingDigestWithLegacyBuffer(100.0);
    digest.add(42.0);
    ByteBuffer scratch = ByteBuffer.allocate(4_096).order(ByteOrder.LITTLE_ENDIAN);

    byte[] bytes = TDigestUtils.serialize(digest, scratch);

    assertEquals(scratch.order(), ByteOrder.LITTLE_ENDIAN);
    assertEquals(ByteBuffer.wrap(bytes).getInt(), VERBOSE_ENCODING);
    assertEquals(TDigestUtils.deserialize(bytes).quantile(0.5), 42.0);
  }

  @Test
  public void testLowCompressionLargeMeansRemainDoublePrecision() {
    int centroidCount = 51;
    double firstValue = 1.0e18;
    double[] means = new double[centroidCount];
    double[] weights = new double[centroidCount];
    for (int i = 0; i < centroidCount; i++) {
      means[i] = firstValue + 256.0 * i;
      weights[i] = 1.0;
    }

    byte[] bytes = TDigestUtils.serialize(craftedVerboseDigest(20.0, means, weights));
    ByteBuffer serialized = ByteBuffer.wrap(bytes);
    assertEquals(serialized.getInt(), VERBOSE_ENCODING);
    assertEquals(serialized.getDouble(), means[0]);
    assertEquals(serialized.getDouble(), means[centroidCount - 1]);
    assertEquals(serialized.getDouble(), 20.0);
    assertEquals(serialized.getInt(), 50, "Verbose output must fit the t-digest 3.2 centroid capacity");

    TDigest roundTripped = TDigestUtils.deserialize(bytes);
    assertEquals(roundTripped.size(), centroidCount);
    assertEquals(roundTripped.getMin(), means[0]);
    assertEquals(roundTripped.getMax(), means[centroidCount - 1]);
    assertTrue(roundTripped.quantile(0.5) >= means[0]);
    assertTrue(roundTripped.quantile(0.5) <= means[centroidCount - 1]);
  }

  @Test
  public void testLowCompressionFloatExactMeansUseCapacityPreservingEncoding() {
    int centroidCount = 51;
    double[] means = new double[centroidCount];
    double[] weights = new double[centroidCount];
    for (int i = 0; i < centroidCount; i++) {
      means[i] = i;
      weights[i] = 1.0;
    }

    byte[] bytes = TDigestUtils.serialize(craftedVerboseDigest(20.0, means, weights));
    ByteBuffer serialized = ByteBuffer.wrap(bytes);
    assertEquals(serialized.getInt(), SMALL_ENCODING);
    serialized.position(28);
    assertEquals(serialized.getShort(), centroidCount);

    TDigest roundTripped = TDigestUtils.deserialize(bytes);
    assertEquals(roundTripped.size(), centroidCount);
    assertEquals(roundTripped.getMin(), 0.0);
    assertEquals(roundTripped.getMax(), centroidCount - 1.0);
  }

  @Test
  public void testNonFloatCompressionUsesLegacyVerboseCapacity() {
    double compression = 20.0000001;
    int centroidCount = 53;
    double[] means = new double[centroidCount];
    double[] weights = new double[centroidCount];
    for (int i = 0; i < centroidCount; i++) {
      means[i] = i;
      weights[i] = 1.0;
    }

    ByteBuffer serialized = ByteBuffer.wrap(TDigestUtils.serialize(craftedVerboseDigest(compression, means, weights)));
    assertEquals(serialized.getInt(), VERBOSE_ENCODING);
    serialized.position(Integer.BYTES + 2 * Double.BYTES);
    assertEquals(serialized.getDouble(), compression);
    assertEquals(serialized.getInt(), 52, "Verbose output must fit the t-digest 3.2 centroid capacity");
  }

  @Test
  public void testSerializeUsesActualCentroidCountForFractionalCompression() {
    double compression = 100.1;
    int centroidCount = 212;
    double[] means = new double[centroidCount];
    double[] weights = new double[centroidCount];
    for (int i = 0; i < centroidCount; i++) {
      means[i] = i;
      weights[i] = 1.0;
    }

    ByteBuffer serialized = ByteBuffer.wrap(TDigestUtils.serialize(craftedVerboseDigest(compression, means, weights)));
    assertEquals(serialized.getInt(), VERBOSE_ENCODING);
    serialized.position(Integer.BYTES + 2 * Double.BYTES);
    assertEquals(serialized.getDouble(), compression);
    assertEquals(serialized.getInt(), centroidCount,
        "Serialization buffer must accommodate the t-digest 3.2 fractional-compression capacity");
  }

  @Test
  public void testSerializeForceCompressesOnce() {
    PercentileTDigestAccumulator digest = spy(new PercentileTDigestAccumulator(100.0));
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }

    TDigestUtils.serialize(digest);
    verify(digest, times(1)).compress();
  }

  @Test
  public void testCompactOutputRetainsMergeCapacityAndUnitWeightBoundaries() {
    for (boolean weighted : new boolean[]{false, true}) {
      PercentileTDigestAccumulator digest = new PercentileTDigestAccumulator(10.0);
      if (weighted) {
        digest.add(10.0, 7);
        digest.add(20.0, 3);
        digest.add(30.0, 11);
      }
      ByteBuffer compact = ByteBuffer.allocate(digest.smallByteSize());
      digest.asSmallBytes(compact);
      assertEquals(compact.position(), compact.capacity());
      assertEquals(compact.getInt(0), SMALL_ENCODING);
      int mainCapacity = compact.getShort(24);
      assertTrue(mainCapacity >= 50, "Legacy readers need room for subsequent merges, including empty digests");
      assertTrue(compact.getShort(26) > mainCapacity);
      int centroidCount = compact.getShort(28);
      if (weighted) {
        assertEquals(compact.getFloat(30), 1.0F);
        assertEquals(compact.getFloat(30 + 8 * (centroidCount - 1)), 1.0F);
      } else {
        assertEquals(centroidCount, 0);
      }
    }
  }

  @Test
  public void testFractionalCompressionLegacyCapacityIsRepairedBeforeDecode() {
    double compression = 100.1;
    int centroidCount = 212;
    double firstValue = 1.0e18;
    double[] means = new double[centroidCount];
    double[] weights = new double[centroidCount];
    double expectedWeight = 0.0;
    double expectedFirstMoment = 0.0;
    for (int i = 0; i < centroidCount; i++) {
      means[i] = firstValue + 256.0 * i;
      weights[i] = i == 0 || i == centroidCount - 1 ? 2.0 : 1.0;
      expectedWeight += weights[i];
      expectedFirstMoment += means[i] * weights[i];
    }

    byte[] bytes = verboseBytes(compression, means, weights);
    int prefixSize = 3;
    int trailingValue = 0x12345678;
    ByteBuffer input = ByteBuffer.allocate(prefixSize + bytes.length + Integer.BYTES);
    input.put(new byte[prefixSize]);
    input.put(bytes);
    input.putInt(trailingValue);
    input.flip();
    input.position(prefixSize);

    TDigest digest = TDigestUtils.deserialize(input);
    assertEquals(input.position(), prefixSize + bytes.length);
    assertEquals(input.getInt(), trailingValue);
    assertEquals(digest.getMin(), means[0]);
    assertEquals(digest.getMax(), means[centroidCount - 1]);

    List<Centroid> centroids = List.copyOf(digest.centroids());
    assertTrue(centroids.size() <= centroidCount, "The payload must fit the legacy verbose capacity");
    assertEquals(centroids.get(0).mean(), means[0]);
    assertEquals(centroids.get(0).count(), 1L);
    assertEquals(centroids.get(centroids.size() - 1).mean(), means[centroidCount - 1]);
    assertEquals(centroids.get(centroids.size() - 1).count(), 1L);
    double actualWeight = 0.0;
    double actualFirstMoment = 0.0;
    for (Centroid centroid : centroids) {
      actualWeight += centroid.count();
      actualFirstMoment += centroid.mean() * centroid.count();
    }
    assertEquals(actualWeight, expectedWeight);
    assertEquals(actualFirstMoment, expectedFirstMoment, Math.ulp(expectedFirstMoment) * 16.0);

    ByteBuffer roundTripped = ByteBuffer.wrap(TDigestUtils.serialize(digest));
    assertEquals(roundTripped.getInt(), VERBOSE_ENCODING,
        "Non-float-exact compression must remain in the double-precision wire format");
    roundTripped.position(Integer.BYTES + 2 * Double.BYTES);
    assertEquals(roundTripped.getDouble(), compression);
    assertTrue(roundTripped.getInt() <= centroidCount,
        "The result must remain within the t-digest 3.2 verbose capacity");
  }

  @Test
  public void testSmallBoundaryRepairDoesNotNarrowExtrema() {
    double min = 0.1;
    double max = 0.9;
    ByteBuffer small = ByteBuffer.allocate(30 + 2 * Float.BYTES);
    small.putInt(SMALL_ENCODING);
    small.putDouble(min);
    small.putDouble(max);
    small.putFloat(20.0F);
    small.putShort((short) 50);
    small.putShort((short) 250);
    small.putShort((short) 1);
    small.putFloat(2.0F);
    small.putFloat(0.5F);

    TDigest digest = TDigestUtils.deserialize(small.array());
    ByteBuffer reEmitted = ByteBuffer.wrap(TDigestUtils.serialize(digest));
    assertEquals(reEmitted.getInt(), VERBOSE_ENCODING);
    assertEquals(reEmitted.getInt(28), 2);
    assertEquals(reEmitted.getDouble(32), 1.0);
    assertEquals(reEmitted.getDouble(48), 1.0);
    assertEquals(digest.getMin(), min);
    assertEquals(digest.getMax(), max);
    List<Centroid> centroids = List.copyOf(digest.centroids());
    assertEquals(centroids.get(0).mean(), min);
    assertEquals(centroids.get(centroids.size() - 1).mean(), max);
  }

  @Test
  public void testFullVerboseCapacityBoundaryRepairPreservesFirstMoment() {
    int centroidCount = 70;
    double min = 1.0e18;
    double[] means = new double[centroidCount];
    double[] weights = new double[centroidCount];
    double expectedWeight = 0.0;
    double expectedFirstMoment = 0.0;
    for (int i = 0; i < centroidCount; i++) {
      means[i] = min + 256.0 * i;
      weights[i] = i == 0 || i == centroidCount - 1 ? 2.0 : 1.0;
      expectedWeight += weights[i];
      expectedFirstMoment += means[i] * weights[i];
    }

    TDigest digest = TDigestUtils.deserialize(verboseBytes(20.0, means, weights));
    double actualWeight = 0.0;
    double actualFirstMoment = 0.0;
    for (Centroid centroid : digest.centroids()) {
      assertTrue(Double.isFinite(centroid.mean()));
      actualWeight += centroid.count();
      actualFirstMoment += centroid.mean() * centroid.count();
    }
    assertEquals(actualWeight, expectedWeight);
    assertEquals(actualFirstMoment, expectedFirstMoment, Math.ulp(expectedFirstMoment) * 4.0);
    assertEquals(digest.getMin(), means[0]);
    assertEquals(digest.getMax(), means[centroidCount - 1]);
  }

  @Test
  public void testLegacyWeightedInfiniteBoundariesDoNotCreateNaN() {
    ByteBuffer verbose = ByteBuffer.allocate(64);
    verbose.putInt(VERBOSE_ENCODING);
    verbose.putDouble(Double.NEGATIVE_INFINITY);
    verbose.putDouble(Double.POSITIVE_INFINITY);
    verbose.putDouble(20.0);
    verbose.putInt(2);
    verbose.putDouble(2.0);
    verbose.putDouble(Double.NEGATIVE_INFINITY);
    verbose.putDouble(2.0);
    verbose.putDouble(Double.POSITIVE_INFINITY);

    TDigest digest = TDigestUtils.deserialize(verbose.array());
    assertEquals(digest.size(), 4L);
    assertEquals(digest.getMin(), Double.NEGATIVE_INFINITY);
    assertEquals(digest.getMax(), Double.POSITIVE_INFINITY);
    for (Centroid centroid : digest.centroids()) {
      assertFalse(Double.isNaN(centroid.mean()));
      assertTrue(centroid.count() > 0);
    }
    assertEquals(digest.quantile(0.0), Double.NEGATIVE_INFINITY);
    assertEquals(digest.quantile(1.0), Double.POSITIVE_INFINITY);
  }

  @Test
  public void testBoundaryRepairDoesNotOverflowFirstMoment() {
    double min = 8.0e307;
    double mean = 1.0e308;
    double max = 1.2e308;
    ByteBuffer verbose = ByteBuffer.allocate(VERBOSE_HEADER_SIZE + 2 * Double.BYTES);
    verbose.putInt(VERBOSE_ENCODING);
    verbose.putDouble(min);
    verbose.putDouble(max);
    verbose.putDouble(100.0);
    verbose.putInt(1);
    verbose.putDouble(3.0);
    verbose.putDouble(mean);

    List<Centroid> centroids = List.copyOf(TDigestUtils.deserialize(verbose.array()).centroids());
    assertEquals(centroids.size(), 3);
    assertEquals(centroids.get(0).mean(), min);
    assertEquals(centroids.get(1).mean(), mean, 4.0 * Math.ulp(mean));
    assertEquals(centroids.get(2).mean(), max);
  }

  @Test
  public void testMalformedCentroidsAndResourceHeadersAreRejected() {
    byte[] bytes = verboseBytes(100.0, new double[]{0.0, 1.0}, new double[]{1.0, 1.0});
    assertThrows(IllegalArgumentException.class, () -> {
      byte[] malformed = bytes.clone();
      ByteBuffer.wrap(malformed).putDouble(20, Double.MAX_VALUE);
      TDigestUtils.deserialize(malformed);
    });
    assertThrows(IllegalArgumentException.class, () -> {
      byte[] malformed = bytes.clone();
      ByteBuffer.wrap(malformed).putDouble(40, Double.NaN);
      TDigestUtils.deserialize(malformed);
    });
    for (double weight : new double[]{0.0, -1.0, Double.NaN, Double.POSITIVE_INFINITY}) {
      assertThrows(IllegalArgumentException.class, () -> {
        byte[] malformed = bytes.clone();
        ByteBuffer.wrap(malformed).putDouble(32, weight);
        TDigestUtils.deserialize(malformed);
      });
    }
    assertThrows(IllegalArgumentException.class, () -> {
      byte[] malformed = bytes.clone();
      ByteBuffer.wrap(malformed).putDouble(40, 2.0);
      TDigestUtils.deserialize(malformed);
    });
    assertThrows(BufferUnderflowException.class, () -> {
      byte[] malformed = bytes.clone();
      ByteBuffer.wrap(malformed).putInt(28, Integer.MAX_VALUE);
      TDigestUtils.deserialize(malformed);
    });
  }

  private static TDigest craftedVerboseDigest(double compression, double[] means, double[] weights) {
    TDigest digest = mock(TDigest.class);
    when(digest.size()).thenReturn((long) means.length);
    when(digest.compression()).thenReturn(compression);
    when(digest.centroidCount()).thenReturn(means.length);
    doAnswer(invocation -> {
      ByteBuffer buffer = invocation.getArgument(0);
      buffer.put(verboseBytes(compression, means, weights));
      return null;
    }).when(digest).asBytes(any(ByteBuffer.class));
    return digest;
  }

  private static byte[] verboseBytes(double compression, double[] means, double[] weights) {
    ByteBuffer buffer = ByteBuffer.allocate(4 * Integer.BYTES + 2 * Double.BYTES
        + 2 * Double.BYTES * means.length);
    buffer.putInt(VERBOSE_ENCODING);
    buffer.putDouble(means[0]);
    buffer.putDouble(means[means.length - 1]);
    buffer.putDouble(compression);
    buffer.putInt(means.length);
    for (int i = 0; i < means.length; i++) {
      buffer.putDouble(weights[i]);
      buffer.putDouble(means[i]);
    }
    return buffer.array();
  }
}
