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
import org.apache.pinot.segment.spi.customobject.TDigest;
import org.apache.pinot.segment.spi.customobject.TDigest.Centroid;
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
    TDigest digest = TDigestUtils.createMergingDigest(100.0);
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }

    TDigest roundTripped = TDigestUtils.deserializeFinite(TDigestUtils.serialize(digest));
    assertEquals(roundTripped.size(), digest.size());
    assertEquals(roundTripped.getMin(), 0.0);
    assertEquals(roundTripped.getMax(), 999.0);
    assertEquals(roundTripped.quantile(0.75), 749.5, 2.0);
  }

  @Test
  public void testBufferedRawQuantilesRetainLegacyTwoLevelCompression() {
    SplittableRandom random = new SplittableRandom(10);
    double[] values = new double[94];
    TDigest raw = TDigestUtils.createMergingDigest(100.0);
    TDigest merged = TDigestUtils.createMergingDigest(100.0);
    for (int start = 0; start < values.length; start += 10) {
      TDigest partial = TDigestUtils.createMergingDigest(100.0);
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
  public void testPinotSerializationLeavesCallerScratchUntouched() {
    TDigest digest = TDigestUtils.createMergingDigest(100.0);
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }
    ByteBuffer scratch = ByteBuffer.allocate(4_096);
    scratch.putInt(1234);
    int originalPosition = scratch.position();

    byte[] bytes = TDigestUtils.serialize(digest, scratch);
    assertEquals(scratch.position(), originalPosition);
    assertEquals(scratch.getInt(0), 1234);
    scratch.put(0, (byte) 0);
    assertEquals(ByteBuffer.wrap(bytes).getInt(), VERBOSE_ENCODING);
    assertEquals(TDigestUtils.deserialize(bytes).size(), digest.size());
  }

  @Test
  public void testSerializeUsesNetworkOrderWithoutChangingScratchOrder() {
    TDigest digest = TDigestUtils.createMergingDigest(100.0);
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
    assertEquals(centroids.get(0).mean(), means[0]);
    assertEquals(centroids.get(0).weight(), 1L);
    assertEquals(centroids.get(centroids.size() - 1).mean(), means[centroidCount - 1]);
    assertEquals(centroids.get(centroids.size() - 1).weight(), 1L);
    double actualWeight = 0.0;
    double actualFirstMoment = 0.0;
    for (Centroid centroid : centroids) {
      actualWeight += centroid.weight();
      actualFirstMoment += centroid.mean() * centroid.weight();
    }
    assertEquals(actualWeight, expectedWeight);
    assertEquals(actualFirstMoment, expectedFirstMoment, Math.ulp(expectedFirstMoment) * 16.0);

    ByteBuffer roundTripped = ByteBuffer.wrap(TDigestUtils.serialize(digest));
    assertEquals(roundTripped.getInt(), VERBOSE_ENCODING,
        "Non-float-exact compression must remain in the double-precision wire format");
    roundTripped.position(Integer.BYTES + 2 * Double.BYTES);
    assertEquals(roundTripped.getDouble(), compression);
    int legacyCapacity = 2 * (int) Math.ceil(compression) + 10;
    assertTrue(roundTripped.getInt() <= legacyCapacity,
        "The result must fit the capacity actually allocated by the t-digest 3.2 verbose reader");
    TDigest decodedAgain = TDigestUtils.deserialize(roundTripped.array());
    assertEquals(decodedAgain.size(), (long) expectedWeight);
    assertEquals(decodedAgain.quantile(0.0), means[0]);
    assertEquals(decodedAgain.quantile(1.0), means[centroidCount - 1]);
    assertEquals(decodedAgain.quantile(0.5), means[centroidCount / 2], 4.0 * Math.ulp(means[0]));
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
      actualWeight += centroid.weight();
      actualFirstMoment += centroid.mean() * centroid.weight();
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
      assertTrue(centroid.weight() > 0);
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
  public void testMalformedHeadersAndNonFiniteWeightsAreRejected() {
    byte[] bytes = verboseBytes(100.0, new double[]{0.0, 1.0}, new double[]{1.0, 1.0});
    for (double compression : new double[]{Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
      assertThrows(IllegalArgumentException.class, () -> {
        byte[] malformed = bytes.clone();
        ByteBuffer.wrap(malformed).putDouble(20, compression);
        TDigestUtils.deserialize(malformed);
      });
    }
    for (double weight : new double[]{Double.NaN, Double.POSITIVE_INFINITY}) {
      assertThrows(IllegalArgumentException.class, () -> {
        byte[] malformed = bytes.clone();
        ByteBuffer.wrap(malformed).putDouble(32, weight);
        TDigestUtils.deserialize(malformed);
      });
    }
    assertThrows(BufferUnderflowException.class, () -> {
      byte[] malformed = bytes.clone();
      ByteBuffer.wrap(malformed).putInt(28, Integer.MAX_VALUE);
      TDigestUtils.deserialize(malformed);
    });
  }

  @Test
  public void testFiniteCompressionBelowMinimumRetainsLegacyClamping() {
    for (double compression : new double[]{-100.0, -1.0, 0.0, 1.0, 9.0}) {
      TDigest configured = TDigestUtils.createMergingDigest(compression);
      configured.add(42.0);
      assertEquals(configured.compression(), 10.0);
      assertEquals(configured.quantile(0.5), 42.0);
      byte[] bytes = verboseBytes(compression, new double[]{10.0, 20.0}, new double[]{1.0, 1.0});
      TDigest stored = TDigestUtils.deserialize(bytes);
      assertEquals(stored.compression(), 10.0);
      assertEquals(stored.size(), 2L);
      assertEquals(stored.quantile(0.5), 20.0);
      ByteBuffer compact = ByteBuffer.allocate(46);
      compact.putInt(SMALL_ENCODING).putDouble(10.0).putDouble(20.0).putFloat((float) compression);
      compact.putShort((short) 50).putShort((short) 250).putShort((short) 2);
      compact.putFloat(1.0F).putFloat(10.0F).putFloat(1.0F).putFloat(20.0F);
      TDigest compactStored = TDigestUtils.deserialize(compact.array());
      assertEquals(compactStored.compression(), 10.0);
      assertEquals(compactStored.quantile(0.5), 20.0);
    }
    for (double compression : new double[]{Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
      assertThrows(IllegalArgumentException.class, () -> TDigestUtils.createMergingDigest(compression));
    }
  }

  @Test
  public void testVerboseReencodingAndOneUlpEndpointDriftAreRepaired() {
    double[][] extremaCases = {
        {0.7, 0.9}, {Math.nextUp(0.7), Math.nextDown(0.9)}
    };
    double[][] meanCases = {
        {(double) 0.7F, (double) 0.9F}, {0.7, 0.9}
    };
    for (int i = 0; i < extremaCases.length; i++) {
      double[] extrema = extremaCases[i];
      byte[] bytes = verboseBytes(100.0, meanCases[i], new double[]{3.0, 4.0});
      ByteBuffer.wrap(bytes).putDouble(4, extrema[0]).putDouble(12, extrema[1]);
      TDigest stored = TDigestUtils.deserialize(bytes);
      assertEquals(stored.size(), 7L);
      assertEquals(stored.quantile(0.0), extrema[0]);
      assertEquals(stored.quantile(1.0), extrema[1]);
      for (Centroid centroid : stored.centroids()) {
        assertTrue(centroid.mean() >= extrema[0] && centroid.mean() <= extrema[1]);
      }
      stored.add(1.0);
      TDigest reEmitted = TDigestUtils.deserialize(TDigestUtils.serialize(stored));
      assertEquals(reEmitted.size(), 8L);
      assertEquals(reEmitted.quantile(0.0), extrema[0]);
      assertEquals(reEmitted.quantile(1.0), 1.0);
    }
  }

  @Test
  public void testCentroidBoundsRejectCorruptionAndPermitCompactEndpointRounding() {
    byte[] malformed = verboseBytes(100.0, new double[]{1.0, 9.0}, new double[]{3.0, 4.0});
    ByteBuffer.wrap(malformed).putDouble(4, 5.0);
    assertThrows(IllegalArgumentException.class, () -> TDigestUtils.deserialize(malformed));

    ByteBuffer compact = ByteBuffer.allocate(46);
    compact.putInt(SMALL_ENCODING).putDouble(0.1).putDouble(0.9).putFloat(100.0F);
    compact.putShort((short) 210).putShort((short) 1050).putShort((short) 2);
    compact.putFloat(1.0F).putFloat(0.1F).putFloat(1.0F).putFloat(0.9F);
    TDigest rounded = TDigestUtils.deserialize(compact.array());
    assertEquals(rounded.getMin(), 0.1);
    assertEquals(rounded.getMax(), 0.9);
    assertEquals(rounded.size(), 2L);
    assertTrue(rounded.quantile(0.5) >= 0.1 && rounded.quantile(0.5) <= 0.9);
    compact.putFloat(42, 1.0F);
    assertThrows(IllegalArgumentException.class, () -> TDigestUtils.deserialize(compact.array()));

    // These endpoints round outside the double extrema, so decode must clamp them before fresh verbose output.
    for (double[] extrema : new double[][]{
        {1.5251489664476673E-27, 0.9}, {0.1, Math.nextDown((double) 0.9F)}
    }) {
      ByteBuffer outsideRounded = ByteBuffer.allocate(46);
      outsideRounded.putInt(SMALL_ENCODING).putDouble(extrema[0]).putDouble(extrema[1]).putFloat(100.0F);
      outsideRounded.putShort((short) 210).putShort((short) 1050).putShort((short) 2);
      outsideRounded.putFloat(1.0F).putFloat((float) extrema[0]);
      outsideRounded.putFloat(1.0F).putFloat((float) extrema[1]);
      TDigest materialized = TDigestUtils.deserialize(outsideRounded.array());
      materialized.add(1.0);
      TDigest reEmitted = TDigestUtils.deserialize(TDigestUtils.serialize(materialized));
      assertEquals(reEmitted.size(), 3L);
      assertEquals(reEmitted.quantile(0.0), extrema[0]);
      assertEquals(reEmitted.quantile(1.0), 1.0);
    }
  }

  @Test
  public void testExtremeHistoricalCompressionDoesNotPreallocate() {
    for (double compression : new double[]{2_000_000.0, 1.0e7, Double.MAX_VALUE}) {
      byte[] bytes = verboseBytes(compression, new double[]{10.0, 20.0}, new double[]{1.0, 1.0});
      TDigest digest = TDigestUtils.deserialize(bytes);
      assertEquals(digest.compression(), compression);
      assertEquals(digest.size(), 2L);
      assertEquals(digest.quantile(0.5), 20.0);
      assertTrue(TDigestUtils.serialize(digest).length <= 64);
    }
  }

  @Test
  public void testFiniteWeightsAboveLongRangeRemainReadable() {
    byte[] bytes = verboseBytes(100.0, new double[]{1.0, 2.0, 3.0},
        new double[]{0x1p63, 0x1p63, 0x1p63});
    assertEquals(TDigestUtils.validateSerialized(bytes), 3.0 * 0x1p63);
    TDigest digest = TDigestUtils.deserialize(bytes);
    assertEquals(digest.size(), Long.MAX_VALUE);
    assertEquals(digest.quantile(0.5), 2.0, 1e-9);
    assertEquals(TDigestUtils.deserialize(TDigestUtils.serialize(digest)).quantile(0.5), 2.0, 1e-9);
  }

  @Test
  public void testLegacyPoisonedNumericalStateRemainsReadableWithoutInventingQuantiles() {
    byte[][] poisoned = {
        verboseBytes(100.0, new double[]{0.0, Double.NaN, 10.0}, new double[]{1.0, 5.0, 1.0}),
        verboseBytes(100.0, new double[]{0.0, 5.0, 10.0}, new double[]{1.0, -0.5, 1.0}),
        verboseBytes(100.0, new double[]{0.0, 5.0, 10.0}, new double[]{1.0, 0.0, 1.0}),
        verboseBytes(100.0, new double[]{0.0, Double.NEGATIVE_INFINITY, 10.0}, new double[]{1.0, 5.0, 1.0})
    };
    for (byte[] bytes : poisoned) {
      TDigest digest = TDigestUtils.deserialize(bytes);
      assertTrue(Double.isNaN(digest.quantile(0.5)));
      assertEquals(TDigestUtils.serialize(digest), bytes);
      assertTrue(Double.isNaN(TDigestUtils.deserializeFinite(bytes).quantile(0.5)));
      TDigest merged = TDigestUtils.createMergingDigest(100.0);
      merged.add(42.0);
      merged.add(digest);
      assertTrue(Double.isNaN(merged.quantile(0.5)));
      assertEquals(merged.getMin(), 0.0);
      assertEquals(merged.getMax(), 42.0);
    }
    // At this mass the addition rounds out of the total, but its newly known extrema must still be serialized.
    TDigest largePoisoned = TDigestUtils.deserialize(
        verboseBytes(100.0, new double[]{0.0, Double.NaN, 10.0}, new double[]{1.0, 0x1p63, 1.0}));
    largePoisoned.add(42.0);
    TDigest updated = TDigestUtils.deserialize(TDigestUtils.serialize(largePoisoned));
    assertEquals(updated.getMax(), 42.0);
    assertTrue(Double.isNaN(updated.quantile(0.5)));
  }

  @Test
  public void testDegradedTotalsRemainPresentAcrossCopyAndMergeOrder() {
    byte[][] poisoned = {
        verboseBytes(100.0, new double[]{10.0}, new double[]{-5.0}),
        verboseBytes(100.0, new double[]{0.0, 10.0}, new double[]{1.0, -1.0})
    };
    for (byte[] bytes : poisoned) {
      TDigest source = TDigestUtils.deserialize(bytes);
      assertFalse(source.hasValidStatistics());
      assertFalse(source.isEmpty());
      assertTrue(Double.isNaN(source.quantile(0.5)));
      assertTrue(Double.isNaN(source.cdf(5.0)));
      TDigest copy = TDigestUtils.createMergingDigest(100.0);
      copy.add(source);
      assertFalse(copy.hasValidStatistics());
      assertFalse(copy.isEmpty());
      assertEquals(copy.getTotalWeight(), source.getTotalWeight());
      for (boolean poisonedFirst : new boolean[]{false, true}) {
        TDigest merged = TDigestUtils.createMergingDigest(100.0);
        if (poisonedFirst) {
          merged.add(source);
          merged.add(42.0);
        } else {
          merged.add(42.0);
          merged.add(source);
        }
        assertFalse(merged.hasValidStatistics());
        assertTrue(Double.isNaN(merged.quantile(0.5)));
        assertTrue(Double.isNaN(merged.cdf(5.0)));
        assertEquals(merged.getTotalWeight(), source.getTotalWeight() + 1.0);
        TDigest roundTripped = TDigestUtils.deserialize(TDigestUtils.serialize(merged));
        assertFalse(roundTripped.hasValidStatistics());
        assertEquals(roundTripped.getMax(), 42.0);
      }
    }
  }

  @Test
  public void testUntouchedWeightedLegacyDigestOnlyRepairsBoundaries() {
    int count = 200;
    double[] means = new double[count];
    double[] weights = new double[count];
    for (int i = 0; i < count; i++) {
      means[i] = i;
      weights[i] = i == 0 || i == count - 1 ? 3.0 : 1.0;
    }
    TDigest pending = TDigestUtils.deserialize(verboseBytes(100.0, means, weights));
    ByteBuffer repaired = ByteBuffer.wrap(TDigestUtils.serialize(pending));
    assertEquals(repaired.getInt(), VERBOSE_ENCODING);
    assertEquals(repaired.getInt(28), count + 2,
        "Re-emitting stored centroids must only split the two weighted endpoints, without a lossy K1 merge");
    repaired.position(VERBOSE_HEADER_SIZE);
    double total = 0.0;
    for (int i = 0; i < count + 2; i++) {
      double weight = repaired.getDouble();
      double mean = repaired.getDouble();
      total += weight;
      if (i == 0 || i == count + 1) {
        assertEquals(weight, 1.0);
      } else if (i > 1 && i < count) {
        assertEquals(weight, 1.0);
        assertEquals(mean, i - 1.0, "Interior centroids must not be recompressed");
      }
    }
    assertEquals(total, count + 4.0);
  }

  @Test
  public void testCentroidViewReconstructionPreservesPreciseMass() {
    for (double weight : new double[]{0.5, 0x1p53}) {
      TDigest source = TDigestUtils.createMergingDigest(100.0);
      source.add(42.0, weight);
      TDigest reconstructed = TDigestUtils.createMergingDigest(100.0);
      double viewedWeight = 0.0;
      for (Centroid centroid : source.centroids()) {
        viewedWeight += centroid.weight();
        reconstructed.add(centroid.mean(), centroid.weight());
      }
      assertEquals(viewedWeight, weight);
      assertEquals(reconstructed.getTotalWeight(), weight);
      assertEquals(reconstructed.quantile(0.5), 42.0);
      assertEquals(TDigestUtils.deserialize(TDigestUtils.serialize(reconstructed)).getTotalWeight(), weight);
    }
  }

  @Test
  public void testLargeWeightRatiosKeepMergedMeansOrderedAndFinite() {
    // The backwards merge visits a tiny centroid before its much heavier neighbour. Without clamping, the
    // same-sign lerp undershoots 0.1 and crosses the preceding nextDown(0.1) centroid; multiplying before
    // dividing also overflows in the large-magnitude case. Both are valid double-weight legacy inputs.
    double[][] meanCases = {
        {0.001, Math.nextDown(0.1), 0.1, 10.0, 20.0, 100.0},
        {1.0, 2.0, 8.0e307, 1.0e308, 1.2e308, 1.7e308}
    };
    for (double[] means : meanCases) {
      PercentileTDigestAccumulator accumulator = new PercentileTDigestAccumulator(100.0);
      accumulator.add(0.0001);
      accumulator.compress(); // The next merge runs backwards.
      accumulator.addSerializedTDigest(verboseBytes(100.0, means,
          new double[]{1.0, 3.0e18, 0x1p53, 1.0, 5.0e18, 1.0}));
      ByteBuffer output = ByteBuffer.wrap(TDigestUtils.serialize(accumulator));
      assertEquals(output.getInt(), VERBOSE_ENCODING);
      output.position(28);
      int count = output.getInt();
      double previous = Double.NEGATIVE_INFINITY;
      double total = 0.0;
      for (int i = 0; i < count; i++) {
        double weight = output.getDouble();
        double mean = output.getDouble();
        assertTrue(Double.isFinite(mean) && mean >= previous, "Merged centroids must remain finite and ordered");
        assertTrue(weight > 0.0 && Double.isFinite(weight));
        total += weight;
        previous = mean;
      }
      assertEquals(total, 8.0e18 + 0x1p53);
      TDigest roundTripped = TDigestUtils.deserialize(output.array());
      assertTrue(Double.isFinite(roundTripped.quantile(0.5)));
      assertEquals(roundTripped.getMin(), 0.0001);
      assertEquals(roundTripped.getMax(), means[means.length - 1]);
    }
  }

  @Test
  public void testHistoricalRoundingInversionIsSortedWithoutLosingWeights() {
    byte[] bytes = verboseBytes(100.0, new double[]{0.0, 0.1, Math.nextDown(0.1), 1.0},
        new double[]{1.0, 39.0, 40.0, 1.0});
    TDigest digest = TDigestUtils.deserialize(bytes);
    assertEquals(digest.size(), 81L);
    assertEquals(digest.quantile(0.5), 0.1, Math.ulp(0.1));
    TDigest roundTripped = TDigestUtils.deserialize(TDigestUtils.serialize(digest));
    assertEquals(roundTripped.size(), 81L);
    assertEquals(roundTripped.quantile(0.5), 0.1, Math.ulp(0.1));
  }

  @Test
  public void testOversizedVerbosePayloadActuallyReducesToLegacyCapacity() {
    double compression = 100.1;
    int legacyCapacity = 2 * (int) Math.ceil(compression) + 10;
    int count = legacyCapacity + 3;
    double[] means = new double[count];
    double[] weights = new double[count];
    for (int i = 0; i < count; i++) {
      means[i] = 1.0e18 + i * 256.0;
      weights[i] = 1.0;
    }
    byte[] compatible = TDigestUtils.makeLegacyCompatible(verboseBytes(compression, means, weights));
    assertEquals(ByteBuffer.wrap(compatible).getInt(28), legacyCapacity,
        "The old verbose reader allocates this exact capacity, not the input centroid count");
    TDigest digest = TDigestUtils.deserialize(compatible);
    assertEquals(digest.size(), (long) count);
    assertEquals(digest.quantile(0.0), means[0]);
    assertEquals(digest.quantile(1.0), means[count - 1]);
    assertEquals(digest.quantile(0.5), means[count / 2], 512.0);
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
