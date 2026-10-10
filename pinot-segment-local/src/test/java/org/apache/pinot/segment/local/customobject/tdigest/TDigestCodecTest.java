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
package org.apache.pinot.segment.local.customobject.tdigest;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest.Centroid;
import org.testng.annotations.Test;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TDigestCodecTest {
  private static final int VERBOSE_ENCODING = 1;
  private static final int SMALL_ENCODING = 2;
  private static final int VERBOSE_HEADER_SIZE = 32;

  @Test
  public void testPinotDigestRetainsQuantileAccuracyAcrossLegacyBytes() {
    TDigest digest = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }

    TDigest roundTripped = PercentileTDigestAccumulator.fromBytes(digest.serialize());
    assertEquals(roundTripped.size(), digest.size());
    assertEquals(roundTripped.getMin(), 0.0);
    assertEquals(roundTripped.getMax(), 999.0);
    assertEquals(roundTripped.quantile(0.75), 749.5, 2.0);
  }

  @Test
  public void testBufferedRawQuantilesRetainLegacyTwoLevelCompression() {
    SplittableRandom random = new SplittableRandom(10);
    double[] values = new double[94];
    TDigest raw = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
    TDigest merged = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
    for (int start = 0; start < values.length; start += 10) {
      TDigest partial = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
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
  public void testCodecRejectsUnsupportedFreshFractionalEndpoints() {
    for (double[] weights : new double[][]{{0.5}, {1.5}, {0.3, 0.3}, {1.0, 0.3}}) {
      double[] means = new double[weights.length];
      Arrays.setAll(means, i -> i + 1.0);
      byte[] verbose = verboseBytes(100.0, means, weights);
      assertThrows(IllegalArgumentException.class, () -> TDigestCodec.makeLegacyCompatible(verbose));
      // Reading historical bytes retains their exact mass and payload instead of minting a new legacy shape.
      TDigest historical = PercentileTDigestAccumulator.fromBytes(verbose);
      assertTrue(historical.hasValidStatistics());
      historical.centroids();
      historical.compress();
      assertEquals(historical.serialize(), verbose);
    }
    byte[] interiorFractional = verboseBytes(100.0,
        new double[]{Double.NEGATIVE_INFINITY, 1.0, 2.0, Double.POSITIVE_INFINITY},
        new double[]{1.0, 0.3, 0.3, 1.0});
    assertEquals(TDigestCodec.makeLegacyCompatible(interiorFractional), interiorFractional,
        "Unit infinity endpoints allow fractional interior mass without reweighting");
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

    byte[] bytes = TDigestCodec.makeLegacyCompatible(verboseBytes(20.0, means, weights));
    ByteBuffer serialized = ByteBuffer.wrap(bytes);
    assertEquals(serialized.getInt(), VERBOSE_ENCODING);
    assertEquals(serialized.getDouble(), means[0]);
    assertEquals(serialized.getDouble(), means[centroidCount - 1]);
    assertEquals(serialized.getDouble(), 20.0);
    assertEquals(serialized.getInt(), 50, "Verbose output must fit the t-digest 3.2 centroid capacity");

    TDigest roundTripped = PercentileTDigestAccumulator.fromBytes(bytes);
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

    byte[] bytes = TDigestCodec.makeLegacyCompatible(verboseBytes(20.0, means, weights));
    ByteBuffer serialized = ByteBuffer.wrap(bytes);
    assertEquals(serialized.getInt(), SMALL_ENCODING);
    serialized.position(28);
    assertEquals(serialized.getShort(), centroidCount);

    TDigest roundTripped = PercentileTDigestAccumulator.fromBytes(bytes);
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

    ByteBuffer serialized =
        ByteBuffer.wrap(TDigestCodec.makeLegacyCompatible(verboseBytes(compression, means, weights)));
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

    ByteBuffer serialized =
        ByteBuffer.wrap(TDigestCodec.makeLegacyCompatible(verboseBytes(compression, means, weights)));
    assertEquals(serialized.getInt(), VERBOSE_ENCODING);
    serialized.position(Integer.BYTES + 2 * Double.BYTES);
    assertEquals(serialized.getDouble(), compression);
    int outputCount = serialized.getInt();
    assertEquals(outputCount, TDigestCodec.getDefaultCentroidCapacity(compression),
        "Verbose output must also fit t-digest 3.3's smaller fractional-compression capacity");
    assertTrue(outputCount <= TDigestCodec.getLegacyDefaultCentroidCapacity(compression));
  }

  @Test
  public void testSerializeForceCompressesOnce() {
    PercentileTDigestAccumulator digest = spy(new PercentileTDigestAccumulator(100.0));
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }

    digest.serialize();
    verify(digest, times(1)).compress();
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

    TDigest digest = PercentileTDigestAccumulator.fromBytes(input);
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

    ByteBuffer roundTripped = ByteBuffer.wrap(digest.serialize());
    assertEquals(roundTripped.getInt(), VERBOSE_ENCODING,
        "Non-float-exact compression must remain in the double-precision wire format");
    roundTripped.position(Integer.BYTES + 2 * Double.BYTES);
    assertEquals(roundTripped.getDouble(), compression);
    int legacyCapacity = Math.min(TDigestCodec.getLegacyDefaultCentroidCapacity(compression),
        TDigestCodec.getDefaultCentroidCapacity(compression));
    assertTrue(roundTripped.getInt() <= legacyCapacity,
        "The result must fit the capacity actually allocated by the t-digest 3.2 verbose reader");
    TDigest decodedAgain = PercentileTDigestAccumulator.fromBytes(roundTripped.array());
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

    TDigest digest = PercentileTDigestAccumulator.fromBytes(small.array());
    ByteBuffer reEmitted = ByteBuffer.wrap(digest.serialize());
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
  public void testCompactFiniteEndpointOverflowIsRestoredBeforePoisonClassification() {
    for (double[] means : new double[][]{{1.0, 2.0, 1e39}, {-1e39, -2.0, -1.0}, {-1e39, 0.0, 1e39}}) {
      ByteBuffer compact = ByteBuffer.allocate(TDigestCodec.SMALL_HEADER_SIZE + 3 * TDigestCodec.SMALL_CENTROID_SIZE);
      compact.putInt(SMALL_ENCODING).putDouble(means[0]).putDouble(means[2]).putFloat(100.0F);
      compact.putShort((short) 210).putShort((short) 1050).putShort((short) means.length);
      for (double mean : means) {
        compact.putFloat(1.0F).putFloat((float) mean);
      }
      TDigestCodec.SerializedTDigestMetadata metadata =
          TDigestCodec.inspectSerialized(ByteBuffer.wrap(compact.array()));
      assertFalse(metadata.needsLegacyFallback());
      assertFalse(metadata.hasNonFiniteMeans(), "Float overflow is recoverable from the finite double extrema");
      TDigest digest = PercentileTDigestAccumulator.fromBytes(compact.array());
      assertTrue(digest.hasValidStatistics());
      assertEquals(digest.quantile(0.5), means[1]);
      assertEquals(digest.quantile(0.0), means[0]);
      assertEquals(digest.quantile(1.0), means[2]);
      digest.add(0.0);
      TDigest merged = PercentileTDigestAccumulator.fromBytes(digest.serialize());
      assertTrue(merged.hasValidStatistics());
      assertTrue(Double.isFinite(merged.quantile(0.5)));
      assertEquals(merged.getTotalWeight(), 4.0);

      // The verbose format did not narrow its means: infinity under finite extrema is genuine unknown state.
      double[] corruptMeans = means.clone();
      int overflowIndex = means[0] < -Float.MAX_VALUE ? 0 : 2;
      corruptMeans[overflowIndex] = Math.copySign(Double.POSITIVE_INFINITY, means[overflowIndex]);
      byte[] verbose = verboseBytes(100.0, corruptMeans, new double[]{1.0, 1.0, 1.0});
      ByteBuffer.wrap(verbose).putDouble(4, means[0]).putDouble(12, means[2]);
      TDigest corrupted = PercentileTDigestAccumulator.fromBytes(verbose);
      assertFalse(corrupted.hasValidStatistics());
      assertTrue(Double.isNaN(corrupted.quantile(0.5)));
      assertEquals(corrupted.serialize(), verbose);
    }
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

    TDigest digest = PercentileTDigestAccumulator.fromBytes(verboseBytes(20.0, means, weights));
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

    TDigest digest = PercentileTDigestAccumulator.fromBytes(verbose.array());
    assertEquals(digest.size(), 4L);
    assertEquals(digest.getMin(), Double.NEGATIVE_INFINITY);
    assertEquals(digest.getMax(), Double.POSITIVE_INFINITY);
    for (Centroid centroid : digest.centroids()) {
      assertFalse(Double.isNaN(centroid.mean()));
      assertTrue(centroid.weight() > 0);
    }
    assertEquals(digest.quantile(0.0), Double.NEGATIVE_INFINITY);
    assertEquals(digest.quantile(1.0), Double.POSITIVE_INFINITY);

    // Both legacy encodings carry the same identifiable NaN tail. Pending metadata and direct decoding must
    // restore it identically, including when one row fans out to several groups and boundary repair changes count.
    for (double infinity : new double[]{Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
      double[] means = infinity < 0 ? new double[]{Double.NaN, -2, -1, 0}
          : new double[]{0, 1, 2, Double.NaN};
      double[] weights = infinity < 0 ? new double[]{4, 1, 0.5, 4} : new double[]{4, 0.5, 1, 4};
      double min = infinity < 0 ? infinity : 0;
      double max = infinity < 0 ? 0 : infinity;
      double[] repairedMeans = means.clone();
      repairedMeans[infinity < 0 ? 0 : 3] = infinity;
      TDigest expected = PercentileTDigestAccumulator.fromBytes(
          TDigestCodec.serializeCentroids(100, min, max, repairedMeans, weights, means.length));
      for (int encoding : new int[]{VERBOSE_ENCODING, TDigestCodec.SMALL_ENCODING}) {
        ByteBuffer bytes = ByteBuffer.allocate(encoding == VERBOSE_ENCODING ? 32 + 16 * means.length
            : 30 + 8 * means.length);
        bytes.putInt(encoding).putDouble(min).putDouble(max);
        if (encoding == VERBOSE_ENCODING) {
          bytes.putDouble(100).putInt(means.length);
          for (int i = 0; i < means.length; i++) {
            bytes.putDouble(weights[i]).putDouble(means[i]);
          }
        } else {
          bytes.putFloat(100).putShort((short) 210).putShort((short) 1050).putShort((short) means.length);
          for (int i = 0; i < means.length; i++) {
            bytes.putFloat((float) weights[i]).putFloat((float) means[i]);
          }
        }
        var input = new SerializedTDigestInput();
        input.reset(bytes.array());
        assertFalse(input.getMetadata().needsLegacyFallback());
        assertEquals(input.getMetadata().recoveredInfinityMean(), infinity);
        double[] decodedMeans = new double[means.length];
        double[] decodedWeights = new double[means.length];
        TDigestCodec.decodeSerializedCentroids(ByteBuffer.wrap(bytes.array()), input.getMetadata(), decodedMeans,
            decodedWeights);
        assertEquals(decodedMeans, repairedMeans);
        assertEquals(decodedWeights, weights);
        byte[] safeOutput = PercentileTDigestAccumulator.fromBytes(bytes.array()).serialize();
        assertTrue(Double.isNaN(TDigestCodec.inspectSerialized(ByteBuffer.wrap(safeOutput)).recoveredInfinityMean()));
        assertEquals(PercentileTDigestAccumulator.fromBytes(safeOutput).getTotalWeight(), 9.5);
        PercentileTDigestAccumulator pending = PercentileTDigestAccumulator.forReduction(input.getCompression());
        pending.addSerializedTDigest(input);
        TDigest direct = PercentileTDigestAccumulator.forLegacyAggregation(100);
        direct.add(0);
        ((PercentileTDigestAccumulator) direct).addSerializedTDigest(input);
        // The same input was decoded (and boundary-split) for the nonempty group before this empty group sees it.
        PercentileTDigestAccumulator fanout = PercentileTDigestAccumulator.forReduction(input.getCompression());
        fanout.addSerializedTDigest(input);
        assertEquals(fanout.getTotalWeight(), 9.5);
        for (double q : new double[]{0, 0.25, 0.5, 0.75, 1}) {
          assertEquals(pending.quantile(q), expected.quantile(q));
        }
        assertEquals(pending.cdf(0), expected.cdf(0));
        assertTrue(pending.hasValidStatistics());
        assertEquals(pending.getTotalWeight(), 9.5);
        assertEquals(input.getMetadata().centroidCount(), means.length,
            "Decoded boundary splits must not replace the encoded metadata count");
        assertTrue(direct.hasValidStatistics());
        assertEquals(direct.getTotalWeight(), 10.5);
        assertTrue(Double.isFinite(direct.quantile(0.5)));
        pending.add(0);
        assertEquals(pending.getTotalWeight(), 10.5);
        assertTrue(Double.isFinite(pending.quantile(0.5)));
        TDigest rewritten = PercentileTDigestAccumulator.fromBytes(pending.serialize());
        assertTrue(rewritten.hasValidStatistics());
        for (Centroid centroid : rewritten.centroids()) {
          assertFalse(Double.isNaN(centroid.mean()));
        }
      }
      // Equal infinite bounds identify the whole distribution even when every legacy mean is NaN.
      TDigest onlyInfinity = PercentileTDigestAccumulator.fromBytes(TDigestCodec.serializeCentroids(100,
          infinity, infinity, new double[]{Double.NaN, Double.NaN}, new double[]{3, 5}, 2));
      assertTrue(onlyInfinity.hasValidStatistics());
      assertEquals(onlyInfinity.quantile(0.5), infinity);
    }
    // Position or a +/-Infinity mixture can make a NaN's lost value ambiguous. Keep those bytes opaque.
    for (byte[] ambiguous : new byte[][]{
        TDigestCodec.serializeCentroids(100, 0, Double.POSITIVE_INFINITY,
            new double[]{0, Double.NaN, 1, Double.POSITIVE_INFINITY}, new double[]{1, 3, 1, 1}, 4),
        TDigestCodec.serializeCentroids(100, Double.NEGATIVE_INFINITY, 1,
            new double[]{Double.NEGATIVE_INFINITY, 0, Double.NaN, 1}, new double[]{1, 1, 3, 1}, 4),
        TDigestCodec.serializeCentroids(100, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
            new double[]{Double.NEGATIVE_INFINITY, 0, Double.NaN}, new double[]{1, 1, 3}, 3),
        TDigestCodec.serializeCentroids(100, 0, Double.POSITIVE_INFINITY,
            new double[]{0, Double.NaN}, new double[]{1, -1}, 2)}) {
      TDigest unknown = PercentileTDigestAccumulator.fromBytes(ambiguous);
      assertFalse(unknown.hasValidStatistics());
      assertTrue(Double.isNaN(unknown.quantile(0.5)));
      assertEquals(unknown.serialize(), ambiguous);
      assertThrows(IllegalArgumentException.class, () -> unknown.add(0));
    }
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

    List<Centroid> centroids = List.copyOf(PercentileTDigestAccumulator.fromBytes(verbose.array()).centroids());
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
        PercentileTDigestAccumulator.fromBytes(malformed);
      });
    }
    for (double weight : new double[]{Double.NaN, Double.POSITIVE_INFINITY}) {
      assertThrows(IllegalArgumentException.class, () -> {
        byte[] malformed = bytes.clone();
        ByteBuffer.wrap(malformed).putDouble(32, weight);
        PercentileTDigestAccumulator.fromBytes(malformed);
      });
    }
    assertThrows(BufferUnderflowException.class, () -> {
      byte[] malformed = bytes.clone();
      ByteBuffer.wrap(malformed).putInt(28, Integer.MAX_VALUE);
      PercentileTDigestAccumulator.fromBytes(malformed);
    });
  }

  @Test
  public void testFiniteCompressionBelowMinimumRetainsLegacyClamping() {
    for (double compression : new double[]{-100.0, -1.0, 0.0, 1.0, 9.0}) {
      TDigest configured = PercentileTDigestAccumulator.forLegacyAggregation(compression);
      configured.add(42.0);
      assertEquals(configured.compression(), 10.0);
      assertEquals(configured.quantile(0.5), 42.0);
      byte[] bytes = verboseBytes(compression, new double[]{10.0, 20.0}, new double[]{1.0, 1.0});
      TDigest stored = PercentileTDigestAccumulator.fromBytes(bytes);
      assertEquals(stored.compression(), 10.0);
      assertEquals(stored.size(), 2L);
      assertEquals(stored.quantile(0.5), 20.0);
      assertEquals(ByteBuffer.wrap(stored.serialize()).getDouble(20), 10.0);
      ByteBuffer compact = ByteBuffer.allocate(46);
      compact.putInt(SMALL_ENCODING).putDouble(10.0).putDouble(20.0).putFloat((float) compression);
      compact.putShort((short) 50).putShort((short) 250).putShort((short) 2);
      compact.putFloat(1.0F).putFloat(10.0F).putFloat(1.0F).putFloat(20.0F);
      TDigest compactStored = PercentileTDigestAccumulator.fromBytes(compact.array());
      assertEquals(compactStored.compression(), 10.0);
      assertEquals(compactStored.quantile(0.5), 20.0);
    }
    for (double compression : new double[]{Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
      assertThrows(IllegalArgumentException.class,
          () -> PercentileTDigestAccumulator.forLegacyAggregation(compression));
    }
  }

  @Test
  public void testLowRawCompressionHeaderIsNormalizedForLegacyVerboseCapacity() {
    double[] means = new double[25];
    double[] weights = new double[25];
    for (int i = 0; i < means.length; i++) {
      means[i] = i;
      weights[i] = 1.0;
    }
    assertEquals(TDigestCodec.getLegacyDefaultCentroidCapacity(5.0), 20);
    byte[] bytes = verboseBytes(5.0, means, weights);
    for (byte[] output : new byte[][]{
        TDigestCodec.makeLegacyCompatible(bytes), PercentileTDigestAccumulator.fromBytes(bytes).serialize()
    }) {
      ByteBuffer encoded = ByteBuffer.wrap(output);
      assertEquals(encoded.getInt(), VERBOSE_ENCODING);
      assertEquals(encoded.getDouble(20), 10.0);
      assertEquals(encoded.getInt(28), 25);
      assertTrue(encoded.getInt(28) <= TDigestCodec.getLegacyDefaultCentroidCapacity(encoded.getDouble(20)));
      assertEquals(PercentileTDigestAccumulator.fromBytes(output).size(), 25L);
    }
    // Unknown distributions cannot be reduced safely merely to fit an old reader's smaller array.
    means[12] = Double.NaN;
    byte[] poisoned = verboseBytes(5.0, means, weights);
    assertEquals(TDigestCodec.makeLegacyCompatible(poisoned), poisoned);
    assertEquals(PercentileTDigestAccumulator.fromBytes(poisoned).serialize(), poisoned);
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
      TDigest stored = PercentileTDigestAccumulator.fromBytes(bytes);
      assertEquals(stored.size(), 7L);
      assertEquals(stored.quantile(0.0), extrema[0]);
      assertEquals(stored.quantile(1.0), extrema[1]);
      for (Centroid centroid : stored.centroids()) {
        assertTrue(centroid.mean() >= extrema[0] && centroid.mean() <= extrema[1]);
      }
      stored.add(1.0);
      TDigest reEmitted = PercentileTDigestAccumulator.fromBytes(stored.serialize());
      assertEquals(reEmitted.size(), 8L);
      assertEquals(reEmitted.quantile(0.0), extrema[0]);
      assertEquals(reEmitted.quantile(1.0), 1.0);
    }
  }

  @Test
  public void testCentroidBoundsRetainUnknownStateAndPermitCompactEndpointRounding() {
    byte[] malformed = verboseBytes(100.0, new double[]{1.0, 9.0}, new double[]{3.0, 4.0});
    ByteBuffer.wrap(malformed).putDouble(4, 5.0);
    TDigest unknown = PercentileTDigestAccumulator.fromBytes(malformed);
    assertFalse(unknown.hasValidStatistics());
    assertTrue(Double.isNaN(unknown.quantile(0.5)));
    assertEquals(unknown.serialize(), malformed);
    byte[] inverted = malformed.clone();
    ByteBuffer.wrap(inverted).putDouble(4, 10.0);
    TDigest invertedStored = PercentileTDigestAccumulator.fromBytes(inverted);
    assertFalse(invertedStored.hasValidStatistics());
    assertTrue(Double.isNaN(invertedStored.quantile(0.5)));
    assertEquals(invertedStored.serialize(), inverted);

    byte[] drifted = verboseBytes(100.0, new double[]{0.7, 0.9}, new double[]{1.0, 1.0});
    ByteBuffer.wrap(drifted).putDouble(4, Math.nextUp(Math.nextUp(0.7)));
    TDigest driftedStored = PercentileTDigestAccumulator.fromBytes(drifted);
    assertEquals(driftedStored.getTotalWeight(), 2.0);
    assertFalse(driftedStored.hasValidStatistics());
    assertTrue(Double.isNaN(driftedStored.cdf(0.8)));
    assertEquals(driftedStored.serialize(), drifted);

    ByteBuffer compact = ByteBuffer.allocate(46);
    compact.putInt(SMALL_ENCODING).putDouble(0.1).putDouble(0.9).putFloat(100.0F);
    compact.putShort((short) 210).putShort((short) 1050).putShort((short) 2);
    compact.putFloat(1.0F).putFloat(0.1F).putFloat(1.0F).putFloat(0.9F);
    TDigest rounded = PercentileTDigestAccumulator.fromBytes(compact.array());
    assertEquals(rounded.getMin(), 0.1);
    assertEquals(rounded.getMax(), 0.9);
    assertEquals(rounded.size(), 2L);
    assertTrue(rounded.quantile(0.5) >= 0.1 && rounded.quantile(0.5) <= 0.9);
    compact.putFloat(42, 1.0F);
    TDigest unknownCompact = PercentileTDigestAccumulator.fromBytes(compact.array());
    assertFalse(unknownCompact.hasValidStatistics());
    assertEquals(unknownCompact.serialize(), compact.array());

    // These endpoints round outside the double extrema, so decode must clamp them before fresh verbose output.
    for (double[] extrema : new double[][]{
        {1.5251489664476673E-27, 0.9}, {0.1, Math.nextDown((double) 0.9F)}
    }) {
      ByteBuffer outsideRounded = ByteBuffer.allocate(46);
      outsideRounded.putInt(SMALL_ENCODING).putDouble(extrema[0]).putDouble(extrema[1]).putFloat(100.0F);
      outsideRounded.putShort((short) 210).putShort((short) 1050).putShort((short) 2);
      outsideRounded.putFloat(1.0F).putFloat((float) extrema[0]);
      outsideRounded.putFloat(1.0F).putFloat((float) extrema[1]);
      TDigest materialized = PercentileTDigestAccumulator.fromBytes(outsideRounded.array());
      materialized.add(1.0);
      TDigest reEmitted = PercentileTDigestAccumulator.fromBytes(materialized.serialize());
      assertEquals(reEmitted.size(), 3L);
      assertEquals(reEmitted.quantile(0.0), extrema[0]);
      assertEquals(reEmitted.quantile(1.0), 1.0);
    }
  }

  @Test
  public void testExtremeHistoricalCompressionDoesNotPreallocate() {
    for (double compression : new double[]{2_000_000.0, 1.0e7, Double.MAX_VALUE}) {
      byte[] bytes = verboseBytes(compression, new double[]{10.0, 20.0}, new double[]{1.0, 1.0});
      TDigest digest = PercentileTDigestAccumulator.fromBytes(bytes);
      assertEquals(digest.compression(), compression);
      assertEquals(digest.size(), 2L);
      assertEquals(digest.quantile(0.5), 20.0);
      assertTrue(digest.serialize().length <= 64);
    }
  }

  @Test
  public void testFiniteWeightsAboveLongRangeRemainReadable() {
    byte[] bytes = verboseBytes(100.0, new double[]{1.0, 2.0, 3.0},
        new double[]{0x1p63, 0x1p63, 0x1p63});
    assertEquals(TDigestCodec.validateSerialized(bytes), 3.0 * 0x1p63);
    TDigest digest = PercentileTDigestAccumulator.fromBytes(bytes);
    assertEquals(digest.size(), Long.MAX_VALUE);
    assertEquals(digest.quantile(0.5), 2.0, 1e-9);
    assertEquals(PercentileTDigestAccumulator.fromBytes(digest.serialize()).quantile(0.5), 2.0, 1e-9);
  }

  @Test
  public void testLegacyPoisonedNumericalStateRemainsReadableWithoutInventingQuantiles() {
    byte[][] poisoned = {
        verboseBytes(100.0, new double[]{0.0, Double.NaN, 10.0}, new double[]{1.0, 5.0, 1.0}),
        verboseBytes(100.0, new double[]{0.0, 5.0, 10.0}, new double[]{1.0, -0.5, 1.0}),
        verboseBytes(100.0, new double[]{0.0, Double.NEGATIVE_INFINITY, 10.0}, new double[]{1.0, 5.0, 1.0})
    };
    for (byte[] bytes : poisoned) {
      TDigest digest = PercentileTDigestAccumulator.fromBytes(bytes);
      assertTrue(Double.isNaN(digest.quantile(0.5)));
      assertEquals(digest.serialize(), bytes);
      assertTrue(Double.isNaN(PercentileTDigestAccumulator.fromBytes(bytes).quantile(0.5)));
      TDigest merged = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
      merged.add(42.0);
      assertThrows(IllegalArgumentException.class, () -> merged.add(digest));
      assertEquals(merged.getTotalWeight(), 1.0);
      assertEquals(merged.quantile(0.5), 42.0);
      assertEquals(merged.getMin(), 42.0);
      assertEquals(merged.getMax(), 42.0);
      assertEquals(digest.serialize(), bytes);
    }
    // Even an addition rounded out of the total is a mutation of the unknown distribution and must fail atomically.
    byte[] largeBytes = verboseBytes(100.0, new double[]{0.0, Double.NaN, 10.0}, new double[]{1.0, 0x1p63, 1.0});
    TDigest largePoisoned = PercentileTDigestAccumulator.fromBytes(largeBytes);
    assertThrows(IllegalArgumentException.class, () -> largePoisoned.add(42.0));
    assertEquals(largePoisoned.getMax(), 10.0);
    assertEquals(largePoisoned.serialize(), largeBytes);
  }

  @Test
  public void testZeroWeightCentroidsDoNotPoisonHealthyStoredOrMergedMass() {
    for (int encoding : new int[]{VERBOSE_ENCODING, SMALL_ENCODING}) {
      for (double unusedMean : new double[]{5.0, Double.NaN}) {
        byte[] bytes;
        if (encoding == VERBOSE_ENCODING) {
          bytes = verboseBytes(100.0, new double[]{0.0, unusedMean, 10.0}, new double[]{1.0, 0.0, 1.0});
        } else {
          ByteBuffer encoded = ByteBuffer.allocate(54);
          encoded.putInt(SMALL_ENCODING).putDouble(0.0).putDouble(10.0).putFloat(100.0F);
          encoded.putShort((short) 210).putShort((short) 1050).putShort((short) 3);
          encoded.putFloat(1.0F).putFloat(0.0F).putFloat(0.0F).putFloat((float) unusedMean);
          encoded.putFloat(1.0F).putFloat(10.0F);
          bytes = encoded.array();
        }
        TDigestCodec.SerializedTDigestMetadata metadata = TDigestCodec.inspectSerialized(ByteBuffer.wrap(bytes));
        assertTrue(metadata.hasZeroWeightCentroids());
        assertFalse(metadata.needsLegacyFallback());
        assertFalse(metadata.weightedBoundaries());
        TDigest stored = PercentileTDigestAccumulator.fromBytes(bytes);
        assertTrue(stored.hasValidStatistics());
        assertEquals(stored.getTotalWeight(), 2.0);
        assertEquals(stored.quantile(0.5), 10.0);
        TDigest emitted =
            PercentileTDigestAccumulator.fromBytes(PercentileTDigestAccumulator.fromBytes(bytes).serialize());
        assertEquals(emitted.centroidCount(), 2);
        for (Centroid centroid : emitted.centroids()) {
          assertTrue(centroid.weight() > 0.0);
        }
        for (boolean storedFirst : new boolean[]{false, true}) {
          TDigest merged = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
          if (storedFirst) {
            merged.add(PercentileTDigestAccumulator.fromBytes(bytes));
          }
          merged.add(10.0, 1_000_000.0);
          if (!storedFirst) {
            merged.add(PercentileTDigestAccumulator.fromBytes(bytes));
          }
          TDigest persisted = PercentileTDigestAccumulator.fromBytes(merged.serialize());
          assertTrue(persisted.hasValidStatistics());
          assertEquals(persisted.getTotalWeight(), 1_000_002.0);
          assertEquals(persisted.quantile(0.5), 10.0);
        }
      }
    }
  }

  @Test
  public void testLegacyPoisonedExtremaRemainReadableAndRetainBytes() {
    for (int encoding : new int[]{VERBOSE_ENCODING, SMALL_ENCODING}) {
      byte[] bytes;
      if (encoding == VERBOSE_ENCODING) {
        bytes = verboseBytes(100.0, new double[]{1.0, 2.0}, new double[]{1.0, 1.0});
      } else {
        ByteBuffer compact = ByteBuffer.allocate(46);
        compact.putInt(SMALL_ENCODING).putDouble(1.0).putDouble(2.0).putFloat(100.0F);
        compact.putShort((short) 210).putShort((short) 1050).putShort((short) 2);
        compact.putFloat(1.0F).putFloat(1.0F).putFloat(1.0F).putFloat(2.0F);
        bytes = compact.array();
      }
      for (int extremaOffset : new int[]{4, 12, -1}) {
        byte[] poisoned = bytes.clone();
        if (extremaOffset < 0) {
          ByteBuffer.wrap(poisoned).putDouble(4, 3.0).putDouble(12, -1.0);
        } else {
          ByteBuffer.wrap(poisoned).putLong(extremaOffset, 0x7ff8000000000042L);
        }
        assertEquals(TDigestCodec.validateSerialized(poisoned), 2.0);
        TDigest digest = PercentileTDigestAccumulator.fromBytes(poisoned);
        assertFalse(digest.hasValidStatistics());
        assertEquals(digest.size(), 2L);
        assertTrue(Double.isNaN(digest.quantile(0.5)));
        assertTrue(Double.isNaN(digest.cdf(1.5)));
        assertEquals(digest.serialize(), poisoned);
        assertTrue(Double.isNaN(PercentileTDigestAccumulator.fromBytes(poisoned).quantile(0.5)));
        assertThrows(IllegalArgumentException.class, () -> digest.add(42.0));
        assertEquals(digest.size(), 2L);
        assertEquals(digest.serialize(), poisoned);
        byte[] invalidCompression = poisoned.clone();
        if (encoding == VERBOSE_ENCODING) {
          ByteBuffer.wrap(invalidCompression).putDouble(20, Double.NaN);
        } else {
          ByteBuffer.wrap(invalidCompression).putFloat(20, Float.NaN);
        }
        assertThrows(IllegalArgumentException.class, () -> PercentileTDigestAccumulator.fromBytes(invalidCompression));
        if (encoding == SMALL_ENCODING) {
          byte[] invalidCapacity = poisoned.clone();
          ByteBuffer.wrap(invalidCapacity).putShort(24, (short) -2);
          assertThrows(IllegalArgumentException.class, () -> PercentileTDigestAccumulator.fromBytes(invalidCapacity));
        }
      }
    }
  }

  @Test
  public void testDegradedTotalsRemainPresentAcrossCopyAndMergeOrder() {
    byte[][] poisoned = {
        verboseBytes(100.0, new double[]{10.0}, new double[]{-5.0}),
        verboseBytes(100.0, new double[]{0.0, 10.0}, new double[]{1.0, -1.0})
    };
    for (byte[] bytes : poisoned) {
      TDigest source = PercentileTDigestAccumulator.fromBytes(bytes);
      assertFalse(source.hasValidStatistics());
      assertFalse(source.isEmpty());
      assertTrue(Double.isNaN(source.quantile(0.5)));
      assertTrue(Double.isNaN(source.cdf(5.0)));
      TDigest copy = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
      copy.add(source);
      assertFalse(copy.hasValidStatistics());
      assertFalse(copy.isEmpty());
      assertEquals(copy.getTotalWeight(), source.getTotalWeight());
      assertEquals(copy.serialize(), bytes);
      for (boolean poisonedFirst : new boolean[]{false, true}) {
        TDigest merged = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
        if (poisonedFirst) {
          merged.add(source);
          assertThrows(IllegalArgumentException.class, () -> merged.add(42.0));
          assertFalse(merged.hasValidStatistics());
          assertEquals(merged.getTotalWeight(), source.getTotalWeight());
          assertEquals(merged.serialize(), bytes);
        } else {
          merged.add(42.0);
          assertThrows(IllegalArgumentException.class, () -> merged.add(source));
          assertTrue(merged.hasValidStatistics());
          assertEquals(merged.getTotalWeight(), 1.0);
          assertEquals(merged.quantile(0.5), 42.0);
        }
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
    TDigest pending = PercentileTDigestAccumulator.fromBytes(verboseBytes(100.0, means, weights));
    assertTrue(TDigestCodec.inspectSerialized(ByteBuffer.wrap(verboseBytes(100.0, means, weights)))
        .weightedBoundaries());
    ByteBuffer repaired = ByteBuffer.wrap(pending.serialize());
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
      TDigest source = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
      source.add(42.0, weight);
      TDigest reconstructed = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
      double viewedWeight = 0.0;
      for (Centroid centroid : source.centroids()) {
        viewedWeight += centroid.weight();
        reconstructed.add(centroid.mean(), centroid.weight());
      }
      assertEquals(viewedWeight, weight);
      assertEquals(reconstructed.getTotalWeight(), weight);
      assertEquals(reconstructed.quantile(0.5), 42.0);
      if (weight < 1.0) {
        assertThrows(IllegalArgumentException.class, () -> reconstructed.serialize());
        assertEquals(reconstructed.getTotalWeight(), weight);
      } else {
        assertEquals(PercentileTDigestAccumulator.fromBytes(reconstructed.serialize()).getTotalWeight(), weight);
      }
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
      ByteBuffer output = ByteBuffer.wrap(accumulator.serialize());
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
      TDigest roundTripped = PercentileTDigestAccumulator.fromBytes(output.array());
      assertTrue(Double.isFinite(roundTripped.quantile(0.5)));
      assertEquals(roundTripped.getMin(), 0.0001);
      assertEquals(roundTripped.getMax(), means[means.length - 1]);
    }
  }

  @Test
  public void testHistoricalRoundingInversionIsSortedWithoutLosingWeights() {
    byte[] bytes = verboseBytes(100.0, new double[]{0.0, 0.1, Math.nextDown(0.1), 1.0},
        new double[]{1.0, 39.0, 40.0, 1.0});
    TDigest digest = PercentileTDigestAccumulator.fromBytes(bytes);
    assertEquals(digest.size(), 81L);
    assertEquals(digest.quantile(0.5), 0.1, Math.ulp(0.1));
    TDigest roundTripped = PercentileTDigestAccumulator.fromBytes(digest.serialize());
    assertEquals(roundTripped.size(), 81L);
    assertEquals(roundTripped.quantile(0.5), 0.1, Math.ulp(0.1));
  }

  @Test
  public void testOversizedVerbosePayloadActuallyReducesToLegacyCapacity() {
    double compression = 100.1;
    int legacyCapacity = Math.min(TDigestCodec.getLegacyDefaultCentroidCapacity(compression),
        TDigestCodec.getDefaultCentroidCapacity(compression));
    int count = legacyCapacity + 3;
    double[] means = new double[count];
    double[] weights = new double[count];
    for (int i = 0; i < count; i++) {
      means[i] = 1.0e18 + i * 256.0;
      weights[i] = 1.0;
    }
    byte[] compatible = TDigestCodec.makeLegacyCompatible(verboseBytes(compression, means, weights));
    assertEquals(ByteBuffer.wrap(compatible).getInt(28), legacyCapacity,
        "The old verbose reader allocates this exact capacity, not the input centroid count");
    TDigest digest = PercentileTDigestAccumulator.fromBytes(compatible);
    assertEquals(digest.size(), (long) count);
    assertEquals(digest.quantile(0.0), means[0]);
    assertEquals(digest.quantile(1.0), means[count - 1]);
    assertEquals(digest.quantile(0.5), means[count / 2], 512.0);
  }

  @Test
  public void testCompatibleEncodingFitsDefaultCapacityAtItsBoundary() {
    for (double compression : new double[]{10, 100}) {
      int defaultCapacity = TDigestCodec.getDefaultCentroidCapacity(compression);
      int legacyCapacity = TDigestCodec.getLegacyDefaultCentroidCapacity(compression);
      for (int count : new int[]{defaultCapacity - 1, defaultCapacity, defaultCapacity + 1}) {
        double[] means = new double[count];
        double[] weights = new double[count];
        Arrays.setAll(means, i -> i);
        Arrays.fill(weights, 1.0);
        byte[] bytes = TDigestCodec.makeLegacyCompatible(verboseBytes(compression, means, weights));
        TDigestCodec.SerializedTDigestMetadata metadata = TDigestCodec.inspectSerialized(ByteBuffer.wrap(bytes));
        if (count > defaultCapacity) {
          assertEquals(metadata.encoding(), VERBOSE_ENCODING);
          assertEquals(metadata.centroidCount(), legacyCapacity);
        } else {
          assertEquals(metadata.encoding(), count <= legacyCapacity ? VERBOSE_ENCODING : SMALL_ENCODING);
          assertEquals(metadata.centroidCount(), count);
        }
        assertTrue(bytes.length <= TDigestCodec.getMaxLegacyCompatibleByteSize(compression, count));
        TDigest digest = PercentileTDigestAccumulator.fromBytes(bytes);
        assertEquals(digest.getTotalWeight(), (double) count);
        assertEquals(digest.getMin(), 0.0);
        assertEquals(digest.getMax(), count - 1.0);
        assertTrue(Double.isFinite(digest.quantile(0.5)));
      }
    }
  }

  @Test
  public void testLegacyCapacityRepairRemovesAdjacentZeroMassBeforeMerging() {
    double compression = 100.1;
    int legacyCapacity = Math.min(TDigestCodec.getLegacyDefaultCentroidCapacity(compression),
        TDigestCodec.getDefaultCentroidCapacity(compression));
    for (int positiveCount : new int[]{2, legacyCapacity + 8}) {
      for (boolean unusedNaN : new boolean[]{false, true}) {
        double[] means = new double[positiveCount + 100];
        double[] weights = new double[means.length];
        means[0] = 1e18;
        weights[0] = 1.0;
        for (int i = 1; i <= 100; i++) {
          means[i] = unusedNaN ? Double.NaN : 1e18 + 256.0 * i;
        }
        for (int i = 1; i < positiveCount; i++) {
          means[i + 100] = 1e18 + 256.0 * i;
          weights[i + 100] = 1.0;
        }
        byte[] compatible = TDigestCodec.makeLegacyCompatible(verboseBytes(compression, means, weights));
        ByteBuffer output = ByteBuffer.wrap(compatible);
        assertEquals(output.getInt(), VERBOSE_ENCODING);
        int count = output.getInt(28);
        assertTrue(count <= legacyCapacity);
        assertEquals(count, Math.min(positiveCount, legacyCapacity));
        output.position(VERBOSE_HEADER_SIZE);
        double totalWeight = 0.0;
        double previous = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < count; i++) {
          double weight = output.getDouble();
          double mean = output.getDouble();
          assertTrue(weight > 0.0);
          assertTrue(Double.isFinite(mean) && mean >= previous);
          totalWeight += weight;
          previous = mean;
        }
        assertEquals(totalWeight, (double) positiveCount);
        TDigest digest = PercentileTDigestAccumulator.fromBytes(compatible);
        assertTrue(digest.hasValidStatistics());
        assertEquals(digest.getMin(), means[0]);
        assertEquals(digest.getMax(), means[means.length - 1]);
      }
    }
  }

  @Test
  public void testOversizedHistoricalDegradedPayloadRetainsReadableBytes() {
    double[] means = new double[300];
    double[] weights = new double[means.length];
    Arrays.setAll(means, i -> i);
    Arrays.fill(weights, 1.0);
    byte[] healthy = verboseBytes(100.0, means, weights);
    assertThrows(IllegalArgumentException.class, () -> PercentileTDigestAccumulator.fromBytes(healthy));
    means[150] = Double.NaN;
    byte[] historical = verboseBytes(100.0, means, weights);
    TDigestCodec.SerializedTDigestMetadata metadata = TDigestCodec.readSerializedHeader(ByteBuffer.wrap(historical));
    assertTrue(metadata.needsLegacyFallback());
    assertEquals(metadata.totalWeight(), 300.0);
    assertEquals(TDigestCodec.validateSerialized(historical), 300.0);
    TDigest retained = PercentileTDigestAccumulator.fromBytes(historical);
    assertFalse(retained.hasValidStatistics());
    assertTrue(Double.isNaN(retained.quantile(0.5)));
    assertEquals(retained.centroidCount(), 300);
    assertEquals(retained.centroids().size(), 300);
    retained.compress();
    assertEquals(retained.serialize(), historical);
    TDigest reread = PercentileTDigestAccumulator.fromBytes(retained.serialize());
    assertFalse(reread.hasValidStatistics());
    assertEquals(reread.getTotalWeight(), 300.0);
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
