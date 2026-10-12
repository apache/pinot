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

import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest.Centroid;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestCodec.SerializedTDigestMetadata;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/// Verifies legacy byte compatibility and numerical validation with invocation-local, unshared digest fixtures.
public class TDigestCodecTest {
  private static final int VERBOSE_ENCODING = 1;
  private static final int SMALL_ENCODING = 2;
  private static final int VERBOSE_HEADER_SIZE = 32;
  private static final double[] LEGACY_QUANTILES = {0, 0.25, 0.5, 0.75, 0.99, 1};

  @DataProvider
  public Object[][] legacyReleaseFixtures() throws Exception {
    List<Object[]> fixtures = new ArrayList<>();
    for (String version : List.of("3.2", "3.3")) {
      try (InputStream input = getClass().getResourceAsStream("/tdigest/legacy-" + version + ".tsv")) {
        assertNotNull(input, "Missing t-digest " + version + " fixtures");
        List<String> rows = new String(input.readAllBytes(), StandardCharsets.UTF_8).lines()
            .filter(line -> !line.isBlank() && !line.startsWith("#")).toList();
        assertEquals(rows.size(), version.equals("3.2") ? 69 : 57, "Incomplete t-digest " + version + " fixtures");
        for (String row : rows) {
          String[] fields = row.split("\t");
          fixtures.add(new Object[]{version + " " + fields[0], fields});
        }
      }
    }
    return fixtures.toArray(Object[][]::new);
  }

  @Test(dataProvider = "legacyReleaseFixtures")
  public void testFrozenLegacyReleaseBytes(String fixture, String[] fields) {
    assertTrue(fields.length > 13, fixture);
    byte[] bytes = Base64.getDecoder().decode(fields[fields.length - 1]);
    SerializedTDigestMetadata metadata = TDigestCodec.inspectSerialized(ByteBuffer.wrap(bytes));
    int count = metadata.centroidCount();
    assertEquals(count, fields.length - 13, fixture);
    double[] means = new double[count];
    double[] weights = new double[count];
    TDigestCodec.decodeSerializedCentroids(ByteBuffer.wrap(bytes), metadata, means, weights);
    double roundingBound = 0;
    for (int i = 0; i < count; i++) {
      String[] centroid = fields[12 + i].split(":");
      double legacyMean = Double.parseDouble(centroid[0]);
      // Compact endpoints can cross header extrema; legacy Centroid construction adds multiply/divide roundoff.
      double expectedMean = Math.max(metadata.min(), Math.min(legacyMean, metadata.max()));
      assertEquals(means[i], expectedMean, 8 * Math.ulp(expectedMean), fixture + " centroid " + i);
      assertEquals(weights[i], Double.parseDouble(centroid[1]), fixture + " centroid weight " + i);
      if (metadata.encoding() == SMALL_ENCODING) {
        roundingBound = Math.max(roundingBound, 2.0 * Math.ulp((float) legacyMean));
      }
    }
    double mass = Double.parseDouble(fields[1]);
    double min = Double.parseDouble(fields[2]);
    double max = Double.parseDouble(fields[3]);
    double compression = Double.parseDouble(fields[4]);
    TDigest digest = TDigestAccumulator.fromBytes(bytes);
    assertLegacyStatistics(digest, mass, min, max, compression, fixture);
    if (Boolean.parseBoolean(fields[5])) {
      for (int i = 0; i < LEGACY_QUANTILES.length; i++) {
        double expected = Double.parseDouble(fields[6 + i]);
        assertEquals(digest.quantile(LEGACY_QUANTILES[i]), expected,
            Math.max(roundingBound, 8 * Math.ulp(expected)), fixture + " quantile " + LEGACY_QUANTILES[i]);
      }
    }
    TDigestAccumulator merged = TDigestAccumulator.forReduction(compression);
    merged.add(1);
    merged.add(digest);
    merged.addSerializedTDigest(bytes);
    assertLegacyStatistics(TDigestAccumulator.fromBytes(merged.serialize()), 2 * mass + 1,
        Math.min(min, 1), Math.max(max, 1), compression, fixture + " merged");
    digest.add(1);
    assertLegacyStatistics(TDigestAccumulator.fromBytes(digest.serialize()), mass + 1,
        Math.min(min, 1), Math.max(max, 1), compression, fixture + " updated");
  }

  private static void assertLegacyStatistics(TDigest digest, double mass, double min, double max,
      double compression, String fixture) {
    assertTrue(digest.hasValidStatistics(), fixture);
    assertEquals(digest.getTotalWeight(), mass, fixture + " mass");
    assertEquals(digest.getMin(), min, fixture + " minimum");
    assertEquals(digest.getMax(), max, fixture + " maximum");
    assertEquals(digest.compression(), compression, fixture + " compression");
    double previous = Double.NEGATIVE_INFINITY;
    for (double quantile : LEGACY_QUANTILES) {
      double value = digest.quantile(quantile);
      assertTrue(Double.isFinite(value) && value >= previous && value >= min && value <= max,
          fixture + " bounded monotone quantile " + quantile);
      previous = value;
    }
  }

  @Test
  public void testPinotDigestRetainsQuantileAccuracyAcrossLegacyBytes() {
    TDigest digest = TDigestAccumulator.forLegacyAggregation(100.0);
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }

    TDigest roundTripped = TDigestAccumulator.fromBytes(digest.serialize());
    assertEquals(roundTripped.size(), digest.size());
    assertEquals(roundTripped.getMin(), 0.0);
    assertEquals(roundTripped.getMax(), 999.0);
    assertEquals(roundTripped.quantile(0.75), 749.5, 2.0);
  }

  @Test
  public void testEncodedQuantileMatchesDecodedHistoricalDistribution() {
    double[][] means = {{0, 10, 10, 20, 30}, {0, 10, 20}, {0, 20, 10, 30}, {0, 10, 30},
        {0, Double.NaN, 30}, {0, 10, Double.NaN}, {Double.NEGATIVE_INFINITY, 10, Double.POSITIVE_INFINITY},
        {-Double.MAX_VALUE, 0, Double.MAX_VALUE}, {-0.0, 0.0, 1}, {5}, {5}, {0, 10, 30}};
    double[][] weights = {{1, 3, 5, 2, 1}, {0.5, 4, 0.5}, {0.5, 2, 3, 1}, {1, 0, 1},
        {1, 4, 1}, {1, 4, 1}, {1, 4, 1}, {1, 4, 1}, {1, 4, 1}, {1.5}, {0}, {1, Double.MAX_VALUE, 1}};
    for (int encoding : new int[]{VERBOSE_ENCODING, SMALL_ENCODING}) {
      for (int i = 0; i < means.length; i++) {
        double min = means[i][0];
        double max = i == 5 ? Double.POSITIVE_INFINITY : means[i][means[i].length - 1];
        byte[] bytes = legacyBytes(encoding, 100, min, max, means[i], weights[i]);
        byte[] original = bytes.clone();
        TDigest decoded = TDigestAccumulator.fromBytes(bytes);
        for (double quantile : new double[]{0, 0.001, 0.1, 0.25, 0.5, 0.75, 0.9, 0.999, 1}) {
          assertEquals(TDigestCodec.quantile(bytes, quantile), decoded.quantile(quantile),
              "Encoding " + encoding + ", distribution " + i + ", quantile " + quantile);
        }
        assertEquals(bytes, original);
      }
    }
  }

  @Test
  public void testBufferedRawQuantilesRetainLegacyTwoLevelCompression() {
    SplittableRandom random = new SplittableRandom(10);
    double[] values = new double[94];
    TDigest raw = TDigestAccumulator.forLegacyAggregation(100.0);
    TDigest merged = TDigestAccumulator.forLegacyAggregation(100.0);
    for (int start = 0; start < values.length; start += 10) {
      TDigest partial = TDigestAccumulator.forLegacyAggregation(100.0);
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
      TDigest historical = TDigestAccumulator.fromBytes(verbose);
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

    TDigest roundTripped = TDigestAccumulator.fromBytes(bytes);
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

    TDigest roundTripped = TDigestAccumulator.fromBytes(bytes);
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
    TDigestAccumulator digest = spy(new TDigestAccumulator(100.0));
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }

    digest.serialize();
    verify(digest, times(1)).compress();
  }

  @Test
  public void testFractionalCompressionLegacyCapacityIsRepairedOnSerialization() {
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

    TDigest digest = TDigestAccumulator.fromBytes(input);
    assertEquals(input.position(), prefixSize + bytes.length);
    assertEquals(input.getInt(), trailingValue);
    assertEquals(digest.getMin(), means[0]);
    assertEquals(digest.getMax(), means[centroidCount - 1]);

    List<Centroid> centroids = List.copyOf(digest.centroids());
    assertEquals(centroids.get(0).mean(), means[0]);
    assertEquals(centroids.get(0).weight(), 2.0);
    assertEquals(centroids.get(centroids.size() - 1).mean(), means[centroidCount - 1]);
    assertEquals(centroids.get(centroids.size() - 1).weight(), 2.0);
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
    TDigest decodedAgain = TDigestAccumulator.fromBytes(roundTripped.array());
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

    TDigest digest = TDigestAccumulator.fromBytes(small.array());
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
      byte[] compact = legacyBytes(SMALL_ENCODING, 100.0, means[0], means[2], means,
          new double[]{1.0, 1.0, 1.0});
      TDigestCodec.SerializedTDigestMetadata metadata =
          TDigestCodec.inspectSerialized(ByteBuffer.wrap(compact));
      assertFalse(metadata.needsLegacyFallback());
      assertFalse(metadata.hasNonFiniteMeans(), "Float overflow is recoverable from the finite double extrema");
      TDigest digest = TDigestAccumulator.fromBytes(compact);
      assertTrue(digest.hasValidStatistics());
      assertEquals(digest.quantile(0.5), means[1]);
      assertEquals(digest.quantile(0.0), means[0]);
      assertEquals(digest.quantile(1.0), means[2]);
      digest.add(0.0);
      TDigest merged = TDigestAccumulator.fromBytes(digest.serialize());
      assertTrue(merged.hasValidStatistics());
      assertTrue(Double.isFinite(merged.quantile(0.5)));
      assertEquals(merged.getTotalWeight(), 4.0);

      // The verbose format did not narrow its means: infinity under finite extrema is genuine unknown state.
      double[] corruptMeans = means.clone();
      int overflowIndex = means[0] < -Float.MAX_VALUE ? 0 : 2;
      corruptMeans[overflowIndex] = Math.copySign(Double.POSITIVE_INFINITY, means[overflowIndex]);
      byte[] verbose = verboseBytes(100.0, corruptMeans, new double[]{1.0, 1.0, 1.0});
      ByteBuffer.wrap(verbose).putDouble(4, means[0]).putDouble(12, means[2]);
      TDigest corrupted = TDigestAccumulator.fromBytes(verbose);
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

    TDigest digest = TDigestAccumulator.fromBytes(verboseBytes(20.0, means, weights));
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
    TDigest digest = TDigestAccumulator.fromBytes(verboseBytes(20.0,
        new double[]{Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}, new double[]{2.0, 2.0}));
    assertEquals(digest.size(), 4L);
    assertEquals(digest.getMin(), Double.NEGATIVE_INFINITY);
    assertEquals(digest.getMax(), Double.POSITIVE_INFINITY);
    for (Centroid centroid : digest.centroids()) {
      assertFalse(Double.isNaN(centroid.mean()));
      assertTrue(centroid.weight() > 0);
    }
    assertEquals(digest.quantile(0.0), Double.NEGATIVE_INFINITY);
    assertEquals(digest.quantile(1.0), Double.POSITIVE_INFINITY);
    // The genuine 3.3 reader returns NaN for these weighted infinite interpolation brackets.
    assertEquals(digest.quantile(0.25), Double.NEGATIVE_INFINITY);
    assertEquals(digest.quantile(0.5), Double.NEGATIVE_INFINITY);
    TDigest zeroInterpolationWeight = TDigestAccumulator.fromBytes(TDigestCodec.serializeCentroids(100,
        0, Double.POSITIVE_INFINITY, new double[]{0, 10, Double.POSITIVE_INFINITY}, new double[]{1, 4, 3}, 3));
    assertEquals(zeroInterpolationWeight.quantile(0.375), 10.0);

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
      TDigest expected = TDigestAccumulator.fromBytes(
          TDigestCodec.serializeCentroids(100, min, max, repairedMeans, weights, means.length));
      for (int encoding : new int[]{VERBOSE_ENCODING, TDigestCodec.SMALL_ENCODING}) {
        byte[] bytes = legacyBytes(encoding, 100.0, min, max, means, weights);
        var input = new SerializedTDigestInput();
        input.reset(bytes);
        assertFalse(input.getMetadata().needsLegacyFallback());
        assertEquals(input.getMetadata().recoveredInfinityMean(), infinity);
        double[] decodedMeans = new double[means.length];
        double[] decodedWeights = new double[means.length];
        TDigestCodec.decodeSerializedCentroids(ByteBuffer.wrap(bytes), input.getMetadata(), decodedMeans,
            decodedWeights);
        assertEquals(decodedMeans, repairedMeans);
        assertEquals(decodedWeights, weights);
        byte[] safeOutput = TDigestAccumulator.fromBytes(bytes).serialize();
        assertTrue(Double.isNaN(TDigestCodec.inspectSerialized(ByteBuffer.wrap(safeOutput)).recoveredInfinityMean()));
        assertEquals(TDigestAccumulator.fromBytes(safeOutput).getTotalWeight(), 9.5);
        TDigestAccumulator pending = TDigestAccumulator.forReduction(input.getCompression());
        pending.addSerializedTDigest(input);
        TDigest direct = TDigestAccumulator.forLegacyAggregation(100);
        direct.add(0);
        ((TDigestAccumulator) direct).addSerializedTDigest(input);
        // The same input was decoded (and boundary-split) for the nonempty group before this empty group sees it.
        TDigestAccumulator fanout = TDigestAccumulator.forReduction(input.getCompression());
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
        TDigest rewritten = TDigestAccumulator.fromBytes(pending.serialize());
        assertTrue(rewritten.hasValidStatistics());
        for (Centroid centroid : rewritten.centroids()) {
          assertFalse(Double.isNaN(centroid.mean()));
        }
      }
      // Equal infinite bounds identify the whole distribution even when every legacy mean is NaN.
      TDigest onlyInfinity = TDigestAccumulator.fromBytes(TDigestCodec.serializeCentroids(100,
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
      TDigest unknown = TDigestAccumulator.fromBytes(ambiguous);
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
    byte[] verbose = legacyBytes(VERBOSE_ENCODING, 100.0, min, max, new double[]{mean}, new double[]{3.0});
    TDigest digest = TDigestAccumulator.fromBytes(verbose);
    List<Centroid> centroids = List.copyOf(TDigestAccumulator.fromBytes(digest.serialize()).centroids());
    assertEquals(centroids.size(), 3);
    assertEquals(centroids.get(0).mean(), min);
    assertEquals(centroids.get(1).mean(), mean, 4.0 * Math.ulp(mean));
    assertEquals(centroids.get(2).mean(), max);
  }

  @Test
  public void testMalformedHeadersAreRejected() {
    byte[] bytes = verboseBytes(100.0, new double[]{0.0, 1.0}, new double[]{1.0, 1.0});
    for (double compression : new double[]{Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
      assertThrows(IllegalArgumentException.class, () -> {
        byte[] malformed = bytes.clone();
        ByteBuffer.wrap(malformed).putDouble(20, compression);
        TDigestAccumulator.fromBytes(malformed);
      });
    }
    assertThrows(BufferUnderflowException.class, () -> {
      byte[] malformed = bytes.clone();
      ByteBuffer.wrap(malformed).putInt(28, Integer.MAX_VALUE);
      TDigestAccumulator.fromBytes(malformed);
    });
  }

  @Test
  public void testHistoricalNonFiniteMassRetainsOpaqueBytesWithoutRepeatedInspection() {
    for (int encoding : new int[]{VERBOSE_ENCODING, SMALL_ENCODING}) {
      for (double weight : new double[]{Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
        byte[] bytes = legacyBytes(encoding, 100.0, 1.0, 2.0, new double[]{1.0, 2.0},
            new double[]{weight, 1.0});
        SerializedTDigestInput input = new SerializedTDigestInput();
        input.reset(bytes);
        TDigestCodec.SerializedTDigestMetadata metadata = input.getMetadata();
        assertTrue(metadata.needsLegacyFallback());
        assertFalse(metadata.isCanonicalPayload());
        assertSame(input.getMetadata(), metadata, "Even an inspected NaN total must be cached");
        input.decode();
        assertSame(input.getMetadata(), metadata);
        for (TDigest digest : new TDigest[]{TDigestAccumulator.fromBytes(bytes),
            NonFiniteAwareTDigest.fromBytes(bytes)}) {
          assertFalse(digest.hasValidStatistics());
          assertFalse(digest.isEmpty());
          assertTrue(Double.isNaN(digest.quantile(0.5)));
          assertTrue(Double.isNaN(digest.cdf(1.5)));
          assertEquals(digest.serialize(), bytes);
          List<Centroid> centroids = List.copyOf(digest.centroids());
          assertEquals(centroids.size(), 2);
          assertEquals(centroids.get(0).mean(), 1.0);
          assertEquals(centroids.get(0).weight(), weight);
          assertThrows(IllegalArgumentException.class, () -> digest.add(3.0));
          TDigest healthy = TDigestAccumulator.forLegacyAggregation(100.0);
          healthy.add(42.0);
          assertThrows(IllegalArgumentException.class, () -> healthy.add(digest));
          assertEquals(healthy.getTotalWeight(), 1.0);
          assertEquals(healthy.quantile(0.5), 42.0);
          assertEquals(digest.serialize(), bytes);
        }
      }
    }
    byte[] overflow = verboseBytes(100.0, new double[]{1.0, 2.0},
        new double[]{Double.MAX_VALUE, Double.MAX_VALUE});
    TDigest historical = TDigestAccumulator.fromBytes(overflow);
    assertFalse(historical.hasValidStatistics());
    assertEquals(historical.getTotalWeight(), Double.POSITIVE_INFINITY);
    assertEquals(historical.serialize(), overflow);
    TDigest fresh = TDigestAccumulator.forLegacyAggregation(100.0);
    fresh.add(1.0, Double.MAX_VALUE);
    assertThrows(IllegalArgumentException.class, () -> fresh.add(2.0, Double.MAX_VALUE));
    assertEquals(fresh.getTotalWeight(), Double.MAX_VALUE);
    for (double weight : new double[]{Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
      assertThrows(IllegalArgumentException.class, () -> fresh.add(2.0, weight));
    }
  }

  @Test
  public void testFractionalEndpointMetadataSurvivesFanoutDecodeAndReset() {
    SerializedTDigestInput input = new SerializedTDigestInput();
    byte[][] historical = {
        TDigestCodec.serializeCentroids(100.0, 1.0, 10.0, new double[]{5.0, 10.0, 1.0, Double.NaN},
            new double[]{4.0, 0.75, 0.5, 0.0}, 4),
        verboseBytes(100.0, new double[]{42.0}, new double[]{1.5})
    };
    TDigest[] pending = new TDigest[historical.length];
    TDigestCodec.SerializedTDigestMetadata[] snapshots = new TDigestCodec.SerializedTDigestMetadata[historical.length];
    for (int row = 0; row < historical.length; row++) {
      byte[] bytes = historical[row];
      input.reset(bytes);
      TDigestAccumulator retained = TDigestAccumulator.forReduction(input.getCompression());
      retained.addSerializedTDigest(input);
      pending[row] = retained;
      TDigestCodec.SerializedTDigestMetadata metadata = input.getMetadata();
      snapshots[row] = metadata;
      double minMean = bytes == historical[0] ? 1.0 : 42.0;
      double maxMean = bytes == historical[0] ? 10.0 : 42.0;
      assertEquals(metadata.historicalFractionalMinMean(), minMean);
      assertEquals(metadata.historicalFractionalMaxMean(), maxMean);
      for (int group = 0; group < 5; group++) {
        TDigestAccumulator target = TDigestAccumulator.forLegacyAggregation(100.0);
        assertTrue(input.recordHistoricalFractionalBoundaries(target));
        assertEquals(target.getHistoricalFractionalBoundaryMean(true), minMean);
        assertEquals(target.getHistoricalFractionalBoundaryMean(false), maxMean);
        assertSame(input.getMetadata(), metadata);
        input.decode();
      }
    }
    input.reset(verboseBytes(100.0, new double[]{0.0, 10.0}, new double[]{1.0, 1.0}));
    TDigestAccumulator fresh = TDigestAccumulator.forLegacyAggregation(100.0);
    assertFalse(input.recordHistoricalFractionalBoundaries(fresh));
    assertTrue(Double.isNaN(fresh.getHistoricalFractionalBoundaryMean(true)));
    assertTrue(Double.isNaN(fresh.getHistoricalFractionalBoundaryMean(false)));
    input.decode();
    for (int row = 0; row < historical.length; row++) {
      double expectedWeight = row == 0 ? 5.25 : 1.5;
      double expectedMin = row == 0 ? 1.0 : 42.0;
      double expectedMax = row == 0 ? 10.0 : 42.0;
      assertEquals(snapshots[row].totalWeight(), expectedWeight);
      assertEquals(snapshots[row].historicalFractionalMinMean(), expectedMin);
      assertEquals(snapshots[row].historicalFractionalMaxMean(), expectedMax);
      assertEquals(pending[row].getTotalWeight(), expectedWeight);
      assertEquals(pending[row].getMin(), expectedMin);
      assertEquals(pending[row].getMax(), expectedMax);
      assertEquals(pending[row].serialize(), historical[row], "Reset must not alias retained bytes or metadata");
      assertEquals(pending[row].quantile(0.5), TDigestAccumulator.fromBytes(historical[row]).quantile(0.5));
    }
  }

  @Test
  public void testInheritedFractionalEndpointsRemainExactAcrossMergesAndCopies() {
    byte[] historical = verboseBytes(100.0, new double[]{1.0, 5.0, 10.0}, new double[]{0.5, 4.0, 0.75});
    for (double compression : new double[]{10.0, 100.0, 1_000.0}) {
      for (boolean wrapped : new boolean[]{false, true}) {
        TDigest source = wrapped ? NonFiniteAwareTDigest.fromBytes(historical).copy()
            : TDigestAccumulator.fromBytes(historical);
        TDigestAccumulator merged = TDigestAccumulator.forLegacyAggregation(compression);
        merged.add(7.0);
        merged.add(source);
        merged.centroids();
        merged.compress();
        byte[] output = merged.serialize();
        List<Centroid> centroids = List.copyOf(TDigestAccumulator.fromBytes(output).centroids());
        assertEquals(centroids.get(0).mean(), 1.0);
        assertEquals(centroids.get(0).weight(), 0.5);
        assertEquals(centroids.get(centroids.size() - 1).mean(), 10.0);
        assertEquals(centroids.get(centroids.size() - 1).weight(), 0.75);
        assertEquals(merged.getTotalWeight(), 6.25);
        assertEquals(source.serialize(), historical);
        merged.add(Math.nextDown(1.0), 0.25);
        // An adjacent fresh endpoint must not inherit a historical endpoint's exception.
        assertThrows(IllegalArgumentException.class, merged::serialize);
      }
    }
  }

  @Test
  public void testFreshWeightAtInheritedMeanDoesNotAcquireProvenance() {
    byte[] historical = verboseBytes(100.0, new double[]{5.0, 10.0}, new double[]{0.4, 1.0});
    for (double freshWeight : new double[]{0.4, 0.6}) {
      for (boolean historyFirst : new boolean[]{false, true}) {
        for (boolean wrapped : new boolean[]{false, true}) {
          TDigest digest = wrapped ? NonFiniteAwareTDigest.forLegacyAggregation(100.0)
              : TDigestAccumulator.forLegacyAggregation(100.0);
          if (!historyFirst) {
            digest.add(5.0, freshWeight);
          }
          digest.add(TDigestAccumulator.fromBytes(historical));
          if (historyFirst) {
            digest.add(5.0, freshWeight);
          }
          TDigest copy = wrapped ? ((NonFiniteAwareTDigest) digest).copy()
              : ((TDigestAccumulator) digest).copy();
          assertThrows(IllegalArgumentException.class, copy::serialize);
          TDigest merged = TDigestAccumulator.forReduction(100.0);
          merged.add(digest);
          assertThrows(IllegalArgumentException.class, merged::serialize);
          assertEquals(merged.getTotalWeight(), 1.4 + freshWeight, 1e-12);
        }
      }
    }
    TDigestAccumulator interior = TDigestAccumulator.fromBytes(historical);
    interior.add(7.0, 0.25);
    assertEquals(TDigestAccumulator.fromBytes(interior.serialize()).getTotalWeight(), 1.65, 1e-12);
  }

  @Test
  public void testRecoveredInfinityKeepsInheritedFractionalBoundary() {
    byte[] historical = TDigestCodec.serializeCentroids(100.0, 1.0, Double.POSITIVE_INFINITY,
        new double[]{1.0, 3.0, Double.NaN}, new double[]{0.5, 1.0, 1.0}, 3);
    assertEquals(TDigestAccumulator.fromBytes(historical).serialize(), historical);
    byte[] repaired = TDigestCodec.makeLegacyCompatible(historical);
    TDigest decoded = TDigestAccumulator.fromBytes(repaired);
    assertTrue(decoded.hasValidStatistics());
    assertEquals(decoded.getTotalWeight(), 2.5);
    assertEquals(decoded.getMin(), 1.0);
    assertEquals(decoded.getMax(), Double.POSITIVE_INFINITY);
    List<Centroid> centroids = List.copyOf(decoded.centroids());
    assertEquals(centroids.getFirst().weight(), 0.5);
    assertEquals(centroids.getLast().mean(), Double.POSITIVE_INFINITY);
    assertEquals(decoded.serialize(), repaired);
  }

  @Test
  public void testFiniteCompressionBelowMinimumRetainsLegacyClamping() {
    for (double compression : new double[]{-100.0, -1.0, 0.0, 1.0, 9.0}) {
      for (TDigest configured : new TDigest[]{new TDigestAccumulator((int) compression),
          new TDigestAccumulator(compression), TDigestAccumulator.forReduction(compression),
          TDigestAccumulator.forLegacyAggregation(compression)}) {
        configured.add(42.0);
        assertEquals(configured.compression(), 10.0);
        assertEquals(configured.quantile(0.5), 42.0);
      }
      TDigest configured = TDigestAccumulator.forLegacyAggregation(compression);
      configured.add(42.0);
      assertEquals(configured.compression(), 10.0);
      assertEquals(configured.quantile(0.5), 42.0);
      byte[] bytes = verboseBytes(compression, new double[]{10.0, 20.0}, new double[]{1.0, 1.0});
      TDigest stored = TDigestAccumulator.fromBytes(bytes);
      assertEquals(stored.compression(), 10.0);
      assertEquals(stored.size(), 2L);
      assertEquals(stored.quantile(0.5), 20.0);
      assertEquals(ByteBuffer.wrap(stored.serialize()).getDouble(20), 10.0);
      ByteBuffer compact = ByteBuffer.allocate(46);
      compact.putInt(SMALL_ENCODING).putDouble(10.0).putDouble(20.0).putFloat((float) compression);
      compact.putShort((short) 50).putShort((short) 250).putShort((short) 2);
      compact.putFloat(1.0F).putFloat(10.0F).putFloat(1.0F).putFloat(20.0F);
      TDigest compactStored = TDigestAccumulator.fromBytes(compact.array());
      assertEquals(compactStored.compression(), 10.0);
      assertEquals(compactStored.quantile(0.5), 20.0);
    }
    for (double compression : new double[]{Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
      assertThrows(IllegalArgumentException.class,
          () -> TDigestAccumulator.forLegacyAggregation(compression));
      assertThrows(IllegalArgumentException.class, () -> new TDigestAccumulator(compression));
      assertThrows(IllegalArgumentException.class, () -> TDigestAccumulator.forReduction(compression));
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
        TDigestCodec.makeLegacyCompatible(bytes), TDigestAccumulator.fromBytes(bytes).serialize()
    }) {
      ByteBuffer encoded = ByteBuffer.wrap(output);
      assertEquals(encoded.getInt(), VERBOSE_ENCODING);
      assertEquals(encoded.getDouble(20), 10.0);
      assertEquals(encoded.getInt(28), 25);
      assertTrue(encoded.getInt(28) <= TDigestCodec.getLegacyDefaultCentroidCapacity(encoded.getDouble(20)));
      assertEquals(TDigestAccumulator.fromBytes(output).size(), 25L);
    }
    // Unknown distributions cannot be reduced safely merely to fit an old reader's smaller array.
    means[12] = Double.NaN;
    byte[] poisoned = verboseBytes(5.0, means, weights);
    assertEquals(TDigestCodec.makeLegacyCompatible(poisoned), poisoned);
    assertEquals(TDigestAccumulator.fromBytes(poisoned).serialize(), poisoned);
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
      TDigest stored = TDigestAccumulator.fromBytes(bytes);
      assertEquals(stored.size(), 7L);
      assertEquals(stored.quantile(0.0), extrema[0]);
      assertEquals(stored.quantile(1.0), extrema[1]);
      for (Centroid centroid : stored.centroids()) {
        assertTrue(centroid.mean() >= extrema[0] && centroid.mean() <= extrema[1]);
      }
      stored.add(1.0);
      TDigest reEmitted = TDigestAccumulator.fromBytes(stored.serialize());
      assertEquals(reEmitted.size(), 8L);
      assertEquals(reEmitted.quantile(0.0), extrema[0]);
      assertEquals(reEmitted.quantile(1.0), 1.0);
    }
  }

  @Test
  public void testPreviousAccumulatorMergeRoundoffRemainsReadable() {
    // Produced by the pre-migration accumulator: add(min), compress(), then merge finite positive-weight
    // centroids [min, min, nextUp(min), max / 10, max / 5, max]. Its backward mean update undershoots min
    // by 6, 26 or 410 endpoint ULPs. The last case first passed through compact encoding before merging.
    double[][] cases = {
        {0.7, 100.0, 0.7, 0.6999999999999993, 3e18, 0x1p53, 5e18},
        {0.1, 100.0, 0.1, 0.09999999999999964, 3e18, 0x1p53, 5e18},
        {0.1, 1000.0, 0.1, 0.09999999999999432, 3e18, 0x1p53, 5e18},
        {0.7, 100.0, (double) 0.7F, 0.6999999880790719, (double) (float) 3e18,
            0x1p53 + 2.0, (double) (float) 5e18}
    };
    for (double[] values : cases) {
      for (boolean reflect : new boolean[]{false, true}) {
        double[] means = {values[2], values[2], values[3], values[1] / 5.0, values[1]};
        double[] weights = {1.0, values[4], values[5], values[6], 1.0};
        double min = values[0];
        double max = values[1];
        if (reflect) {
          for (int i = 0; i < means.length / 2; i++) {
            int other = means.length - 1 - i;
            double mean = means[i];
            means[i] = -means[other];
            means[other] = -mean;
            double weight = weights[i];
            weights[i] = weights[other];
            weights[other] = weight;
          }
          means[means.length / 2] = -means[means.length / 2];
          min = -values[1];
          max = -values[0];
        }
        byte[] bytes = TDigestCodec.serializeCentroids(100.0, min, max, means, weights, means.length);
        TDigest stored = TDigestAccumulator.fromBytes(bytes);
        assertTrue(stored.hasValidStatistics());
        assertEquals(stored.getTotalWeight(), Arrays.stream(weights).sum());
        assertEquals(stored.quantile(0.0), min);
        assertEquals(stored.quantile(1.0), max);
        assertTrue(Double.isFinite(stored.quantile(0.5)));
        for (Centroid centroid : stored.centroids()) {
          assertTrue(centroid.mean() >= min && centroid.mean() <= max);
        }
        assertTrue(TDigestAccumulator.fromBytes(stored.serialize()).hasValidStatistics());
      }
    }
  }

  @Test
  public void testLegacyRoundoffAllowanceStaysFiniteAndBounded() {
    for (double[] extrema : new double[][]{
        {0.75, 1.0}, {-1.0, -0.75}, {-1.0, 1.0}, {0.0, Double.MIN_VALUE},
        {1e-30, 1e30}, {1.0, Double.MAX_VALUE}, {-Double.MAX_VALUE, Double.MAX_VALUE}
    }) {
      double tolerance = 4.0 * Math.max(Math.ulp(extrema[0]), Math.ulp(extrema[1]));
      for (int side = 0; side < 2; side++) {
        double endpoint = extrema[side];
        double rounded = (double) (float) endpoint;
        double boundary = side == 0 ? endpoint - tolerance : endpoint + tolerance;
        if (Double.isFinite(rounded)) {
          boundary = side == 0 ? Math.min(boundary, rounded) : Math.max(boundary, rounded);
        }
        byte[] bytes = verboseBytes(100.0, extrema, new double[]{1.0, 1.0});
        int meanOffset = VERBOSE_HEADER_SIZE + Double.BYTES + side * TDigestCodec.VERBOSE_CENTROID_SIZE;
        ByteBuffer.wrap(bytes).putDouble(meanOffset, boundary);
        assertEquals(TDigestCodec.inspectSerialized(ByteBuffer.wrap(bytes)).needsLegacyFallback(),
            !Double.isFinite(boundary));
        double outside = side == 0 ? Math.nextDown(boundary) : Math.nextUp(boundary);
        ByteBuffer.wrap(bytes).putDouble(meanOffset, outside);
        assertTrue(TDigestCodec.inspectSerialized(ByteBuffer.wrap(bytes)).needsLegacyFallback());
      }
    }
  }

  @Test
  public void testCentroidBoundsRetainUnknownStateAndPermitCompactEndpointRounding() {
    byte[] malformed = verboseBytes(100.0, new double[]{1.0, 9.0}, new double[]{3.0, 4.0});
    ByteBuffer.wrap(malformed).putDouble(4, 5.0);
    TDigest unknown = TDigestAccumulator.fromBytes(malformed);
    assertFalse(unknown.hasValidStatistics());
    assertTrue(Double.isNaN(unknown.quantile(0.5)));
    assertEquals(unknown.serialize(), malformed);
    byte[] inverted = malformed.clone();
    ByteBuffer.wrap(inverted).putDouble(4, 10.0);
    TDigest invertedStored = TDigestAccumulator.fromBytes(inverted);
    assertFalse(invertedStored.hasValidStatistics());
    assertTrue(Double.isNaN(invertedStored.quantile(0.5)));
    assertEquals(invertedStored.serialize(), inverted);

    byte[] drifted = verboseBytes(100.0, new double[]{0.75, 1.0}, new double[]{1.0, 1.0});
    ByteBuffer.wrap(drifted).putDouble(40, 0.75 - 5.0 * Math.ulp(1.0));
    TDigest driftedStored = TDigestAccumulator.fromBytes(drifted);
    assertEquals(driftedStored.getTotalWeight(), 2.0);
    assertFalse(driftedStored.hasValidStatistics());
    assertTrue(Double.isNaN(driftedStored.cdf(0.8)));
    assertEquals(driftedStored.serialize(), drifted);

    ByteBuffer compact = ByteBuffer.wrap(legacyBytes(SMALL_ENCODING, 100.0, 0.1, 0.9,
        new double[]{0.1, 0.9}, new double[]{1.0, 1.0}));
    TDigest rounded = TDigestAccumulator.fromBytes(compact.array());
    assertEquals(rounded.getMin(), 0.1);
    assertEquals(rounded.getMax(), 0.9);
    assertEquals(rounded.size(), 2L);
    assertTrue(rounded.quantile(0.5) >= 0.1 && rounded.quantile(0.5) <= 0.9);
    compact.putFloat(42, 1.0F);
    TDigest unknownCompact = TDigestAccumulator.fromBytes(compact.array());
    assertFalse(unknownCompact.hasValidStatistics());
    assertEquals(unknownCompact.serialize(), compact.array());

    // These endpoints round outside the double extrema, so decode must clamp them before fresh verbose output.
    for (double[] extrema : new double[][]{
        {1.5251489664476673E-27, 0.9}, {0.1, Math.nextDown((double) 0.9F)}
    }) {
      byte[] outsideRounded = legacyBytes(SMALL_ENCODING, 100.0, extrema[0], extrema[1], extrema,
          new double[]{1.0, 1.0});
      TDigest materialized = TDigestAccumulator.fromBytes(outsideRounded);
      materialized.add(1.0);
      TDigest reEmitted = TDigestAccumulator.fromBytes(materialized.serialize());
      assertEquals(reEmitted.size(), 3L);
      assertEquals(reEmitted.quantile(0.0), extrema[0]);
      assertEquals(reEmitted.quantile(1.0), 1.0);
    }
  }

  @Test
  public void testExtremeHistoricalCompressionDoesNotPreallocate() {
    for (double compression : new double[]{2_000_000.0, 1.0e7, Double.MAX_VALUE}) {
      byte[] bytes = verboseBytes(compression, new double[]{10.0, 20.0}, new double[]{1.0, 1.0});
      TDigest digest = TDigestAccumulator.fromBytes(bytes);
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
    TDigest digest = TDigestAccumulator.fromBytes(bytes);
    assertEquals(digest.size(), Long.MAX_VALUE);
    assertEquals(digest.quantile(0.5), 2.0, 1e-9);
    assertEquals(TDigestAccumulator.fromBytes(digest.serialize()).quantile(0.5), 2.0, 1e-9);
  }

  @Test
  public void testLegacyPoisonedNumericalStateRemainsReadableWithoutInventingQuantiles() {
    byte[][] poisoned = {
        verboseBytes(100.0, new double[]{0.0, Double.NaN, 10.0}, new double[]{1.0, 5.0, 1.0}),
        verboseBytes(100.0, new double[]{0.0, 5.0, 10.0}, new double[]{1.0, -0.5, 1.0}),
        verboseBytes(100.0, new double[]{0.0, Double.NEGATIVE_INFINITY, 10.0}, new double[]{1.0, 5.0, 1.0})
    };
    for (byte[] bytes : poisoned) {
      TDigest digest = TDigestAccumulator.fromBytes(bytes);
      assertTrue(Double.isNaN(digest.quantile(0.5)));
      assertEquals(digest.serialize(), bytes);
      assertTrue(Double.isNaN(TDigestAccumulator.fromBytes(bytes).quantile(0.5)));
      TDigest merged = TDigestAccumulator.forLegacyAggregation(100.0);
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
    TDigest largePoisoned = TDigestAccumulator.fromBytes(largeBytes);
    assertThrows(IllegalArgumentException.class, () -> largePoisoned.add(42.0));
    assertEquals(largePoisoned.getMax(), 10.0);
    assertEquals(largePoisoned.serialize(), largeBytes);
  }

  @Test
  public void testZeroWeightCentroidsDoNotPoisonHealthyStoredOrMergedMass() {
    for (int encoding : new int[]{VERBOSE_ENCODING, SMALL_ENCODING}) {
      for (double unusedMean : new double[]{5.0, Double.NaN}) {
        byte[] bytes = legacyBytes(encoding, 100.0, 0.0, 10.0, new double[]{0.0, unusedMean, 10.0},
            new double[]{1.0, 0.0, 1.0});
        TDigestCodec.SerializedTDigestMetadata metadata = TDigestCodec.inspectSerialized(ByteBuffer.wrap(bytes));
        assertTrue(metadata.hasZeroWeightCentroids());
        assertFalse(metadata.needsLegacyFallback());
        assertFalse(metadata.weightedBoundaries());
        TDigest stored = TDigestAccumulator.fromBytes(bytes);
        assertTrue(stored.hasValidStatistics());
        assertEquals(stored.getTotalWeight(), 2.0);
        assertEquals(stored.quantile(0.5), 10.0);
        TDigest emitted =
            TDigestAccumulator.fromBytes(TDigestAccumulator.fromBytes(bytes).serialize());
        assertEquals(emitted.centroidCount(), 2);
        for (Centroid centroid : emitted.centroids()) {
          assertTrue(centroid.weight() > 0.0);
        }
        for (boolean storedFirst : new boolean[]{false, true}) {
          TDigest merged = TDigestAccumulator.forLegacyAggregation(100.0);
          if (storedFirst) {
            merged.add(TDigestAccumulator.fromBytes(bytes));
          }
          merged.add(10.0, 1_000_000.0);
          if (!storedFirst) {
            merged.add(TDigestAccumulator.fromBytes(bytes));
          }
          TDigest persisted = TDigestAccumulator.fromBytes(merged.serialize());
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
      byte[] bytes = legacyBytes(encoding, 100.0, 1.0, 2.0, new double[]{1.0, 2.0}, new double[]{1.0, 1.0});
      for (int extremaOffset : new int[]{4, 12, -1}) {
        byte[] poisoned = bytes.clone();
        if (extremaOffset < 0) {
          ByteBuffer.wrap(poisoned).putDouble(4, 3.0).putDouble(12, -1.0);
        } else {
          ByteBuffer.wrap(poisoned).putLong(extremaOffset, 0x7ff8000000000042L);
        }
        assertEquals(TDigestCodec.validateSerialized(poisoned), 2.0);
        TDigest digest = TDigestAccumulator.fromBytes(poisoned);
        assertFalse(digest.hasValidStatistics());
        assertEquals(digest.size(), 2L);
        assertTrue(Double.isNaN(digest.quantile(0.5)));
        assertTrue(Double.isNaN(digest.cdf(1.5)));
        assertEquals(digest.serialize(), poisoned);
        assertTrue(Double.isNaN(TDigestAccumulator.fromBytes(poisoned).quantile(0.5)));
        assertThrows(IllegalArgumentException.class, () -> digest.add(42.0));
        assertEquals(digest.size(), 2L);
        assertEquals(digest.serialize(), poisoned);
        byte[] invalidCompression = poisoned.clone();
        if (encoding == VERBOSE_ENCODING) {
          ByteBuffer.wrap(invalidCompression).putDouble(20, Double.NaN);
        } else {
          ByteBuffer.wrap(invalidCompression).putFloat(20, Float.NaN);
        }
        assertThrows(IllegalArgumentException.class, () -> TDigestAccumulator.fromBytes(invalidCompression));
        if (encoding == SMALL_ENCODING) {
          byte[] invalidCapacity = poisoned.clone();
          ByteBuffer.wrap(invalidCapacity).putShort(24, (short) -2);
          assertThrows(IllegalArgumentException.class, () -> TDigestAccumulator.fromBytes(invalidCapacity));
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
      TDigest source = TDigestAccumulator.fromBytes(bytes);
      assertFalse(source.hasValidStatistics());
      assertFalse(source.isEmpty());
      assertTrue(Double.isNaN(source.quantile(0.5)));
      assertTrue(Double.isNaN(source.cdf(5.0)));
      TDigest copy = TDigestAccumulator.forLegacyAggregation(100.0);
      copy.add(source);
      assertFalse(copy.hasValidStatistics());
      assertFalse(copy.isEmpty());
      assertEquals(copy.getTotalWeight(), source.getTotalWeight());
      assertEquals(copy.serialize(), bytes);
      for (boolean poisonedFirst : new boolean[]{false, true}) {
        TDigest merged = TDigestAccumulator.forLegacyAggregation(100.0);
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
  public void testWeightedLegacyReadsDeferBoundaryRepair() {
    byte[] bytes = TDigestCodec.serializeCentroids(100, 0, 100, new double[]{10, 90}, new double[]{10, 10}, 2);
    TDigest digest = TDigestAccumulator.fromBytes(bytes);
    assertEquals(digest.quantile(0.25), 10.0);
    assertEquals(digest.cdf(25), 0.34375);
    assertEquals(digest.cdf(75), 0.65625);
    assertEquals(digest.centroidCount(), 2);
    assertEquals(List.copyOf(digest.centroids()).getFirst().weight(), 10.0);
    byte[] serialized = digest.serialize();
    double firstMoment = 0.0;
    for (Centroid centroid : TDigestAccumulator.fromBytes(serialized).centroids()) {
      firstMoment += centroid.mean() * centroid.weight();
    }
    assertEquals(firstMoment, 1000.0, 1e-10);
    assertEquals(TDigestAccumulator.fromBytes(serialized).getTotalWeight(), 20.0);
    byte[] singleton = TDigestCodec.serializeCentroids(100, 0, 2, new double[]{1}, new double[]{10}, 1);
    TDigest untouchedSource = TDigestAccumulator.fromBytes(singleton);
    TDigest queriedSource = TDigestAccumulator.fromBytes(singleton);
    assertEquals(queriedSource.quantile(0.5), 1.0);
    TDigest untouchedTarget = TDigestAccumulator.forLegacyAggregation(100);
    TDigest queriedTarget = TDigestAccumulator.forLegacyAggregation(100);
    untouchedTarget.add(100);
    queriedTarget.add(100);
    untouchedTarget.add(untouchedSource);
    queriedTarget.add(queriedSource);
    assertEquals(queriedTarget.serialize(), untouchedTarget.serialize());
    TDigest self = TDigestAccumulator.fromBytes(bytes);
    TDigestAccumulator copied = TDigestAccumulator.fromBytes(bytes);
    self.add(self);
    copied.add(copied.copy());
    assertEquals(self.serialize(), copied.serialize());
    byte[] infinite = TDigestCodec.serializeCentroids(100, 0, Double.POSITIVE_INFINITY,
        new double[]{99, Double.POSITIVE_INFINITY}, new double[]{10, 3}, 2);
    TDigest nativeTarget = TDigestAccumulator.forLegacyAggregation(100);
    TDigest wrappedTarget = TDigestAccumulator.forLegacyAggregation(100);
    nativeTarget.add(50);
    wrappedTarget.add(50);
    nativeTarget.add(TDigestAccumulator.fromBytes(infinite));
    wrappedTarget.add(NonFiniteAwareTDigest.fromBytes(infinite));
    assertEquals(wrappedTarget.serialize(), nativeTarget.serialize());
  }

  @Test
  public void testUnorderedWeightedBoundariesUseSortedExtremes() {
    for (double max : new double[]{30.0, Double.POSITIVE_INFINITY}) {
      double upperMean = Double.isFinite(max) ? 25.0 : max;
      byte[] unordered = TDigestCodec.serializeCentroids(100, 0, max,
          new double[]{10, 5, upperMean, 20}, new double[]{1, 3, 3, 1}, 4);
      byte[] sorted = TDigestCodec.serializeCentroids(100, 0, max,
          new double[]{5, 10, 20, upperMean}, new double[]{3, 1, 1, 3}, 4);
      assertTrue(TDigestCodec.inspectSerialized(ByteBuffer.wrap(unordered)).weightedBoundaries());
      for (boolean wrapped : new boolean[]{false, true}) {
        TDigest actual = wrapped ? NonFiniteAwareTDigest.fromBytes(unordered)
            : TDigestAccumulator.fromBytes(unordered);
        TDigest expected = wrapped ? NonFiniteAwareTDigest.fromBytes(sorted)
            : TDigestAccumulator.fromBytes(sorted);
        actual.add(15);
        expected.add(15);
        assertEquals(actual.getTotalWeight(), 9.0);
        assertEquals(actual.serialize(), expected.serialize());
      }
    }
    // Signed zeros alone must trigger stored total-order sorting when both survive the endpoint clamp.
    for (boolean interior : new boolean[]{false, true}) {
      double[] means = interior ? new double[]{1, +0.0, -0.0, Double.POSITIVE_INFINITY}
          : new double[]{+0.0, -0.0, Double.POSITIVE_INFINITY};
      double[] weights = interior ? new double[]{1, 1, 3, 1} : new double[]{1, 3, 1};
      double[] sortedMeans = interior ? new double[]{-0.0, +0.0, 1, Double.POSITIVE_INFINITY}
          : new double[]{-0.0, +0.0, Double.POSITIVE_INFINITY};
      double[] sortedWeights = interior ? new double[]{3, 1, 1, 1} : new double[]{3, 1, 1};
      byte[] unordered = TDigestCodec.serializeCentroids(100, -0.0, Double.POSITIVE_INFINITY,
          means, weights, means.length);
      byte[] sorted = TDigestCodec.serializeCentroids(100, -0.0, Double.POSITIVE_INFINITY,
          sortedMeans, sortedWeights, sortedMeans.length);
      assertTrue(TDigestCodec.inspectSerialized(ByteBuffer.wrap(unordered)).unorderedMeans());
      assertTrue(TDigestCodec.inspectSerialized(ByteBuffer.wrap(unordered)).weightedBoundaries());
      for (boolean wrapped : new boolean[]{false, true}) {
        TDigest actual = wrapped ? NonFiniteAwareTDigest.fromBytes(unordered)
            : TDigestAccumulator.fromBytes(unordered);
        TDigest expected = wrapped ? NonFiniteAwareTDigest.fromBytes(sorted)
            : TDigestAccumulator.fromBytes(sorted);
        actual.add(-5);
        expected.add(-5);
        assertEquals(actual.serialize(), expected.serialize());
      }
    }
    byte[] clampedZeros = TDigestCodec.serializeCentroids(100, +0.0, Double.POSITIVE_INFINITY,
        new double[]{+0.0, -0.0, Double.POSITIVE_INFINITY}, new double[]{1, 3, 1}, 3);
    assertFalse(TDigestCodec.inspectSerialized(ByteBuffer.wrap(clampedZeros)).unorderedMeans());
    assertFalse(TDigestCodec.inspectSerialized(ByteBuffer.wrap(clampedZeros)).weightedBoundaries());
    // Stable sorting keeps the first minimum and last maximum when endpoint means are tied.
    for (double[] weights : new double[][]{{1, 1, 3, 3, 1}, {1, 3, 1, 1, 3}}) {
      byte[] bytes = TDigestCodec.serializeCentroids(100, 0, 20,
          new double[]{10, 0, 0, 20, 20}, weights, weights.length);
      assertEquals(TDigestCodec.inspectSerialized(ByteBuffer.wrap(bytes)).weightedBoundaries(), weights[1] > 1);
    }
  }

  @Test
  public void testUnrepresentableLegacyBoundariesRemainReadable() {
    for (byte[] bytes : new byte[][]{
        TDigestCodec.serializeCentroids(100, 0, 100, new double[]{99}, new double[]{10}, 1),
        TDigestCodec.serializeCentroids(100, 0, 100, new double[]{99}, new double[]{2}, 1),
        TDigestCodec.serializeCentroids(100, 0, 100, new double[]{1, 10}, new double[]{10, 10}, 2)}) {
      TDigestAccumulator digest = TDigestAccumulator.fromBytes(bytes);
      TDigest untouched = TDigestAccumulator.fromBytes(bytes);
      assertEquals(untouched.serialize(), bytes);
      byte[] emptyBytes = TDigestAccumulator.forLegacyAggregation(100).serialize();
      digest.addSerializedTDigest(emptyBytes);
      SerializedTDigestInput emptyInput = new SerializedTDigestInput();
      emptyInput.reset(emptyBytes);
      digest.addSerializedTDigest(emptyInput);
      List<Centroid> original = List.copyOf(digest.centroids());
      digest.addSerializedTDigest(emptyBytes);
      digest.addSerializedTDigestDirect(emptyInput);
      double originalWeight = digest.getTotalWeight();
      if (original.size() == 1) {
        for (double q : new double[]{0, 0.05, 0.5, 0.95, 1}) {
          assertEquals(digest.quantile(q), 99.0);
        }
        for (double x : new double[]{0, 25, 99, 100}) {
          assertEquals(digest.cdf(x), 0.5);
        }
      }
      assertThrows(IllegalArgumentException.class, () -> digest.add(50));
      assertThrows(IllegalArgumentException.class, () -> digest.add(50, 2));
      assertThrows(IllegalArgumentException.class, () -> digest.add(new double[]{50}, 0, 1));
      TDigest healthy = TDigestAccumulator.forLegacyAggregation(100);
      healthy.add(50);
      assertThrows(IllegalArgumentException.class, () -> digest.add(healthy));
      assertThrows(IllegalArgumentException.class, () -> healthy.add(digest));
      assertEquals(healthy.getTotalWeight(), 1.0);
      assertEquals(digest.getTotalWeight(), originalWeight);
      assertTrue(digest.hasValidStatistics());
      List<Centroid> retained = List.copyOf(digest.centroids());
      for (int i = 0; i < original.size(); i++) {
        assertEquals(retained.get(i).mean(), original.get(i).mean());
        assertEquals(retained.get(i).weight(), original.get(i).weight());
      }
      assertEquals(digest.serialize(), bytes);
      assertEquals(digest.copy().serialize(), bytes);
      TDigest empty = TDigestAccumulator.forLegacyAggregation(100);
      empty.add(digest);
      assertEquals(empty.serialize(), bytes);
    }
  }

  @Test
  public void testCdfInfiniteNeighborsRetainsLegacyUnitExclusions() {
    for (double leftWeight : new double[]{1, 4}) {
      for (double rightWeight : new double[]{1, 3}) {
        byte[] bytes = TDigestCodec.serializeCentroids(100, 0, Double.POSITIVE_INFINITY,
            new double[]{0, 10, Double.POSITIVE_INFINITY}, new double[]{1, leftWeight, rightWeight}, 3);
        TDigest digest = TDigestAccumulator.fromBytes(bytes);
        double expectedMass = leftWeight == 1 ? 2 : 1 + leftWeight / 2.0;
        assertEquals(digest.cdf(25), expectedMass / (1 + leftWeight + rightWeight));
      }
    }
    TDigest lowerInfinity = TDigestAccumulator.fromBytes(TDigestCodec.serializeCentroids(100,
        Double.NEGATIVE_INFINITY, 20, new double[]{Double.NEGATIVE_INFINITY, 10, 20}, new double[]{3, 4, 1}, 3));
    // The old Pinot accumulator also returned 3/8; the genuine 3.3 reader returns NaN on the same bytes.
    assertEquals(lowerInfinity.cdf(0), 3.0 / 8.0);
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
    TDigest pending = TDigestAccumulator.fromBytes(verboseBytes(100.0, means, weights));
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
      TDigest source = TDigestAccumulator.forLegacyAggregation(100.0);
      source.add(42.0, weight);
      TDigest reconstructed = TDigestAccumulator.forLegacyAggregation(100.0);
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
        assertEquals(TDigestAccumulator.fromBytes(reconstructed.serialize()).getTotalWeight(), weight);
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
      TDigestAccumulator accumulator = new TDigestAccumulator(100.0);
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
      TDigest roundTripped = TDigestAccumulator.fromBytes(output.array());
      assertTrue(Double.isFinite(roundTripped.quantile(0.5)));
      assertEquals(roundTripped.getMin(), 0.0001);
      assertEquals(roundTripped.getMax(), means[means.length - 1]);
    }
  }

  @Test
  public void testHistoricalRoundingInversionIsSortedWithoutLosingWeights() {
    byte[] bytes = verboseBytes(100.0, new double[]{0.0, 0.1, Math.nextDown(0.1), 1.0},
        new double[]{1.0, 39.0, 40.0, 1.0});
    TDigest digest = TDigestAccumulator.fromBytes(bytes);
    assertEquals(digest.size(), 81L);
    assertEquals(digest.quantile(0.5), 0.1, Math.ulp(0.1));
    TDigest roundTripped = TDigestAccumulator.fromBytes(digest.serialize());
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
    TDigest digest = TDigestAccumulator.fromBytes(compatible);
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
        TDigest digest = TDigestAccumulator.fromBytes(bytes);
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
        TDigest digest = TDigestAccumulator.fromBytes(compatible);
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
    assertThrows(IllegalArgumentException.class, () -> TDigestAccumulator.fromBytes(healthy));
    means[150] = Double.NaN;
    byte[] historical = verboseBytes(100.0, means, weights);
    TDigestCodec.SerializedTDigestMetadata metadata = TDigestCodec.readSerializedHeader(ByteBuffer.wrap(historical));
    assertTrue(metadata.needsLegacyFallback());
    assertEquals(metadata.totalWeight(), 300.0);
    assertEquals(TDigestCodec.validateSerialized(historical), 300.0);
    TDigest retained = TDigestAccumulator.fromBytes(historical);
    assertFalse(retained.hasValidStatistics());
    assertTrue(Double.isNaN(retained.quantile(0.5)));
    assertEquals(retained.centroidCount(), 300);
    assertEquals(retained.centroids().size(), 300);
    retained.compress();
    assertEquals(retained.serialize(), historical);
    TDigest reread = TDigestAccumulator.fromBytes(retained.serialize());
    assertFalse(reread.hasValidStatistics());
    assertEquals(reread.getTotalWeight(), 300.0);
  }

  private static byte[] verboseBytes(double compression, double[] means, double[] weights) {
    return legacyBytes(VERBOSE_ENCODING, compression, means[0], means[means.length - 1], means, weights);
  }

  // Write the legacy wire layout directly so decoder fixtures are independent of the production serializer.
  private static byte[] legacyBytes(int encoding, double compression, double min, double max, double[] means,
      double[] weights) {
    ByteBuffer buffer = ByteBuffer.allocate(encoding == VERBOSE_ENCODING ? 32 + 16 * means.length
        : 30 + 8 * means.length);
    buffer.putInt(encoding).putDouble(min).putDouble(max);
    if (encoding == VERBOSE_ENCODING) {
      buffer.putDouble(compression).putInt(means.length);
      for (int i = 0; i < means.length; i++) {
        buffer.putDouble(weights[i]).putDouble(means[i]);
      }
    } else {
      buffer.putFloat((float) compression).putShort((short) 210).putShort((short) 1050)
          .putShort((short) means.length);
      for (int i = 0; i < means.length; i++) {
        buffer.putFloat((float) weights[i]).putFloat((float) means[i]);
      }
    }
    return buffer.array();
  }
}
