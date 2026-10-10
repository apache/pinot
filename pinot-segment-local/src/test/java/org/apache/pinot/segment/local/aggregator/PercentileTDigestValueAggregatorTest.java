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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.customobject.tdigest.PercentileTDigestAccumulator;
import org.apache.pinot.segment.local.customobject.tdigest.SerializedTDigest;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest.Centroid;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestCodec;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/// Tests the serialization-size contract and deferred compression used by [PercentileTDigestValueAggregator].
public class PercentileTDigestValueAggregatorTest {

  @Test(dataProvider = "compressionBounds")
  public void testRawValuesTrackMaxVerboseSize(int compression, int numValues,
      int expectedMaxByteSize) {
    PercentileTDigestValueAggregator aggregator = newAggregator(compression);
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), 0);

    TDigest digest = aggregator.getInitialAggregatedValue(0.0);
    for (int i = 1; i < numValues; i++) {
      aggregator.applyRawValue(digest, i);
    }
    int registeredBound = aggregator.getMaxAggregatedValueByteSize();
    assertEquals(registeredBound, expectedMaxByteSize);
    byte[] serialized = aggregator.serializeAggregatedValue(digest);
    assertTrue(serialized.length <= registeredBound);
  }

  @DataProvider
  public static Object[][] compressionBounds() {
    return new Object[][]{
        {10, 30, 512},
        {100, 1_049, 3_392},
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
    int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
    assertEquals(maxByteSize, 3_392);
    byte[] serialized = aggregator.serializeAggregatedValue(digest);
    assertTrue(digest.centroidCount() > 0);
    assertTrue(serialized.length <= maxByteSize);
  }

  @Test
  public void testPreAggregatedCompressionExpandsRegisteredBound() {
    byte[] inputBytes = createSmallEncoding(200, 410, 2050, 100);

    PercentileTDigestValueAggregator aggregator = newAggregator(10);
    TDigest result = aggregator.getInitialAggregatedValue(inputBytes);
    for (int i = 100; i < 410; i++) {
      aggregator.applyRawValue(result, i);
    }

    assertEquals(result.compression(), 200.0);
    int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
    assertEquals(maxByteSize, 6_592);
    byte[] serialized = aggregator.serializeAggregatedValue(result);
    assertTrue(serialized.length > 512, "The larger-compression input must exceed a compression-10 buffer");
    assertTrue(serialized.length <= maxByteSize);

    double[] means = new double[5_000];
    double[] weights = new double[means.length];
    Arrays.fill(weights, 2.0);
    weights[0] = 1.0;
    weights[weights.length - 1] = 1.0;
    for (int i = 0; i < 5_000; i++) {
      means[i] = i + 1.0;
    }
    PercentileTDigestValueAggregator sourceAggregator = newAggregator(10_000);
    TDigest highCompression = sourceAggregator.deserializeAggregatedValue(
        createVerboseEncoding(means, weights, 10_000.0));
    sourceAggregator.applyRawValue(highCompression, 2_500.0);
    byte[] highCompressionBytes = highCompression.serialize();
    PercentileTDigestValueAggregator lowerCompression = newAggregator(100);
    TDigest merged = lowerCompression.getInitialAggregatedValue(new Object[0]);
    lowerCompression.applyRawValue(merged, highCompressionBytes);
    byte[] rewritten = lowerCompression.serializeAggregatedValue(merged);
    TDigest restored = lowerCompression.deserializeAggregatedValue(rewritten);
    assertTrue(restored.hasValidStatistics());
    assertEquals(restored.getTotalWeight(), 9_999.0);
    assertEquals(restored.getMin(), 1.0);
    assertEquals(restored.getMax(), 5_000.0);
    assertEquals(restored.quantile(0.0), 1.0);
    assertEquals(restored.quantile(1.0), 5_000.0);
    assertEquals(restored.quantile(0.198), 990.901, 1.0);
  }

  @Test
  public void testFractionalPreAggregatedCompressionUsesLegacyCompatibleBound() {
    int centroidCount = 96;
    double[] means = new double[centroidCount];
    double[] weights = new double[centroidCount];
    for (int i = 0; i < centroidCount; i++) {
      means[i] = i;
      weights[i] = 1.0;
    }

    PercentileTDigestValueAggregator aggregator = newAggregator(10);
    TDigest result = aggregator.getInitialAggregatedValue(createVerboseEncoding(means, weights, 42.125));

    assertEquals(result.size(), centroidCount);
    assertEquals(result.compression(), 42.125);
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), 32 + 16 * centroidCount);
    assertTrue(aggregator.serializeAggregatedValue(result).length <= aggregator.getMaxAggregatedValueByteSize());
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

    // Unchanged compact bytes remain byte-exact and reserve their original size.
    assertEquals(maxByteSize, smallEncoding.length);
    assertEquals(serialized, smallEncoding);
    assertTrue(serialized.length <= maxByteSize);
    assertEquals(clone.size(), result.size());
    assertEquals(clone.getMin(), result.getMin());
    assertEquals(clone.getMax(), result.getMax());
    assertTrue(Double.isFinite(clone.quantile(0.75)));
  }

  @Test
  public void testAggregatedUpdateDoesNotForceDestinationCompression() {
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest destination = aggregator.getInitialAggregatedValue(1.0);
    TDigest source = PercentileTDigestAccumulator.forLegacyAggregation(100);
    source.add(2.0);
    source.add(3.0);

    aggregator.applyAggregatedValue(destination, source);

    assertEquals(destination.size(), 3L);
    int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
    byte[] serialized = aggregator.serializeAggregatedValue(destination);
    assertTrue(serialized.length <= maxByteSize);
  }

  @Test
  public void testConsumingMergeKeepsSourceMassAndDerivedCachesConsistent() {
    SplittableRandom random = new SplittableRandom(10);
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest source = aggregator.getInitialAggregatedValue(random.nextDouble() * 10_000);
    for (int i = 1; i < 94; i++) {
      aggregator.applyRawValue(source, random.nextDouble() * 10_000);
    }
    source.centroids();
    source.quantile(0.86);
    TDigest copy = aggregator.cloneAggregatedValue(source);
    TDigest parent = aggregator.getInitialAggregatedValue(0.0);
    aggregator.applyAggregatedValue(parent, source);
    TDigest queryAccumulator = PercentileTDigestAccumulator.forLegacyAggregation(100.0);
    queryAccumulator.add(source);

    assertEquals(source.getTotalWeight(), 94.0);
    assertEquals(copy.getTotalWeight(), 94.0);
    assertEquals(parent.getTotalWeight(), 95.0);
    TDigest roundTripped = aggregator.deserializeAggregatedValue(aggregator.serializeAggregatedValue(source));
    assertEquals(source.centroids().stream().mapToDouble(Centroid::weight).sum(), 94.0);
    assertEquals(roundTripped.getTotalWeight(), 94.0);
    assertEquals(source.quantile(0.86), roundTripped.quantile(0.86));
  }

  @Test
  public void testCompactFiniteMeansRestoreBoundsBeforeAddingInfiniteTails() {
    for (double[] values : new double[][]{
        {0.1, 0.3, Double.NEGATIVE_INFINITY}, {0.7, 0.9, Double.POSITIVE_INFINITY}
    }) {
      double min = values[0];
      double max = values[1];
      ByteBuffer compact = ByteBuffer.allocate(46);
      compact.putInt(TDigestCodec.SMALL_ENCODING).putDouble(min).putDouble(max).putFloat(100.0F);
      compact.putShort((short) 210).putShort((short) 1050).putShort((short) 2);
      compact.putFloat(1.0F).putFloat((float) min).putFloat(1.0F).putFloat((float) max);
      PercentileTDigestValueAggregator aggregator = newAggregator(100);
      TDigest result = aggregator.getInitialAggregatedValue(compact.array());
      aggregator.applyRawValue(result, values[2]);
      int registeredBound = aggregator.getMaxAggregatedValueByteSize();
      byte[] serialized = aggregator.serializeAggregatedValue(result);
      assertTrue(serialized.length <= registeredBound);
      ByteBuffer verbose = ByteBuffer.wrap(serialized);
      assertEquals(verbose.getInt(), TDigestCodec.VERBOSE_ENCODING);
      int count = verbose.getInt(28);
      verbose.position(TDigestCodec.VERBOSE_HEADER_SIZE);
      int finiteCount = 0;
      for (int i = 0; i < count; i++) {
        verbose.getDouble();
        double mean = verbose.getDouble();
        if (Double.isFinite(mean)) {
          double endpoint = finiteCount++ == 0 ? min : max;
          assertEquals(mean, Math.max(min, Math.min((double) (float) endpoint, max)));
        }
      }
      assertEquals(finiteCount, 2);
      TDigest roundTripped = aggregator.deserializeAggregatedValue(serialized);
      assertEquals(roundTripped.getTotalWeight(), 3.0);
      assertEquals(roundTripped.quantile(values[2] < 0.0 ? 1.0 : 0.0), values[2] < 0.0 ? max : min);
    }
  }

  @Test
  public void testWeightedFiniteBoundariesAreNormalizedBeforeAddingInfiniteTails() {
    for (boolean historical : new boolean[]{false, true}) {
      for (double[] tails : new double[][]{
          {Double.NEGATIVE_INFINITY}, {Double.POSITIVE_INFINITY},
          {Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}
      }) {
        PercentileTDigestValueAggregator aggregator = newAggregator(100);
        TDigest digest;
        if (historical) {
          digest = aggregator.getInitialAggregatedValue(createVerboseEncoding(
              new double[]{0.0, 10.0}, new double[]{7.0, 5.0}));
        } else {
          digest = aggregator.getInitialAggregatedValue(0.0);
          digest.add(0.0, 6.0);
          digest.add(10.0, 5.0);
        }
        for (double tail : tails) {
          digest.add(tail, 3.0);
        }
        int bound = digest.maxSerializedByteSize();
        List<Centroid> finiteCentroids = digest.centroids().stream()
            .filter(centroid -> Double.isFinite(centroid.mean())).toList();
        assertEquals(finiteCentroids.getFirst().weight(), 1.0);
        assertEquals(finiteCentroids.getLast().weight(), 1.0);
        assertEquals(finiteCentroids.stream().mapToDouble(Centroid::weight).sum(), 12.0);

        byte[] serialized = aggregator.serializeAggregatedValue(digest);
        assertTrue(serialized.length <= bound);
        assertFalse(TDigestCodec.inspectSerialized(ByteBuffer.wrap(serialized)).weightedBoundaries());
        TDigest roundTripped = aggregator.deserializeAggregatedValue(serialized);
        assertEquals(roundTripped.getTotalWeight(), 12.0 + 3.0 * tails.length);
        assertEquals(roundTripped.getMin(), digest.getMin());
        assertEquals(roundTripped.getMax(), digest.getMax());
        assertEquals(aggregator.serializeAggregatedValue(digest), serialized);
      }
    }
  }

  @Test
  public void testFractionalSingletonWithUnitInfinityTailHasUnitWholeBoundaries() {
    for (boolean historical : new boolean[]{false, true}) {
      for (boolean positiveTail : new boolean[]{false, true}) {
        PercentileTDigestValueAggregator aggregator = newAggregator(100);
        TDigest finite = historical
            ? aggregator.deserializeAggregatedValue(createVerboseEncoding(new double[]{0.0}, new double[]{1.5}))
            : PercentileTDigestAccumulator.forLegacyAggregation(100);
        if (!historical) {
          finite.add(0.0, 1.5);
        }
        TDigest result = aggregator.getInitialAggregatedValue(
            positiveTail ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY);
        aggregator.applyAggregatedValue(result, finite);
        int bound = result.maxSerializedByteSize();
        List<Centroid> centroids = new ArrayList<>(result.centroids());
        assertEquals(centroids.size(), 3);
        assertEquals(centroids.getFirst().weight(), 1.0);
        assertEquals(centroids.getLast().weight(), 1.0);
        byte[] serialized = aggregator.serializeAggregatedValue(result);
        assertTrue(serialized.length <= bound);
        assertFalse(TDigestCodec.inspectSerialized(ByteBuffer.wrap(serialized)).weightedBoundaries());
        assertEquals(aggregator.deserializeAggregatedValue(serialized).centroidCount(), centroids.size());
        assertEquals(TDigestCodec.validateSerialized(serialized), 2.5);
        assertEquals(result.cdf(0.0), positiveTail ? 0.3 : 0.7, 1e-12);
        assertEquals(aggregator.deserializeAggregatedValue(serialized).cdf(0.0), result.cdf(0.0));
      }
    }
  }

  @Test
  public void testRepeatedInfinitiesAcrossRawAggregationAndSerialization() {
    int repetitions = 1_000;
    Object[] values = new Object[3 * repetitions];
    for (int i = 0; i < repetitions; i++) {
      values[3 * i] = Double.NEGATIVE_INFINITY;
      values[3 * i + 1] = 0.0;
      values[3 * i + 2] = Double.POSITIVE_INFINITY;
    }

    PercentileTDigestValueAggregator aggregator = newAggregator(20);
    TDigest digest = aggregator.getInitialAggregatedValue(values);
    TDigest aggregatedValue = aggregator.cloneAggregatedValue(digest);
    digest = aggregator.applyAggregatedValue(digest, aggregatedValue);

    long expectedSize = 2L * values.length;
    assertInfinityDistribution(digest, expectedSize);
    byte[] serialized = aggregator.serializeAggregatedValue(digest);
    TDigest standardDigest = CustomSerDeUtils.TDIGEST_SER_DE.deserialize(serialized);
    assertEquals(standardDigest.size(), expectedSize);
    assertEquals(standardDigest.getMin(), Double.NEGATIVE_INFINITY);
    assertEquals(standardDigest.getMax(), Double.POSITIVE_INFINITY);
    TDigest roundTripped = aggregator.deserializeAggregatedValue(serialized);
    assertInfinityDistribution(roundTripped, expectedSize);

    for (double[] means : new double[][]{
        {5.0, Double.POSITIVE_INFINITY}, {Double.NEGATIVE_INFINITY, -5.0},
        {Double.NEGATIVE_INFINITY, 5.0}, {-5.0, Double.POSITIVE_INFINITY},
        {Double.NEGATIVE_INFINITY, 0.0, Double.POSITIVE_INFINITY}
    }) {
      double[] weights = new double[means.length];
      Arrays.fill(weights, 1.0);
      byte[] input = createVerboseEncoding(means, weights);
      TDigest nativeDigest = PercentileTDigestAccumulator.fromBytes(input);
      TDigest wrapped = aggregator.deserializeAggregatedValue(input);
      TDigest raw = aggregator.getInitialAggregatedValue(Arrays.stream(means).boxed().toArray());
      for (double quantile : new double[]{
          0.0, Math.nextDown(0.5), 0.5, Math.nextUp(0.5), 1.0 / 3.0, 2.0 / 3.0, 1.0
      }) {
        assertEquals(wrapped.quantile(quantile), nativeDigest.quantile(quantile));
        assertEquals(raw.quantile(quantile), nativeDigest.quantile(quantile));
      }
    }
  }

  @Test
  public void testNonFiniteQuantileRejectsNaN() {
    PercentileTDigestValueAggregator aggregator = newAggregator(20);
    TDigest digest = aggregator.getInitialAggregatedValue(
        new Object[]{Double.NEGATIVE_INFINITY, 0.0, Double.POSITIVE_INFINITY});

    assertThrows(IllegalArgumentException.class, () -> digest.quantile(Double.NaN));
  }

  @Test
  public void testCompatibilityReductionPreservesFiniteExtremaBetweenInfiniteTails() {
    int finiteCount = 50;
    double finiteMin = 0.123456789;
    double finiteMax = finiteMin + finiteCount - 1.0;
    double[] means = new double[finiteCount + 2];
    double[] weights = new double[means.length];
    means[0] = Double.NEGATIVE_INFINITY;
    weights[0] = 1.0;
    for (int i = 0; i < finiteCount; i++) {
      means[i + 1] = finiteMin + i;
      weights[i + 1] = 1.0;
    }
    means[means.length - 1] = Double.POSITIVE_INFINITY;
    weights[weights.length - 1] = 1.0;

    byte[] compatible = TDigestCodec.makeLegacyCompatible(createVerboseEncoding(means, weights, 20.0));
    assertEquals(ByteBuffer.wrap(compatible).getInt(), 1);
    PercentileTDigestValueAggregator aggregator = newAggregator(20);
    TDigest digest = aggregator.deserializeAggregatedValue(compatible);
    double firstFiniteQuantile = 1.0 / means.length;
    double lastFiniteQuantile = Math.nextDown((means.length - 1.0) / means.length);
    assertEquals(digest.quantile(firstFiniteQuantile), finiteMin);
    assertEquals(digest.quantile(lastFiniteQuantile), finiteMax);

    TDigest roundTripped = aggregator.deserializeAggregatedValue(aggregator.serializeAggregatedValue(digest));
    assertEquals(roundTripped.quantile(firstFiniteQuantile), finiteMin);
    assertEquals(roundTripped.quantile(lastFiniteQuantile), finiteMax);
  }

  @Test
  public void testLargeFiniteCentroidWeightWithInfinitiesIsNotNarrowed() {
    long finiteWeight = 3_000_000_000L;
    byte[] input = createVerboseEncoding(
        new double[]{Double.NEGATIVE_INFINITY, 1.0e308, Double.POSITIVE_INFINITY},
        new double[]{1.0, finiteWeight, 1.0});

    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest digest = aggregator.deserializeAggregatedValue(input);
    assertEquals(digest.size(), finiteWeight + 2L);
    assertCentroidWeight(digest, finiteWeight + 2L);
    assertHasFiniteCentroid(digest, 1.0e308);

    TDigest clone = aggregator.cloneAggregatedValue(digest);
    assertEquals(clone.size(), finiteWeight + 2L);
    digest = aggregator.applyAggregatedValue(digest, clone);
    assertEquals(digest.size(), 2L * finiteWeight + 4L);
    assertCentroidWeight(digest, 2L * finiteWeight + 4L);
    assertHasFiniteCentroid(digest, 1.0e308);

    byte[] serialized = aggregator.serializeAggregatedValue(digest);
    assertEquals(CustomSerDeUtils.TDIGEST_SER_DE.deserialize(serialized).size(), 2L * finiteWeight + 4L);
    assertEquals(aggregator.deserializeAggregatedValue(serialized).size(), 2L * finiteWeight + 4L);
  }

  @Test
  public void testCentroidInspectionPreservesSerializationStatistics() {
    PercentileTDigestValueAggregator aggregator = newAggregator(20);
    TDigest digest = aggregator.getInitialAggregatedValue(
        new Object[]{Double.NEGATIVE_INFINITY, 0.0, 1.0, 2.0, Double.POSITIVE_INFINITY});
    byte[] beforeInspection = digest.serialize();
    digest.compress();
    assertTrue(digest.centroidCount() > 0);
    assertCentroidWeight(digest, digest.size());
    assertEquals(digest.serialize(), beforeInspection);

    for (byte[] compact : new byte[][]{
        createSmallEncoding(new double[]{1.0, 5.0, 9.0}, new double[]{1.0, 1.0, 1.0}),
        createSmallEncoding(100, 500, 2_500, 500)}) {
      for (int read = 0; read < 7; read++) {
        TDigest source = aggregator.deserializeAggregatedValue(compact);
        double mass = source.getTotalWeight();
        double min = source.getMin();
        double max = source.getMax();
        byte[] returned = source.serialize();
        Arrays.fill(returned, (byte) 0);
        switch (read) {
          case 0 -> source.quantile(0.5);
          case 1 -> source.cdf(5.0);
          case 2 -> source.centroids();
          case 3 -> source.centroidCount();
          case 4 -> source.compress();
          case 5 -> aggregator.cloneAggregatedValue(source);
          case 6 -> aggregator.applyAggregatedValue(aggregator.getInitialAggregatedValue(0.0), source);
          default -> throw new AssertionError();
        }
        assertEquals(source.getTotalWeight(), mass);
        assertEquals(source.getMin(), min);
        assertEquals(source.getMax(), max);
        byte[] serialized = source.serialize();
        assertEquals(TDigestCodec.validateSerialized(serialized), mass);
        assertTrue(serialized.length <= source.maxSerializedByteSize());
        source.add(1_000.0);
        byte[] updated = source.serialize();
        assertEquals(TDigestCodec.validateSerialized(updated), mass + 1.0);
        assertEquals(source.getMax(), 1_000.0);
      }
    }
  }

  @Test(dataProvider = "serializationOwnership")
  public void testSerializedBytesAreIndependentlyOwned(boolean infiniteTail) {
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest digest = aggregator.getInitialAggregatedValue(1.0);
    aggregator.applyRawValue(digest, 2.0);
    if (infiniteTail) {
      aggregator.applyRawValue(digest, Double.POSITIVE_INFINITY);
    }
    byte[] bytes = aggregator.serializeAggregatedValue(digest);
    byte[] expected = bytes.clone();
    double median = digest.quantile(0.5);
    Arrays.fill(bytes, (byte) 0);
    assertEquals(aggregator.serializeAggregatedValue(digest), expected);
    assertEquals(digest.quantile(0.5), median);
  }

  @DataProvider
  public static Object[][] serializationOwnership() {
    return new Object[][]{{false}, {true}};
  }

  @Test
  public void testCompactOverflowWithGenuineInfinityTailRemainsFinite() {
    ByteBuffer compact = ByteBuffer.allocate(TDigestCodec.SMALL_HEADER_SIZE + 3 * TDigestCodec.SMALL_CENTROID_SIZE);
    compact.putInt(TDigestCodec.SMALL_ENCODING).putDouble(Double.NEGATIVE_INFINITY).putDouble(1e39).putFloat(100);
    compact.putShort((short) 210).putShort((short) 1050).putShort((short) 3);
    compact.putFloat(1).putFloat(Float.NEGATIVE_INFINITY);
    compact.putFloat(1).putFloat(0);
    compact.putFloat(1).putFloat(Float.POSITIVE_INFINITY);
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest digest = aggregator.deserializeAggregatedValue(compact.array());
    assertTrue(digest.hasValidStatistics());
    assertEquals(digest.getTotalWeight(), 3.0);
    assertEquals(digest.getMax(), 1e39);
    assertEquals(digest.quantile(0.5), 0.0);
    TDigest restored = aggregator.deserializeAggregatedValue(aggregator.serializeAggregatedValue(digest));
    assertTrue(restored.hasValidStatistics());
    assertEquals(restored.quantile(0.5), 0.0);
    assertEquals(restored.getMax(), 1e39);
  }

  @Test
  public void testFractionalCentroidsWithInfiniteTailsPreserveWireMass() {
    TDigest source = PercentileTDigestAccumulator.fromBytes(createVerboseEncoding(
        new double[]{Double.NEGATIVE_INFINITY, 2.0, Double.POSITIVE_INFINITY},
        new double[]{1.0, 0.5, 1.0}));
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest result = aggregator.getInitialAggregatedValue(0.0);

    aggregator.applyAggregatedValue(result, source);
    assertEquals(result.getTotalWeight(), 3.5);
    TDigest roundTripped = aggregator.deserializeAggregatedValue(aggregator.serializeAggregatedValue(result));
    assertEquals(roundTripped.getTotalWeight(), 3.5);
    assertEquals(roundTripped.quantile(0.0), Double.NEGATIVE_INFINITY);
    assertEquals(roundTripped.quantile(1.0), Double.POSITIVE_INFINITY);

    TDigest weightedFinite = aggregator.deserializeAggregatedValue(createVerboseEncoding(
        new double[]{1.0, 2.0}, new double[]{0.5, 5.0}));
    weightedFinite.add(Double.NEGATIVE_INFINITY, 2.0);
    weightedFinite.add(Double.POSITIVE_INFINITY, 2.0);
    int bound = weightedFinite.maxSerializedByteSize();
    byte[] weightedBytes = aggregator.serializeAggregatedValue(weightedFinite);
    assertTrue(weightedBytes.length <= bound);
    assertFalse(TDigestCodec.inspectSerialized(ByteBuffer.wrap(weightedBytes)).weightedBoundaries());
    assertEquals(aggregator.deserializeAggregatedValue(weightedBytes).getTotalWeight(), 9.5);

    TDigest fractional = aggregator.getInitialAggregatedValue(createVerboseEncoding(
        new double[]{4.0}, new double[]{0.5}));
    assertEquals(aggregator.cloneAggregatedValue(fractional).getTotalWeight(), 0.5);
    assertEquals(fractional.quantile(0.5), 4.0);

    TDigest unordered = aggregator.deserializeAggregatedValue(createVerboseEncoding(
        new double[]{Double.NEGATIVE_INFINITY, 2.0, 1.0, Double.POSITIVE_INFINITY},
        new double[]{1.0, 1.0, 1.0, 1.0}));
    assertEquals(unordered.getTotalWeight(), 4.0);
    assertEquals(unordered.cdf(1.5), 0.5, 1.0e-12);
    assertEquals(aggregator.deserializeAggregatedValue(aggregator.serializeAggregatedValue(unordered)).cdf(1.5),
        0.5, 1.0e-12);
  }

  @Test
  public void testHugeInfinityMassUsesBoundedDoubleWeightEncoding() {
    double hugeWeight = 0x1p63;
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest digest = aggregator.deserializeAggregatedValue(createVerboseEncoding(
        new double[]{Double.NEGATIVE_INFINITY, 0.0, Double.POSITIVE_INFINITY},
        new double[]{hugeWeight, hugeWeight, hugeWeight}));

    assertEquals(digest.size(), Long.MAX_VALUE);
    assertEquals(digest.getTotalWeight(), 3.0 * hugeWeight);
    assertEquals(digest.cdf(0.0), 0.5, 1.0e-12);
    byte[] serialized = aggregator.serializeAggregatedValue(digest);
    assertTrue(serialized.length < 200);
    TDigest roundTripped = aggregator.deserializeAggregatedValue(serialized);
    assertEquals(roundTripped.getTotalWeight(), 3.0 * hugeWeight);
    assertEquals(roundTripped.cdf(0.0), 0.5, 1.0e-12);

    TDigest hugeCompression = aggregator.getInitialAggregatedValue(createVerboseEncoding(
        new double[]{0.0, 1.0}, new double[]{hugeWeight, hugeWeight}, Double.MAX_VALUE));
    aggregator.applyRawValue(hugeCompression, 0.5);
    assertEquals(hugeCompression.getTotalWeight(), 2.0 * hugeWeight);
    assertTrue(aggregator.serializeAggregatedValue(hugeCompression).length < 200);
  }

  @Test
  public void testFreshFractionalBoundaryFailsBeforeWritingLegacyBytes() {
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest result = PercentileTDigestAccumulator.forLegacyAggregation(100);
    result.add(0.0, 0.5);
    result.add(2.0, 1.5);
    assertThrows(IllegalArgumentException.class, () -> aggregator.serializeAggregatedValue(result));
    assertEquals(result.getTotalWeight(), 2.0);
    assertEquals(result.quantile(1.0), 2.0);
  }

  @Test
  public void testInheritedFractionalBoundaryWithOppositeInfinitySurvivesMixedMerges() {
    for (boolean compact : new boolean[]{false, true}) {
      for (boolean positiveTail : new boolean[]{false, true}) {
        for (int sourceKind = 0; sourceKind < 3; sourceKind++) {
          double infinity = positiveTail ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
          double healthyValue = positiveTail ? 1.0 : -1.0;
          double[] means = positiveTail ? new double[]{0.0, 1.0, infinity} : new double[]{infinity, -1.0, 0.0};
          double[] weights = positiveTail ? new double[]{0.5, 1.0, 1.0} : new double[]{1.0, 1.0, 0.5};
          if (sourceKind == 0) {
            int from = positiveTail ? 0 : 1;
            means = Arrays.copyOfRange(means, from, from + 2);
            weights = Arrays.copyOfRange(weights, from, from + 2);
          }
          byte[] input = compact ? createSmallEncoding(means, weights) : createVerboseEncoding(means, weights);
          PercentileTDigestValueAggregator aggregator = newAggregator(100);
          TDigest result;
          if (sourceKind == 2) {
            result = aggregator.getInitialAggregatedValue(healthyValue);
            aggregator.applyAggregatedValue(result, PercentileTDigestAccumulator.fromBytes(input));
          } else {
            result = aggregator.deserializeAggregatedValue(input);
            result.add(sourceKind == 0 ? infinity : healthyValue);
          }
          double expectedWeight = sourceKind == 0 ? 2.5 : 3.5;
          int bound = result.maxSerializedByteSize();
          byte[] serialized = aggregator.serializeAggregatedValue(result);
          assertTrue(serialized.length <= bound);
          assertEquals(ByteBuffer.wrap(serialized).getInt(), TDigestCodec.VERBOSE_ENCODING);
          for (TDigest view : List.of(result, aggregator.cloneAggregatedValue(result),
              aggregator.deserializeAggregatedValue(serialized), PercentileTDigestAccumulator.fromBytes(serialized))) {
            assertTrue(view.hasValidStatistics());
            assertEquals(view.getTotalWeight(), expectedWeight);
            assertEquals(view.getMin(), positiveTail ? 0.0 : infinity);
            assertEquals(view.getMax(), positiveTail ? infinity : 0.0);
            assertTrue(Double.isFinite(view.cdf(healthyValue / 2.0)));
            assertEquals(TDigestCodec.validateSerialized(view.serialize()), expectedWeight);
          }

          TDigest newFractionalEndpoint = aggregator.cloneAggregatedValue(result);
          newFractionalEndpoint.add(positiveTail ? -1.0 : 1.0, 0.5);
          assertThrows(IllegalArgumentException.class,
              () -> aggregator.serializeAggregatedValue(newFractionalEndpoint));
        }
      }
    }
  }

  @Test
  public void testDegradedNonPositiveMassRetainsBytesAndRejectsBothMergeOrders() {
    for (double[] weights : new double[][]{{1.0, -1.0}, {-5.0, 0.0}}) {
      PercentileTDigestValueAggregator aggregator = newAggregator(100);
      byte[] poisonedBytes = createVerboseEncoding(new double[]{1.0, 2.0}, weights);
      TDigest poisoned = aggregator.getInitialAggregatedValue(poisonedBytes);
      assertFalse(poisoned.hasValidStatistics());
      assertFalse(poisoned.isEmpty());
      TDigest copied = aggregator.cloneAggregatedValue(poisoned);
      assertFalse(copied.hasValidStatistics());
      assertTrue(Double.isNaN(copied.quantile(0.5)));
      assertTrue(Double.isNaN(copied.cdf(1.5)));
      assertFalse(aggregator.deserializeAggregatedValue(
          aggregator.serializeAggregatedValue(copied)).hasValidStatistics());

      for (boolean poisonedFirst : new boolean[]{true, false}) {
        TDigest valid = aggregator.getInitialAggregatedValue(42.0);
        TDigest degraded = aggregator.deserializeAggregatedValue(poisonedBytes);
        assertThrows(IllegalArgumentException.class, () -> {
          if (poisonedFirst) {
            aggregator.applyAggregatedValue(degraded, valid);
          } else {
            aggregator.applyAggregatedValue(valid, degraded);
          }
        });
        assertEquals(valid.getTotalWeight(), 1.0);
        assertEquals(valid.quantile(0.5), 42.0);
        assertEquals(aggregator.serializeAggregatedValue(degraded), poisonedBytes);
      }
    }
  }

  @Test
  public void testGenericInfinityMergeUsesPreciseCentroidsWithoutSerializingSource() {
    double largeWeight = 3_000_000_000.0;
    TDigest source = mock(TDigest.class);
    when(source.hasValidStatistics()).thenReturn(true);
    when(source.getTotalWeight()).thenReturn(largeWeight + 2.5);
    when(source.getMin()).thenReturn(Double.NEGATIVE_INFINITY);
    when(source.getMax()).thenReturn(Double.POSITIVE_INFINITY);
    when(source.compression()).thenReturn(100.0);
    when(source.centroids()).thenReturn(List.of(
        new Centroid(Double.NEGATIVE_INFINITY, 1.0), new Centroid(42.0, 0.5),
        new Centroid(42.0, largeWeight), new Centroid(Double.POSITIVE_INFINITY, 1.0)));
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest result = aggregator.getInitialAggregatedValue(42.0);

    aggregator.applyAggregatedValue(result, source);

    assertEquals(result.getTotalWeight(), largeWeight + 3.5);
    assertEquals(result.centroids().stream().mapToDouble(Centroid::weight).sum(), largeWeight + 3.5);
    assertEquals(result.quantile(0.5), 42.0);
    TDigest roundTripped = aggregator.deserializeAggregatedValue(aggregator.serializeAggregatedValue(result));
    assertEquals(roundTripped.getTotalWeight(), largeWeight + 3.5);
    verify(source, never()).serialize();
  }

  @Test
  public void testDegradedCompactPayloadRejectsInfinityMutationAndRetainsBytes() {
    int centroidCount = 100;
    byte[] compact = createSmallEncoding(10, centroidCount, 500, centroidCount);
    ByteBuffer.wrap(compact).putFloat(TDigestCodec.SMALL_HEADER_SIZE, -1.0F);
    PercentileTDigestValueAggregator aggregator = newAggregator(10);
    TDigest digest = aggregator.getInitialAggregatedValue(compact);
    assertThrows(IllegalArgumentException.class, () -> aggregator.applyRawValue(digest, Double.NEGATIVE_INFINITY));
    assertEquals(aggregator.serializeAggregatedValue(digest), compact);
  }

  @Test
  public void testInfinityOnlyReceiverRejectsDegradedCompactPayloadWithoutMutation() {
    int centroidCount = 100;
    byte[] compact = createSmallEncoding(10, centroidCount, 500, centroidCount);
    ByteBuffer.wrap(compact).putFloat(TDigestCodec.SMALL_HEADER_SIZE, -1.0F);
    PercentileTDigestValueAggregator aggregator = newAggregator(10);
    TDigest result = aggregator.getInitialAggregatedValue(Double.NEGATIVE_INFINITY);
    TDigest source = aggregator.deserializeAggregatedValue(compact);
    byte[] original = aggregator.serializeAggregatedValue(result);

    assertThrows(IllegalArgumentException.class, () -> aggregator.applyAggregatedValue(result, source));

    assertEquals(result.getTotalWeight(), 1.0);
    assertEquals(aggregator.serializeAggregatedValue(result), original);
    assertEquals(aggregator.serializeAggregatedValue(source), compact);
  }

  @Test
  public void testHistoricalFractionalBytesSurviveAllSerializationPathsAndReturnedByteMutation() {
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    for (byte[] original : new byte[][]{
        createVerboseEncoding(new double[]{Double.NEGATIVE_INFINITY, 2.0, Double.POSITIVE_INFINITY},
            new double[]{0.5, 0.3, 0.5}), createVerboseEncoding(new double[]{42.0}, new double[]{1.5})}) {
      TDigest digest = aggregator.deserializeAggregatedValue(original);
      digest.centroids();
      digest.quantile(0.5);
      digest.compress();
      TDigest copy = aggregator.cloneAggregatedValue(digest);
      assertEquals(digest.serialize(), original);
      assertEquals(CustomSerDeUtils.TDIGEST_SER_DE.serialize(digest), original);
      assertEquals(new SerializedTDigest(digest, 50).toString(), BytesUtils.toHexString(original));
      byte[] returned = digest.serialize();
      returned[0] = 0;
      assertEquals(digest.serialize(), original);
      assertEquals(aggregator.serializeAggregatedValue(digest), original);
      assertEquals(copy.serialize(), original);
      digest.add(Double.NEGATIVE_INFINITY, 0.5);
      if (digest.getMax() == Double.POSITIVE_INFINITY) {
        assertEquals(TDigestCodec.validateSerialized(digest.serialize()),
            TDigestCodec.validateSerialized(original) + 0.5);
      } else {
        // This new fractional infinity endpoint has no historical provenance.
        assertThrows(IllegalArgumentException.class, () -> digest.serialize());
      }
      assertEquals(digest.getTotalWeight(), TDigestCodec.validateSerialized(original) + 0.5);
    }

    TDigest fresh = aggregator.deserializeAggregatedValue(createVerboseEncoding(
        new double[]{Double.NEGATIVE_INFINITY, 1.0, 2.0, Double.POSITIVE_INFINITY},
        new double[]{1.0, 0.3, 0.3, 1.0}));
    // Mutating removes original-byte provenance. Unit outer tails still make fractional interior mass readable.
    fresh.add(1.5, 0.1);
    TDigest roundTripped = aggregator.deserializeAggregatedValue(aggregator.serializeAggregatedValue(fresh));
    assertEquals(roundTripped.getTotalWeight(), 2.7, 1e-12);
    assertEquals(roundTripped.quantile(0.0), Double.NEGATIVE_INFINITY);
    assertEquals(roundTripped.quantile(1.0), Double.POSITIVE_INFINITY);
  }

  @Test
  public void testPureHistoricalFractionalInfinityProvenanceSurvivesMergeAndCopy() {
    for (boolean compact : new boolean[]{false, true}) {
      for (double infinity : new double[]{Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
        double[] means = {infinity};
        double[] weights = {0.5};
        byte[] original = compact ? createSmallEncoding(means, weights) : createVerboseEncoding(means, weights);
        PercentileTDigestValueAggregator aggregator = newAggregator(100);
        for (boolean historicalFirst : new boolean[]{false, true}) {
          TDigest historical = aggregator.deserializeAggregatedValue(original);
          TDigest healthy = aggregator.getInitialAggregatedValue(0.0);
          TDigest result = aggregator.applyAggregatedValue(historicalFirst ? historical : healthy,
              historicalFirst ? healthy : historical);
          assertEquals(TDigestCodec.validateSerialized(result.serialize()), 1.5);
          assertEquals(TDigestCodec.validateSerialized(aggregator.serializeAggregatedValue(
              aggregator.cloneAggregatedValue(result))), 1.5);
          assertEquals(result.cdf(0.0), infinity < 0.0 ? 2.0 / 3.0 : 1.0 / 3.0);
        }
        TDigest pureInfinity = aggregator.deserializeAggregatedValue(original);
        pureInfinity.add(infinity);
        TDigest copied = aggregator.cloneAggregatedValue(pureInfinity);
        byte[] serialized = aggregator.serializeAggregatedValue(copied);
        assertEquals(TDigestCodec.validateSerialized(serialized), 1.5);
        assertEquals(copied.quantile(0.5), infinity);
        assertEquals(aggregator.serializeAggregatedValue(pureInfinity), serialized);
      }
    }
  }

  @Test
  public void testRetainedFractionalInfinityBytesSetExactSizeBound() {
    for (boolean compact : new boolean[]{false, true}) {
      for (double infinity : new double[]{Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
        double[] means = new double[10];
        double[] weights = new double[means.length];
        Arrays.fill(means, infinity);
        Arrays.fill(weights, 0.5);
        byte[] original = compact ? createSmallEncoding(means, weights) : createVerboseEncoding(means, weights);
        TDigest digest = newAggregator(100).deserializeAggregatedValue(original);
        assertEquals(digest.maxSerializedByteSize(), original.length);
        byte[] encoded = digest.serialize();
        assertEquals(encoded, original);
        assertTrue(encoded.length <= digest.maxSerializedByteSize());
      }
    }
  }

  @Test
  public void testSerializationRegistersPreviouslyUnobservedDigestBound() {
    PercentileTDigestValueAggregator aggregator = newAggregator(10);
    TDigest digest = PercentileTDigestAccumulator.forLegacyAggregation(500.0);
    for (int i = 0; i < 500; i++) {
      digest.add(i);
    }
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), 0);

    byte[] serialized = aggregator.serializeAggregatedValue(digest);

    assertTrue(serialized.length > 512);
    assertTrue(serialized.length <= aggregator.getMaxAggregatedValueByteSize());
    assertEquals(aggregator.deserializeAggregatedValue(serialized).getTotalWeight(), 500.0);
  }

  @Test
  public void testGenericInfinityMergeAcceptsWorkingCentroidsAboveDefaultCapacity() {
    int finiteCount = TDigestCodec.getDefaultCentroidCapacity(100.0) + 20;
    List<Centroid> centroids = new ArrayList<>(finiteCount + 2);
    centroids.add(new Centroid(Double.NEGATIVE_INFINITY, 1.0));
    for (int i = 0; i < finiteCount; i++) {
      centroids.add(new Centroid(i, 1.0));
    }
    centroids.add(new Centroid(Double.POSITIVE_INFINITY, 1.0));
    TDigest source = mock(TDigest.class);
    when(source.hasValidStatistics()).thenReturn(true);
    when(source.getTotalWeight()).thenReturn(finiteCount + 2.0);
    when(source.getMin()).thenReturn(Double.NEGATIVE_INFINITY);
    when(source.getMax()).thenReturn(Double.POSITIVE_INFINITY);
    when(source.compression()).thenReturn(100.0);
    when(source.centroids()).thenReturn(centroids);
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest result = aggregator.getInitialAggregatedValue(0.0);

    aggregator.applyAggregatedValue(result, source);

    assertEquals(result.getTotalWeight(), finiteCount + 3.0);
    assertEquals(result.centroids().stream().mapToDouble(Centroid::weight).sum(), finiteCount + 3.0);
    int registeredBound = aggregator.getMaxAggregatedValueByteSize();
    assertTrue(registeredBound <= 3_392);
    byte[] serialized = aggregator.serializeAggregatedValue(result);
    assertTrue(serialized.length <= registeredBound);
    assertEquals(aggregator.deserializeAggregatedValue(serialized).getTotalWeight(), finiteCount + 3.0);
    verify(source, never()).compress();
    verify(source, never()).serialize();
  }

  private static void assertInfinityDistribution(TDigest digest, long expectedSize) {
    assertEquals(digest.size(), expectedSize);
    assertEquals(digest.getMin(), Double.NEGATIVE_INFINITY);
    assertEquals(digest.getMax(), Double.POSITIVE_INFINITY);
    assertEquals(digest.quantile(0.0), Double.NEGATIVE_INFINITY);
    assertEquals(digest.quantile(0.5), 0.0);
    assertEquals(digest.quantile(1.0), Double.POSITIVE_INFINITY);
    assertEquals(digest.cdf(-1.0), 1.0 / 3.0, 1.0e-12);
    assertEquals(digest.cdf(0.0), 0.5, 1.0e-12);
    assertEquals(digest.cdf(1.0), 2.0 / 3.0, 1.0e-12);

    double centroidWeight = 0.0;
    double previousMean = Double.NEGATIVE_INFINITY;
    for (Centroid centroid : digest.centroids()) {
      assertTrue(!Double.isNaN(centroid.mean()));
      assertTrue(centroid.mean() >= previousMean);
      assertTrue(centroid.weight() > 0);
      centroidWeight += centroid.weight();
      previousMean = centroid.mean();
    }
    assertEquals(centroidWeight, (double) expectedSize);
  }

  private static void assertCentroidWeight(TDigest digest, long expectedWeight) {
    assertEquals(digest.centroids().stream().mapToDouble(Centroid::weight).sum(), (double) expectedWeight);
    assertEquals(TDigestCodec.validateSerialized(digest.serialize()), (double) expectedWeight);
  }

  private static void assertHasFiniteCentroid(TDigest digest, double expectedMean) {
    boolean found = false;
    for (Centroid centroid : digest.centroids()) {
      if (Double.isFinite(centroid.mean())) {
        assertEquals(centroid.mean(), expectedMean);
        found = true;
      }
    }
    assertTrue(found);
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

  private static byte[] createSmallEncoding(double[] means, double[] weights) {
    ByteBuffer buffer = ByteBuffer.allocate(TDigestCodec.SMALL_HEADER_SIZE
        + means.length * TDigestCodec.SMALL_CENTROID_SIZE);
    buffer.putInt(TDigestCodec.SMALL_ENCODING).putDouble(means[0]).putDouble(means[means.length - 1]).putFloat(100);
    buffer.putShort((short) 210).putShort((short) 1050).putShort((short) means.length);
    for (int i = 0; i < means.length; i++) {
      buffer.putFloat((float) weights[i]).putFloat((float) means[i]);
    }
    return buffer.array();
  }

  private static byte[] createVerboseEncoding(double[] means, double[] weights) {
    return createVerboseEncoding(means, weights, 100.0);
  }

  private static byte[] createVerboseEncoding(double[] means, double[] weights, double compression) {
    ByteBuffer buffer = ByteBuffer.allocate(32 + means.length * 2 * Double.BYTES);
    buffer.putInt(1);
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
