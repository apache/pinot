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
import java.util.List;
import java.util.SplittableRandom;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.local.utils.TDigestUtils;
import org.apache.pinot.segment.spi.customobject.TDigest;
import org.apache.pinot.segment.spi.customobject.TDigest.Centroid;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
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
        {100, 210, 3_392},
        {1_000, 2_010, 32_032}
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
    TDigest source = TDigestUtils.createMergingDigest(100);
    source.add(2.0);
    source.add(3.0);

    aggregator.applyAggregatedValue(destination, source);

    assertEquals(destination.size(), 3L);
    int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
    byte[] serialized = aggregator.serializeAggregatedValue(destination);
    assertTrue(serialized.length <= maxByteSize);
  }

  @Test
  public void testCopyAndParentMergeDoNotRecompressSource() {
    SplittableRandom random = new SplittableRandom(10);
    PercentileTDigestValueAggregator aggregator = newAggregator(100);
    TDigest source = aggregator.getInitialAggregatedValue(random.nextDouble() * 10_000);
    for (int i = 1; i < 94; i++) {
      aggregator.applyRawValue(source, random.nextDouble() * 10_000);
    }
    // Query the working-compression state before any serialization; serializing first hides source recompression.
    double beforeCopy = source.quantile(0.86);
    aggregator.cloneAggregatedValue(source);
    assertEquals(source.quantile(0.86), beforeCopy);
    TDigest parent = aggregator.getInitialAggregatedValue(0.0);
    aggregator.applyAggregatedValue(parent, source);
    assertEquals(source.quantile(0.86), beforeCopy);
    TDigest queryAccumulator = TDigestUtils.createMergingDigest(100.0);
    queryAccumulator.add(source);
    assertEquals(source.quantile(0.86), beforeCopy,
        "Generic centroid ingestion must not perform the source's final compression");
  }

  @Test
  public void testCompactFiniteMeansRestoreBoundsBeforeAddingInfiniteTails() {
    for (double[] values : new double[][]{
        {0.1, 0.3, Double.NEGATIVE_INFINITY}, {0.7, 0.9, Double.POSITIVE_INFINITY}
    }) {
      double min = values[0];
      double max = values[1];
      ByteBuffer compact = ByteBuffer.allocate(46);
      compact.putInt(TDigestUtils.SMALL_ENCODING).putDouble(min).putDouble(max).putFloat(100.0F);
      compact.putShort((short) 210).putShort((short) 1050).putShort((short) 2);
      compact.putFloat(1.0F).putFloat((float) min).putFloat(1.0F).putFloat((float) max);
      PercentileTDigestValueAggregator aggregator = newAggregator(100);
      TDigest result = aggregator.getInitialAggregatedValue(compact.array());
      aggregator.applyRawValue(result, values[2]);
      int registeredBound = aggregator.getMaxAggregatedValueByteSize();
      byte[] serialized = aggregator.serializeAggregatedValue(result);
      assertTrue(serialized.length <= registeredBound);
      ByteBuffer verbose = ByteBuffer.wrap(serialized);
      assertEquals(verbose.getInt(), TDigestUtils.VERBOSE_ENCODING);
      int count = verbose.getInt(28);
      verbose.position(TDigestUtils.VERBOSE_HEADER_SIZE);
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

    byte[] compatible = TDigestUtils.makeLegacyCompatible(createVerboseEncoding(means, weights, 20.0));
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
  public void testCentroidInspectionDoesNotInvalidateCachedSerialization() {
    PercentileTDigestValueAggregator aggregator = newAggregator(20);
    TDigest digest = aggregator.getInitialAggregatedValue(
        new Object[]{Double.NEGATIVE_INFINITY, 0.0, 1.0, 2.0, Double.POSITIVE_INFINITY});
    byte[] beforeInspection = aggregator.serializeAggregatedValue(digest);

    digest.compress();
    assertTrue(digest.centroidCount() > 0);
    assertCentroidWeight(digest, digest.size());

    assertEquals(aggregator.serializeAggregatedValue(digest), beforeInspection);
  }

  @Test
  public void testFractionalCentroidsWithInfiniteTailsPreserveWireMass() {
    TDigest source = TDigestUtils.deserialize(createVerboseEncoding(
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
  public void testFractionalMassDoesNotUnderestimateRegisteredByteSize() {
    for (double secondWeight : new double[]{0.5, 1.5}) {
      PercentileTDigestValueAggregator aggregator = newAggregator(100);
      TDigest result = aggregator.getInitialAggregatedValue(createVerboseEncoding(
          new double[]{0.0}, new double[]{0.5}));
      aggregator.applyRawValue(result, createVerboseEncoding(new double[]{2.0}, new double[]{secondWeight}));

      int registeredBound = aggregator.getMaxAggregatedValueByteSize();
      byte[] serialized = aggregator.serializeAggregatedValue(result);
      assertTrue(serialized.length <= registeredBound);
      assertEquals(aggregator.deserializeAggregatedValue(serialized).getTotalWeight(), 0.5 + secondWeight);
    }
  }

  @Test
  public void testDegradedNonPositiveMassSurvivesCopyAndBothMergeOrders() {
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
        TDigest merged = poisonedFirst ? aggregator.applyAggregatedValue(degraded, valid)
            : aggregator.applyAggregatedValue(valid, degraded);
        assertFalse(merged.hasValidStatistics());
        assertTrue(Double.isNaN(merged.quantile(0.5)));
        assertTrue(Double.isNaN(aggregator.deserializeAggregatedValue(
            aggregator.serializeAggregatedValue(merged)).quantile(0.5)));
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
    verify(source, never()).byteSize();
    verify(source, never()).asBytes(any(ByteBuffer.class));
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
    assertEquals(TDigestUtils.validateSerialized(TDigestUtils.serialize(digest)), (double) expectedWeight);
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
