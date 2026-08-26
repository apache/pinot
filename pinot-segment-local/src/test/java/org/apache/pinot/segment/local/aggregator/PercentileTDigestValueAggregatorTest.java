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

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.CustomSerDeUtils;
import org.apache.pinot.segment.local.utils.TDigestUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
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

    assertEquals(maxByteSize, 832);
    assertEquals(ByteBuffer.wrap(serialized).getInt(), 1);
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
    TDigest source = TDigest.createMergingDigest(100);
    source.add(2.0);
    source.add(3.0);

    aggregator.applyAggregatedValue(destination, source);

    assertEquals(destination.size(), 3L);
    int maxByteSize = aggregator.getMaxAggregatedValueByteSize();
    byte[] serialized = aggregator.serializeAggregatedValue(destination);
    assertTrue(serialized.length <= maxByteSize);
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

    long centroidWeight = 0L;
    double previousMean = Double.NEGATIVE_INFINITY;
    for (Centroid centroid : digest.centroids()) {
      assertTrue(!Double.isNaN(centroid.mean()));
      assertTrue(centroid.mean() >= previousMean);
      assertTrue(centroid.count() > 0);
      centroidWeight += centroid.count();
      previousMean = centroid.mean();
    }
    assertEquals(centroidWeight, expectedSize);
  }

  private static void assertCentroidWeight(TDigest digest, long expectedWeight) {
    long centroidWeight = 0L;
    for (Centroid centroid : digest.centroids()) {
      centroidWeight += centroid.count();
    }
    assertEquals(centroidWeight, expectedWeight);
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
