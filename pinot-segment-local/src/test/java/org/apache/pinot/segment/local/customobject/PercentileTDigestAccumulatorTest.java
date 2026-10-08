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
package org.apache.pinot.segment.local.customobject;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.pinot.segment.local.utils.TDigestUtils;
import org.apache.pinot.segment.spi.customobject.TDigest;
import org.apache.pinot.segment.spi.customobject.TDigest.Centroid;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/// Regression tests for degraded legacy views, source-preserving merges, and primitive weighted sorting.
public class PercentileTDigestAccumulatorTest {
  @Test
  public void testDegradedCentroidViewsMatchUntouchedAndMergedBytes() {
    byte[] poison = verbose(new double[]{1, 2, 3}, new double[]{1, -0.5, 1});
    PercentileTDigestAccumulator digest = fromBytes(poison);
    List<Centroid> original = List.copyOf(digest.centroids());
    assertFalse(digest.hasValidStatistics());
    assertEquals(original, List.of(new Centroid(1, 1), new Centroid(2, -0.5), new Centroid(3, 1)));
    assertEquals(digest.centroidCount(), original.size());
    assertEquals(original.stream().mapToDouble(Centroid::weight).sum(), digest.getTotalWeight());
    digest.compress();
    assertEquals(digest.serialize(), poison, "A read or flush must preserve historical bytes");
    ByteBuffer small = ByteBuffer.allocate(digest.smallByteSize());
    digest.asSmallBytes(small);
    assertEquals(small.array(), poison);

    PercentileTDigestAccumulator merged = PercentileTDigestAccumulator.forReduction(100);
    merged.add(1);
    merged.add(2);
    merged.quantile(0.5);
    merged.add(digest);
    List<Centroid> canonical = List.copyOf(merged.centroids());
    assertEquals(canonical.size(), 1);
    assertEquals(merged.centroidCount(), 1);
    assertTrue(Double.isNaN(canonical.get(0).mean()));
    assertEquals(canonical.get(0).weight(), 3.5);
    assertEquals(canonical.get(0).weight(), merged.getTotalWeight());
    assertEquals(TDigestUtils.deserialize(merged.serialize()).centroids(), canonical);
  }

  @Test
  public void testZeroMassPoisonMergeUsesCanonicalBytesInBothOrders() {
    byte[] poison = verbose(new double[]{1, 2, 3}, new double[]{1, -0.5, 1});
    byte[] zeroMass = verbose(new double[]{1, 3}, new double[]{3, -3});
    PercentileTDigestAccumulator first = fromBytes(poison);
    first.add(fromBytes(zeroMass));
    PercentileTDigestAccumulator reversed = fromBytes(zeroMass);
    reversed.add(fromBytes(poison));
    assertEquals(first.serialize(), reversed.serialize());
    assertEquals(first.centroidCount(), 1);
    assertEquals(first.getTotalWeight(), 1.5);
  }

  @Test
  public void testRawAdditionMarksDegradedStateDirtyEvenWhenMassRoundsAway() {
    byte[] poison = verbose(new double[]{1, 2, 3}, new double[]{1, -1, 1e30});
    PercentileTDigestAccumulator digest = fromBytes(poison);
    digest.add(2);
    assertEquals(digest.getTotalWeight(), 1e30);
    assertEquals(digest.centroidCount(), 1);
    assertEquals(List.copyOf(digest.centroids()), List.of(new Centroid(Double.NaN, 1e30)));
  }

  @Test
  public void testDegradedOverflowLeavesOriginalBytesAndFiniteMassIntact() {
    byte[] poison = verbose(new double[]{1, 2}, new double[]{1e308, -1});
    PercentileTDigestAccumulator healthy = PercentileTDigestAccumulator.forReduction(100);
    healthy.add(1, 1e308);
    TDigest generic = mock(TDigest.class);
    when(generic.hasValidStatistics()).thenReturn(true);
    when(generic.getTotalWeight()).thenReturn(1e308);
    for (int mergeKind = 0; mergeKind < 4; mergeKind++) {
      PercentileTDigestAccumulator digest = fromBytes(poison);
      switch (mergeKind) {
        case 0:
          expectThrows(IllegalArgumentException.class, () -> digest.add(1, 1e308));
          break;
        case 1:
          expectThrows(IllegalArgumentException.class, () -> digest.add(healthy));
          break;
        case 2:
          expectThrows(IllegalArgumentException.class, () -> digest.add(generic));
          break;
        default:
          expectThrows(IllegalArgumentException.class, () -> digest.addSerializedTDigest(poison));
          break;
      }
      assertEquals(digest.getTotalWeight(), 1e308);
      assertEquals(digest.serialize(), poison);
    }

    PercentileTDigestAccumulator negative = fromBytes(verbose(new double[]{1, 2}, new double[]{1, -2}));
    negative.add(1, 0.5);
    assertEquals(negative.getTotalWeight(), -0.5);
    negative.add(1);
    assertEquals(negative.getTotalWeight(), 0.5);
    assertEquals(TDigestUtils.deserialize(negative.serialize()).getTotalWeight(), 0.5);
  }

  @Test
  public void testMergedDegradedBytesCanonicalizeNaNPayloads() {
    byte[] first = verbose(new double[]{Double.NaN}, new double[]{1});
    byte[] second = first.clone();
    ByteBuffer.wrap(first).putLong(4, 0x7ff8000000000001L);
    ByteBuffer.wrap(second).putLong(4, 0x7ff8000000000002L);
    assertEquals(fromBytes(first).serialize(), first, "Untouched historical NaN payload bits remain intact");
    PercentileTDigestAccumulator forward = fromBytes(first);
    forward.addSerializedTDigest(second);
    PercentileTDigestAccumulator reverse = fromBytes(second);
    reverse.addSerializedTDigest(first);
    assertEquals(forward.serialize(), reverse.serialize());
    assertEquals(ByteBuffer.wrap(forward.serialize()).getLong(4), Double.doubleToRawLongBits(Double.NaN));
  }

  @Test
  public void testObjectMergeIgnoresZeroWeightAndItsUnusedMean() {
    TDigest source = mock(TDigest.class);
    when(source.hasValidStatistics()).thenReturn(true);
    when(source.getTotalWeight()).thenReturn(2.0);
    when(source.getMin()).thenReturn(10.0);
    when(source.getMax()).thenReturn(20.0);
    when(source.centroids()).thenReturn(List.of(new Centroid(Double.NaN, 0), new Centroid(-100, 0),
        new Centroid(10, 1), new Centroid(20, 1)));
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(100);
    digest.add(source);
    assertEquals(digest.quantile(0), 10.0);
    assertEquals(digest.quantile(1), 20.0);
    assertEquals(digest.getTotalWeight(), 2.0);
    assertEquals(digest.centroidCount(), 2);

    when(source.getTotalWeight()).thenReturn(0.0);
    when(source.getMin()).thenReturn(-100.0);
    when(source.getMax()).thenReturn(100.0);
    when(source.centroids()).thenReturn(List.of(new Centroid(Double.NaN, 0)));
    digest.add(source);
    assertEquals(digest.quantile(0), 10.0);
    assertEquals(digest.quantile(1), 20.0);
  }

  @Test
  public void testZeroMassBytesHaveConsistentEmptyViewsAndDoNotChangeExtrema() {
    byte[] empty = verbose(new double[]{-100, 100}, new double[]{0, 0});
    PercentileTDigestAccumulator pending = fromBytes(empty);
    for (int phase = 0; phase < 2; phase++) {
      assertTrue(pending.isEmpty());
      assertEquals(pending.getMin(), Double.POSITIVE_INFINITY);
      assertEquals(pending.getMax(), Double.NEGATIVE_INFINITY);
      assertEquals(pending.centroidCount(), 0);
      pending.compress();
    }
    for (boolean direct : new boolean[]{false, true}) {
      PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(100);
      digest.add(10);
      digest.add(20);
      if (direct) {
        PercentileTDigestAccumulator.SerializedTDigestInput input =
            new PercentileTDigestAccumulator.SerializedTDigestInput();
        input.reset(empty);
        digest.addSerializedTDigestDirect(input);
      } else {
        digest.addSerializedTDigest(empty);
      }
      assertEquals(digest.getTotalWeight(), 2.0);
      assertEquals(digest.quantile(0), 10.0);
      assertEquals(digest.quantile(1), 20.0);
    }
  }

  @Test
  public void testSmallEncodingRetainsFieldsThatOverflowFloat() {
    for (double[] centroid : new double[][]{{1e39, 1}, {1, 1e39}, {-1e39, 1}}) {
      PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(100);
      digest.add(centroid[0], centroid[1]);
      ByteBuffer encoded = ByteBuffer.allocate(digest.smallByteSize());
      digest.asSmallBytes(encoded);
      assertEquals(encoded.position(), encoded.capacity());
      assertEquals(encoded.getInt(0), TDigestUtils.VERBOSE_ENCODING);
      TDigest restored = TDigestUtils.deserialize(encoded.array());
      assertTrue(restored.hasValidStatistics());
      assertEquals(restored.getTotalWeight(), centroid[1]);
      assertEquals(restored.getMin(), centroid[0]);
      assertEquals(restored.getMax(), centroid[0]);
    }
  }

  @Test
  public void testHighCompressionRetainsBoundedInputBatches() throws Exception {
    double[] values = new double[100_000];
    for (int i = 0; i < values.length; i++) {
      values[i] = i;
    }
    for (double compression : new double[]{8_000, 20_000}) {
      for (boolean weighted : new boolean[]{false, true}) {
        PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(compression);
        if (weighted) {
          for (double value : values) {
            digest.add(value, 0.5);
          }
        } else {
          digest.add(values, 0, values.length);
        }
        digest.compress();
        assertTrue((int) field(digest, "_mergeCount") <= 1_000, "Large compression must keep batching inputs");
        assertEquals(digest.getTotalWeight(), weighted ? values.length / 2.0 : values.length);
        assertEquals(digest.quantile(0), values[0]);
        assertEquals(digest.quantile(1), values[values.length - 1]);
      }
    }
  }

  @Test
  public void testOverflowDuringFlushRetainsIncomingPairs() throws Exception {
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(100);
    digest.add(10, Double.MAX_VALUE);
    digest.compress();
    digest.add(30, 5e291);
    digest.add(20, 5e291);
    double[] means = ((double[]) field(digest, "_incomingMeans")).clone();
    double[] weights = ((double[]) field(digest, "_incomingWeights")).clone();
    expectThrows(IllegalArgumentException.class, digest::compress);
    assertEquals((int) field(digest, "_numIncomingCentroids"), 2);
    assertEquals((double) field(digest, "_incomingWeight"), 1e292);
    assertEquals(((double[]) field(digest, "_incomingMeans"))[0], means[0]);
    assertEquals(((double[]) field(digest, "_incomingMeans"))[1], means[1]);
    assertEquals(((double[]) field(digest, "_incomingWeights"))[0], weights[0]);
    assertEquals(((double[]) field(digest, "_incomingWeights"))[1], weights[1]);
    assertEquals((double) field(digest, "_totalWeight"), Double.MAX_VALUE);
  }

  private static Object field(PercentileTDigestAccumulator digest, String name) throws Exception {
    Field field = PercentileTDigestAccumulator.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(digest);
  }

  @Test
  public void testUnknownInputRemainsPresentAfterMergingWithHealthyMass() {
    byte[] poison = verbose(new double[]{1, 3}, new double[]{3, -3});
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(100);
    digest.add(10, 1_000_000);
    digest.add(fromBytes(poison));
    assertFalse(digest.hasValidStatistics());
    assertFalse(digest.isEmpty(), "Unknown historical mass must not be discarded as empty");
    assertTrue(Double.isNaN(digest.quantile(0.5)));
    assertEquals(digest.getTotalWeight(), 1_000_000.0);
    PercentileTDigestAccumulator stored = fromBytes(digest.serialize());
    assertFalse(stored.hasValidStatistics());
    assertTrue(Double.isNaN(stored.quantile(0.5)));
    assertEquals(stored.getTotalWeight(), digest.getTotalWeight());
  }

  @Test
  public void testCompactSizeAndWriteRejectTooManyCentroidsConsistently() {
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(1_000_000);
    double[] values = new double[Short.MAX_VALUE + 1];
    for (int i = 0; i < values.length; i++) {
      values[i] = i;
    }
    digest.add(values, 0, values.length);
    IllegalStateException sizeFailure = expectThrows(IllegalStateException.class, digest::smallByteSize);
    IllegalStateException writeFailure = expectThrows(IllegalStateException.class,
        () -> digest.asSmallBytes(ByteBuffer.allocate(TDigestUtils.SMALL_HEADER_SIZE)));
    assertEquals(sizeFailure.getMessage(), writeFailure.getMessage());
    assertEquals(digest.centroidCount(), values.length);
    assertTrue(digest.serialize().length <= digest.maxSerializedByteSize());
  }

  @Test
  public void testPendingMergesReuseReaderWithoutReplacingTheIncomingInput() {
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(100);
    for (double value : new double[]{1, 5, 9}) {
      digest.add(fromBytes(verbose(new double[]{value}, new double[]{1})));
    }
    assertEquals(digest.getTotalWeight(), 3.0);
    assertEquals(digest.getMin(), 1.0);
    assertEquals(digest.getMax(), 9.0);
    assertEquals(digest.quantile(0.5), 5.0);
  }

  @Test
  public void testNativeMergeDoesNotPubliclyCompressItsSource() {
    PercentileTDigestAccumulator source = PercentileTDigestAccumulator.forLegacyAggregation(100);
    SplittableRandom random = new SplittableRandom(10);
    for (int i = 0; i < 94; i++) {
      source.add(random.nextDouble() * 10_000);
    }
    double expected = source.quantile(0.86);
    List<Centroid> centroids = List.copyOf(source.centroids());
    PercentileTDigestAccumulator parent = PercentileTDigestAccumulator.forLegacyAggregation(100);
    parent.add(source);
    assertEquals(source.quantile(0.86), expected);
    assertEquals(List.copyOf(source.centroids()), centroids);
  }

  @Test
  public void testDuplicatePlateauKeepsItsFullMassAndCdfAgreesAtItsEdges() {
    PercentileTDigestAccumulator digest = fromBytes(verbose(new double[]{0, 10, 10, 20, 30},
        new double[]{1, 2, 6, 2, 1}));
    for (double index : new double[]{1.25, 4, 8.75}) {
      assertEquals(digest.quantile(index / 12.0), 10.0);
    }
    assertEquals(digest.cdf(10), 5.0 / 12.0);
    assertEquals(digest.cdf(5), 1.0 / 12.0);
    assertEquals(digest.cdf(15), 9.5 / 12.0);
    assertEquals(digest.quantile(digest.cdf(15)), 15.0);

    PercentileTDigestAccumulator continuous = fromBytes(verbose(new double[]{0, 10, 20, 30},
        new double[]{1, 8, 2, 1}));
    assertTrue(continuous.quantile(4.0 / 12.0) < 10.0);
    assertTrue(continuous.quantile(8.75 / 12.0) > 10.0);
    assertEquals(continuous.quantile(continuous.cdf(15)), 15.0);

    PercentileTDigestAccumulator fractional = fromBytes(verbose(new double[]{0, 10, 10, 20},
        new double[]{1, 0.25, 0.25, 1}));
    assertEquals(fractional.quantile(1.1 / 2.5), 10.0);
    assertEquals(fractional.quantile(1.4 / 2.5), 10.0);

    PercentileTDigestAccumulator first = fromBytes(verbose(new double[]{0, 0, 10, 20},
        new double[]{1, 2, 3, 1}));
    assertEquals(first.quantile(2.75 / 7.0), 0.0);
    assertEquals(first.quantile(first.cdf(0)), 0.0);
    PercentileTDigestAccumulator last = fromBytes(verbose(new double[]{0, 10, 20, 20},
        new double[]{1, 3, 2, 1}));
    assertEquals(last.quantile(4.25 / 7.0), 20.0);
    assertEquals(last.quantile(last.cdf(20)), 20.0);

    PercentileTDigestAccumulator huge = fromBytes(verbose(new double[]{0, 10, 10, 20, 30},
        new double[]{1, 1e300, 1e300, 1e300, 1}));
    assertEquals(huge.quantile(0.6), 10.0);
    assertEquals(huge.quantile(0), 0.0);
    assertEquals(huge.quantile(1), 30.0);
  }

  @Test
  public void testWeightedSortKeepsPairsAndEqualMeanOrderAcrossFlushes() {
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(1_000_000);
    for (int iteration = 0; iteration < 2; iteration++) {
      double offset = 10 * iteration;
      digest.add(offset + 3, 0.25);
      digest.add(offset + 1, 0.5);
      digest.add(offset + 3, 0.75);
      digest.add(offset + 2, 0.125);
      List<Centroid> centroids = List.copyOf(digest.centroids());
      int start = 4 * iteration;
      assertEquals(centroids.subList(start, start + 4), List.of(new Centroid(offset + 1, 0.5),
          new Centroid(offset + 2, 0.125), new Centroid(offset + 3, 0.25), new Centroid(offset + 3, 0.75)));
      assertEquals(centroids.stream().mapToDouble(Centroid::weight).sum(), digest.getTotalWeight());
    }
  }

  private static PercentileTDigestAccumulator fromBytes(byte[] bytes) {
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forSerializedTDigest(bytes);
    digest.addSerializedTDigest(bytes);
    return digest;
  }

  private static byte[] verbose(double[] means, double[] weights) {
    ByteBuffer encoded = ByteBuffer.allocate(32 + 16 * means.length);
    encoded.putInt(TDigestUtils.VERBOSE_ENCODING);
    encoded.putDouble(means[0]);
    encoded.putDouble(means[means.length - 1]);
    encoded.putDouble(100);
    encoded.putInt(means.length);
    for (int i = 0; i < means.length; i++) {
      encoded.putDouble(weights[i]);
      encoded.putDouble(means[i]);
    }
    return encoded.array();
  }
}
