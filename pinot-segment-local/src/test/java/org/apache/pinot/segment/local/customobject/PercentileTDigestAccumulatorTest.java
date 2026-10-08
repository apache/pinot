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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.pinot.segment.local.utils.TDigestUtils;
import org.apache.pinot.segment.spi.customobject.TDigest.Centroid;
import org.testng.annotations.Test;

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
