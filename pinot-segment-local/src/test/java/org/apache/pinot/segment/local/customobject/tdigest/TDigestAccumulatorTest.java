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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import org.apache.pinot.segment.local.customobject.tdigest.TDigest.Centroid;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/// Regression tests for opaque legacy payloads, fractional statistics, and primitive centroid merging.
public class TDigestAccumulatorTest {
  @Test
  public void testHistoricalCorruptionRemainsOpaqueAndRejectsMutation() {
    for (byte[] poison : new byte[][]{verbose(new double[]{1, 2, 3}, new double[]{1, -0.5, 1}),
        verbose(new double[]{1, 2}, new double[]{1, -200}),
        verbose(new double[]{Double.NaN}, new double[]{1}),
        verbose(new double[]{1, 3}, new double[]{3, -3})}) {
      TDigestAccumulator digest = fromBytes(poison);
      List<Centroid> original = List.copyOf(digest.centroids());
      assertFalse(digest.hasValidStatistics());
      assertFalse(digest.isEmpty(), "Unknown state cannot be discarded as empty");
      assertTrue(Double.isNaN(digest.quantile(0.5)));
      assertTrue(Double.isNaN(digest.cdf(1.5)));
      digest.compress();
      assertEquals(digest.serialize(), poison);
      assertEquals(digest.centroidCount(), original.size());
      double originalWeight = digest.getTotalWeight();
      expectThrows(IllegalArgumentException.class, () -> digest.add(2));
      expectThrows(IllegalArgumentException.class, () -> digest.add(2, 0.5));
      expectThrows(IllegalArgumentException.class, () -> digest.add(new double[]{2}, 0, 1));
      expectThrows(IllegalArgumentException.class, () -> digest.addSerializedTDigest(poison));
      assertEquals(digest.getTotalWeight(), originalWeight);
      assertEquals(List.copyOf(digest.centroids()), original);
      assertEquals(digest.serialize(), poison);

      TDigestAccumulator copy = TDigestAccumulator.forReduction(100);
      copy.add(digest);
      assertEquals(copy.serialize(), poison);
      TDigestAccumulator healthy = TDigestAccumulator.forReduction(100);
      healthy.add(10, 100);
      expectThrows(IllegalArgumentException.class, () -> healthy.add(digest));
      assertEquals(healthy.getTotalWeight(), 100.0, "Historical negative weights must not subtract valid mass");
      assertEquals(healthy.quantile(0.5), 10.0);
      expectThrows(IllegalArgumentException.class, () -> digest.add(healthy));
      assertEquals(digest.serialize(), poison);
    }
  }

  @Test
  public void testSubUnitFractionalQuantileCdfAndHistoricalSerialization() {
    TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
    digest.add(1, 0.3);
    digest.add(2, 0.3);
    assertEquals(digest.getTotalWeight(), 0.6);
    assertEquals(digest.quantile(0), 1.0);
    assertEquals(digest.quantile(0.5), 1.5, Math.ulp(1.5));
    assertEquals(digest.quantile(0.99), 2.0);
    assertEquals(digest.quantile(1), 2.0);
    assertEquals(digest.cdf(1), 0.25);
    assertEquals(digest.cdf(1.5), 0.5);
    assertEquals(digest.cdf(2), 0.75);
    expectThrows(IllegalArgumentException.class, digest::serialize);

    TDigest wideBounds = TDigestAccumulator.fromBytes(TDigestCodec.serializeCentroids(100, 0, 3,
        new double[]{1, 2}, new double[]{0.3, 0.3}, 2));
    assertEquals(wideBounds.quantile(0.25), 1.0);
    assertEquals(wideBounds.quantile(0.75), 2.0);
    assertEquals(wideBounds.cdf(2.0), 0.75);


    for (byte[] historical : new byte[][]{verbose(new double[]{1, 2}, new double[]{0.3, 0.3}),
        verbose(new double[]{42}, new double[]{1.5}),
        TDigestCodec.serializeCentroids(100, 0, 3, new double[]{1, 0, 2, 3}, new double[]{1, 0.3, 1, 1}, 4)}) {
      TDigestAccumulator stored = fromBytes(historical);
      assertEquals(stored.serialize(), historical);
      stored.quantile(0.5);
      stored.centroids();
      assertEquals(stored.serialize(), historical, "Read-only materialization preserves unsupported original bytes");
      for (double compression : new double[]{stored.compression(), 500}) {
        TDigestAccumulator copy = TDigestAccumulator.forReduction(compression);
        copy.add(stored);
        assertEquals(copy.serialize(), historical, "Copying a queried source retains its original encoding");
        copy.quantile(0.5);
        copy.compress();
        assertEquals(copy.serialize(), historical,
            "Configured compression cannot rewrite an unchanged fractional input");
        TDigestAccumulator adopted = TDigestAccumulator.forReduction(compression);
        adopted.addSerializedTDigest(historical);
        assertEquals(adopted.serialize(), historical);
        adopted.quantile(0.5);
        assertEquals(adopted.serialize(), historical);
      }
      expectThrows(IllegalArgumentException.class, () -> stored.add(Double.NaN));
      expectThrows(IllegalArgumentException.class,
          () -> stored.add(fromBytes(verbose(new double[]{Double.NaN}, new double[]{1}))));
      assertEquals(stored.serialize(), historical, "Failed mutation preserves retained original bytes");
      byte[] isolated = stored.serialize();
      isolated[0] = 0;
      assertEquals(stored.serialize(), historical);
    }
    TDigestAccumulator singleton = TDigestAccumulator.forReduction(100);
    singleton.add(42, 1.5);
    assertEquals(singleton.quantile(1), 42.0);
    expectThrows(IllegalArgumentException.class, singleton::serialize);
    singleton = fromBytes(verbose(new double[]{42}, new double[]{1.5}));
    singleton.add(1);
    assertFalse(singleton.hasOriginalFractionalPayload());
    assertEquals(TDigestAccumulator.fromBytes(singleton.serialize()).getTotalWeight(), 2.5);
  }

  @Test
  public void testQuantileRoundingAtLastCentroidRemainsMonotonic() {
    byte[] historical = verbose(new double[]{1, 5}, new double[]{0.7, 0.9});
    ByteBuffer.wrap(historical).putDouble(Integer.BYTES, 0).putDouble(Integer.BYTES + Double.BYTES, 10);
    TDigestAccumulator digest = fromBytes(historical);
    // The loop accumulates the last center as 1.15; totalWeight - lastWeight / 2 is 1.1500000000000001.
    double quantile = Math.nextDown(0.71875);
    double below = digest.quantile(Math.nextDown(quantile));
    double at = digest.quantile(quantile);
    double above = digest.quantile(Math.nextUp(quantile));
    assertEquals(at, 5.0, Math.ulp(5.0));
    assertTrue(below <= at && at <= above);
    assertEquals(digest.getTotalWeight(), 1.6);
    assertEquals(digest.quantile(0), 0.0);
    assertEquals(digest.quantile(1), 10.0);
    assertEquals(digest.serialize(), historical);
  }

  @Test
  public void testMixedHistoricalFractionalBoundariesPreserveMassAndFiniteStatistics() {
    byte[] historical = verbose(new double[]{0, 5, 10}, new double[]{0.5, 4, 1});
    for (boolean historyFirst : new boolean[]{false, true}) {
      for (boolean direct : new boolean[]{false, true}) {
        TDigestAccumulator digest = TDigestAccumulator.forReduction(200);
        if (!historyFirst) {
          digest.add(7);
        }
        if (direct) {
          var input = new SerializedTDigestInput();
          input.reset(historical);
          digest.addSerializedTDigestDirect(input);
        } else {
          digest.addSerializedTDigest(historical);
        }
        if (historyFirst) {
          digest.add(7);
        }
        assertEquals(digest.getTotalWeight(), 6.5);
        double median = digest.quantile(0.5);
        assertTrue(Double.isFinite(median));
        assertTrue(Double.isFinite(digest.cdf(5)));
        byte[] bytes = digest.serialize();
        assertEquals(ByteBuffer.wrap(bytes).getInt(0), TDigestCodec.VERBOSE_ENCODING);
        assertEquals(ByteBuffer.wrap(bytes).getDouble(32), 0.5);
        TDigestAccumulator restored = fromBytes(bytes);
        assertEquals(restored.getTotalWeight(), 6.5);
        assertEquals(restored.quantile(0.5), median);
        assertEquals(restored.getMin(), 0.0);
        assertEquals(restored.getMax(), 10.0);

        TDigestAccumulator copy = TDigestAccumulator.forReduction(500);
        copy.add(digest);
        assertEquals(fromBytes(copy.serialize()).getTotalWeight(), 6.5);
        // Historical provenance may not authorize a newly added unsupported global endpoint.
        copy.add(-1, 0.1);
        expectThrows(IllegalArgumentException.class, copy::serialize);
      }
    }
    for (boolean historyFirst : new boolean[]{false, true}) {
      TDigestAccumulator fractional = TDigestAccumulator.forReduction(100);
      if (!historyFirst) {
        fractional.add(10);
      }
      fractional.add(fromBytes(verbose(new double[]{0}, new double[]{0.5})));
      if (historyFirst) {
        fractional.add(10);
      }
      assertEquals(fractional.getTotalWeight(), 1.5);
      assertTrue(Double.isFinite(fractional.quantile(0.5)));
      assertEquals(fromBytes(fractional.serialize()).getTotalWeight(), 1.5);
      for (boolean newExtrema : new boolean[]{false, true}) {
        TDigestAccumulator representable = fromBytes(historical);
        representable.add(newExtrema ? -1 : 0);
        ByteBuffer bytes = ByteBuffer.wrap(representable.serialize());
        assertEquals(bytes.getDouble(32), 1.0, "Representable historical boundaries use the normal unit repair");
        assertEquals(fromBytes(bytes.array()).getTotalWeight(), 6.5);
      }
    }
    TDigestAccumulator fresh = TDigestAccumulator.forReduction(100);
    fresh.add(0, 0.5);
    fresh.add(10);
    expectThrows(IllegalArgumentException.class, fresh::serialize);
  }

  @Test
  public void testQueryCompressionRetainsLegacyClamping() {
    assertEquals(new TDigestAccumulator(0).compression(), 10.0);
    assertEquals(new TDigestAccumulator(-5).compression(), 10.0);
    assertEquals(TDigestAccumulator.forReduction(0).compression(), 10.0);
    byte[] historical = verbose(new double[]{1}, new double[]{1});
    ByteBuffer.wrap(historical).putDouble(20, -5);
    assertEquals(fromBytes(historical).compression(), 10.0);
  }

  @Test
  public void testBulkCentroidsSortWithoutChangingCallerArrays() {
    double[] means = {20, Double.NaN, 10, 30};
    double[] weights = {2, 0, 1, 1};
    TDigestAccumulator digest = TDigestAccumulator.forLegacyAggregation(100);
    digest.addCentroids(means, weights, means.length, 10, 30, true);
    assertEquals(means, new double[]{20, Double.NaN, 10, 30});
    assertEquals(weights, new double[]{2, 0, 1, 1});
    assertEquals(digest.getTotalWeight(), 4.0);
    assertEquals(digest.quantile(0), 10.0);
    assertEquals(digest.quantile(1), 30.0);
    assertEquals(digest.centroids(), fromBytes(verbose(new double[]{10, 20, 30}, new double[]{1, 2, 1})).centroids());
    double[] copiedMeans = {-1, -1, -1, -1, -1};
    double[] copiedWeights = {-1, -1, -1, -1, -1};
    digest.copyCentroids(copiedMeans, copiedWeights, 1);
    assertEquals(copiedMeans, new double[]{-1, 10, 20, 30, -1});
    assertEquals(copiedWeights, new double[]{-1, 1, 2, 1, -1});
    copiedMeans[2] = -2;
    assertEquals(digest.quantile(0.5), 20.0);
    digest.addCentroids(new double[]{5, 25}, new double[]{1, 1}, 2, 5, 25, false);
    assertEquals(digest.getTotalWeight(), 6.0);
    assertEquals(digest.quantile(0), 5.0);
    assertEquals(digest.quantile(1), 30.0);
    TDigestAccumulator pending = TDigestAccumulator.forReduction(100);
    pending.addSerializedTDigest(TDigestCodec.serializeCentroids(100, 1, 9,
        new double[]{5, 1, 9}, new double[]{1, 4, 1}, 3));
    ByteBuffer ordered = ByteBuffer.wrap(pending.serialize());
    ordered.position(28);
    int count = ordered.getInt();
    double previous = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < count; i++) {
      double weight = ordered.getDouble();
      double mean = ordered.getDouble();
      assertTrue(mean >= previous);
      if (i == 0 || i == count - 1) {
        assertEquals(weight, 1.0);
      }
      previous = mean;
    }
    assertEquals(pending.getTotalWeight(), 6.0);
    for (double[][] fixture : new double[][][]{
        {{1, 5, 9}, {3, 1, 3}}, {{1, 5, 9}, {0, 1, 0}}, {{1, 5, 9}, {3, 0, 3}},
        {{5, 1, 9}, {1, 3, 1}}, {{5, 1, 9, 6}, {1, 3, 3, 1}}
    }) {
      byte[] payload = TDigestCodec.serializeCentroids(100, 1, 9, fixture[0], fixture[1], fixture[0].length);
      TDigestAccumulator normalized = fromBytes(payload);
      int normalizedCount = normalized.centroidCount();
      double[] normalizedMeans = new double[normalizedCount];
      double[] normalizedWeights = new double[normalizedCount];
      normalized.copyCentroids(normalizedMeans, normalizedWeights, 0);
      assertEquals(normalized.centroids().size(), normalizedCount);
      double totalWeight = 0.0;
      for (double weight : normalizedWeights) {
        assertTrue(weight > 0.0);
        totalWeight += weight;
      }
      assertEquals(totalWeight, normalized.getTotalWeight());
    }
  }

  @Test
  public void testSerializationOwnsBytesAcrossMutationAndHistoricalRetention() {
    TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
    digest.add(10);
    byte[] first = digest.serialize();
    byte[] snapshot = first.clone();
    digest.add(20);
    assertEquals(first, snapshot, "Subsequent mutations must not change returned bytes");
    assertEquals(fromBytes(digest.serialize()).getTotalWeight(), 2.0);
    first[0] = 0;
    assertEquals(fromBytes(digest.serialize()).getTotalWeight(), 2.0);

    byte[] historical = verbose(new double[]{1, 2}, new double[]{0.5, 1});
    TDigestAccumulator retained = TDigestAccumulator.fromBytes(historical);
    byte[] owned = retained.serialize();
    historical[0] = 0;
    owned[0] = 0;
    assertEquals(retained.serialize(), verbose(new double[]{1, 2}, new double[]{0.5, 1}));
    byte[] trailing = ByteBuffer.allocate(snapshot.length + Integer.BYTES).put(snapshot).putInt(1234).array();
    TDigestAccumulator bounded = TDigestAccumulator.fromBytes(trailing);
    assertEquals(bounded.serialize(), snapshot, "The reading factory retains only one complete payload");
    assertEquals(bounded.maxSerializedByteSize(), snapshot.length);
  }

  @Test
  public void testMixedCompressionOversizedSourcesSerializeBeforeAnyQuery() {
    int count = 5000;
    double[] means = new double[count];
    double[] weights = new double[count];
    for (int i = 0; i < count; i++) {
      means[i] = i + 1;
      weights[i] = i == 0 || i == count - 1 ? 1 : 2;
    }
    byte[] bytes = TDigestCodec.serializeCentroids(10000, 1, count, means, weights, count);
    for (boolean legacy : new boolean[]{false, true}) {
      for (boolean existing : new boolean[]{false, true}) {
        for (int route = 0; route < 5; route++) {
          TDigestAccumulator source = TDigestAccumulator.fromBytes(bytes);
          source.add(2500);
          TDigestAccumulator target = legacy ? TDigestAccumulator.forLegacyAggregation(100)
              : TDigestAccumulator.forReduction(100);
          if (existing) {
            target.add(2500);
          }
          switch (route) {
            case 0 -> target.add(source);
            case 1 -> {
              source.compress();
              int sourceCount = source.centroidCount();
              double[] sourceMeans = new double[sourceCount];
              double[] sourceWeights = new double[sourceCount];
              source.copyCentroids(sourceMeans, sourceWeights, 0);
              double[] originalMeans = sourceMeans.clone();
              double[] originalWeights = sourceWeights.clone();
              target.addCentroids(sourceMeans, sourceWeights, sourceCount, 1, count, false);
              assertEquals(sourceMeans, originalMeans);
              assertEquals(sourceWeights, originalWeights);
            }
            case 2 -> {
              TDigest generic = mock(TDigest.class);
              when(generic.hasValidStatistics()).thenReturn(true);
              when(generic.getTotalWeight()).thenReturn(9999.0);
              when(generic.getMin()).thenReturn(1.0);
              when(generic.getMax()).thenReturn((double) count);
              when(generic.centroids()).thenReturn(source.centroids());
              target.add(generic);
            }
            case 3 -> target.addSerializedTDigest(source.serialize());
            case 4 -> {
              SerializedTDigestInput input = new SerializedTDigestInput();
              input.reset(source.serialize());
              target.addSerializedTDigestDirect(input);
            }
            default -> throw new AssertionError();
          }
          // No query or centroid read of the target may repair it before the actual wire boundary.
          TDigest restored = TDigestAccumulator.fromBytes(target.serialize());
          assertTrue(restored.hasValidStatistics(), "route=" + route + ", legacy=" + legacy);
          assertEquals(restored.getTotalWeight(), existing ? 10000.0 : 9999.0);
          assertEquals(restored.getMin(), 1.0);
          assertEquals(restored.getMax(), (double) count);
          assertTrue(Double.isFinite(restored.quantile(0.198)));
          assertEquals(restored.quantile(0.198), 990.901, 30.0);
        }
      }
    }
  }

  @Test
  public void testBufferBoundaryDoesNotSplitAnInteriorCentroidIntoPointMass() {
    int count = 5 * TDigestCodec.getDefaultCentroidCapacity(100) - 3;
    double[] values = new double[count];
    Arrays.fill(values, -100);
    for (boolean serialized : new boolean[]{false, true}) {
      TDigestAccumulator digest = TDigestAccumulator.forLegacyAggregation(100);
      digest.add(values, 0, values.length);
      byte[] bytes = verbose(new double[]{0, 100, 200}, new double[]{1, 1000, 1});
      if (serialized) {
        digest.addSerializedTDigest(bytes);
      } else {
        digest.add(fromBytes(bytes));
      }
      // The old chunked path ended its first batch at the weighted interior mean, then repaired it into
      // adjacent (100, 999) and (100, 1) centroids that incorrectly pinned this quantile to 100.
      TDigest restored = fromBytes(digest.serialize());
      double quantile = restored.quantile(0.75);
      assertTrue(quantile > 0 && quantile < 100);
      List<Centroid> centroids = List.copyOf(restored.centroids());
      assertEquals(centroids.stream().filter(centroid -> centroid.mean() == 100).count(), 1L);
      assertEquals(centroids.stream().mapToDouble(Centroid::weight).sum(), count + 1002.0);
      assertEquals(centroids.stream().mapToDouble(centroid -> centroid.mean() * centroid.weight()).sum(),
          -100.0 * count + 100_200.0, 1e-8);
      assertEquals(restored.getMin(), -100.0);
      assertEquals(restored.getMax(), 200.0);
    }
  }

  @Test
  public void testOrdinaryMixedCompressionSourcesStraddleOnePendingBatch() {
    byte[][] inputs = new byte[2][];
    for (int source = 0; source < inputs.length; source++) {
      int count = source == 0 ? 600 : 500;
      int offset = source == 0 ? 0 : 1000;
      double[] means = new double[count];
      double[] weights = new double[count];
      for (int i = 0; i < count; i++) {
        means[i] = offset + i;
        weights[i] = i == 0 || i == count - 1 ? 1 : 2;
      }
      inputs[source] = TDigestCodec.serializeCentroids(10000, offset, offset + count - 1, means, weights, count);
    }
    for (boolean reversed : new boolean[]{false, true}) {
      for (boolean serialized : new boolean[]{false, true}) {
        TDigestAccumulator target = TDigestAccumulator.forReduction(100);
        target.add(0.0);
        for (int i = 0; i < inputs.length; i++) {
          byte[] input = inputs[reversed ? inputs.length - i - 1 : i];
          if (serialized) {
            target.addSerializedTDigest(input);
          } else {
            target.add(TDigestAccumulator.fromBytes(input));
          }
        }
        TDigest restored = TDigestAccumulator.fromBytes(target.serialize());
        assertTrue(restored.hasValidStatistics());
        assertEquals(restored.getTotalWeight(), 2197.0);
        assertEquals(restored.getMin(), 0.0);
        assertEquals(restored.getMax(), 1499.0);
        double moment = restored.centroids().stream().mapToDouble(c -> c.mean() * c.weight()).sum();
        assertEquals(moment, 1_605_802.0, 1e-8, "reversed=" + reversed + ", serialized=" + serialized);
      }
    }
  }

  @Test
  public void testReadThenAddWeightedExtremaPreservesFirstMoment() {
    for (double extreme : new double[]{-100, 100}) {
      for (int read = 0; read < 3; read++) {
        for (int route = 0; route < 6; route++) {
          TDigestAccumulator target = TDigestAccumulator.forReduction(100);
          target.add(10, 2);
          switch (read) {
            case 0 -> target.quantile(0.5);
            case 1 -> target.centroids();
            case 2 -> target.compress();
            default -> throw new AssertionError();
          }
          TDigestAccumulator source = TDigestAccumulator.forReduction(100);
          source.add(extreme, 2);
          switch (route) {
            case 0 -> target.add(extreme, 2);
            case 1 -> target.addCentroids(new double[]{extreme}, new double[]{2}, 1, extreme, extreme, false);
            case 2 -> target.add(source);
            case 3 -> {
              TDigest generic = mock(TDigest.class);
              when(generic.hasValidStatistics()).thenReturn(true);
              when(generic.getTotalWeight()).thenReturn(2.0);
              when(generic.getMin()).thenReturn(extreme);
              when(generic.getMax()).thenReturn(extreme);
              when(generic.centroids()).thenReturn(List.of(new Centroid(extreme, 2)));
              target.add(generic);
            }
            case 4 -> target.addSerializedTDigest(source.serialize());
            case 5 -> {
              SerializedTDigestInput input = new SerializedTDigestInput();
              input.reset(source.serialize());
              target.addSerializedTDigestDirect(input);
            }
            default -> throw new AssertionError();
          }
          TDigest restored = TDigestAccumulator.fromBytes(target.serialize());
          assertTrue(restored.hasValidStatistics());
          assertEquals(restored.getTotalWeight(), 4.0);
          assertEquals(restored.getMin(), Math.min(10, extreme));
          assertEquals(restored.getMax(), Math.max(10, extreme));
          double moment = restored.centroids().stream().mapToDouble(c -> c.mean() * c.weight()).sum();
          assertEquals(moment, 20 + 2 * extreme, "read=" + read + ", route=" + route);
        }
      }
    }
  }

  @Test
  public void testLegacyBoundaryResidualAllowsLocalRoundoff() {
    double first = 0x1.117f03c25ccf8p+8;
    double second = 0x1.174ec5c369cebp+8;
    double mean = first + (second - first) / 2.0;
    assertEquals(mean + (mean - first), Math.nextUp(second));
    for (boolean reflected : new boolean[]{false, true}) {
      double min = reflected ? -second : first;
      double max = reflected ? -first : second;
      double[] means = reflected ? new double[]{-second, -mean} : new double[]{mean, second};
      double[] weights = reflected ? new double[]{1, 2} : new double[]{2, 1};
      byte[] bytes = TDigestCodec.serializeCentroids(100, min, max, means, weights, 2);
      for (boolean direct : new boolean[]{false, true}) {
        TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
        double addedValue = reflected ? -mean : mean;
        digest.add(addedValue);
        if (direct) {
          SerializedTDigestInput input = new SerializedTDigestInput();
          input.reset(bytes);
          digest.addSerializedTDigestDirect(input);
        } else {
          digest.add(fromBytes(bytes));
        }
        TDigest restored = fromBytes(digest.serialize());
        assertEquals(restored.getTotalWeight(), 4.0);
        assertEquals(restored.getMin(), min);
        assertEquals(restored.getMax(), max);
        double expectedMoment = (reflected ? -1 : 1) * (3 * mean + second);
        assertEquals(restored.centroids().stream().mapToDouble(c -> c.mean() * c.weight()).sum(), expectedMoment,
            4 * Math.ulp(expectedMoment));
      }
      // A materially inconsistent first moment must still reject mutation and retain the original payload.
      means[reflected ? 1 : 0] += (reflected ? -8 : 8) * Math.ulp(mean);
      byte[] inconsistent = TDigestCodec.serializeCentroids(100, min, max, means, weights, 2);
      TDigestAccumulator rejected = fromBytes(inconsistent);
      expectThrows(IllegalArgumentException.class, () -> rejected.add(min));
      assertEquals(rejected.serialize(), inconsistent);
      assertEquals(rejected.getTotalWeight(), 3.0);
    }
    expectThrows(IllegalArgumentException.class,
        () -> TDigestAccumulator.checkedResidualWeight(0x1.0p54, 1.0));
  }

  @Test
  public void testTwoPointBoundaryRepairAllowsLegacyMidpointRoundoff() {
    double min = -0x1.f1e4d9b005593p+9;
    double max = 0x1.0647b37be50b4p+9;
    double mean = min + (max - min) / 2.0;
    double midpoint = min / 2.0 + max / 2.0;
    assertEquals(mean - midpoint, 2 * Math.ulp(midpoint));
    for (double storedMean : new double[]{mean, -mean}) {
      double lower = storedMean == mean ? min : -max;
      double upper = storedMean == mean ? max : -min;
      byte[] bytes = TDigestCodec.serializeCentroids(100, lower, upper, new double[]{storedMean}, new double[]{2}, 1);
      TDigestAccumulator digest = fromBytes(bytes);
      digest.add(0);
      TDigest restored = fromBytes(digest.serialize());
      assertEquals(restored.getTotalWeight(), 3.0);
      assertEquals(restored.getMin(), lower);
      assertEquals(restored.getMax(), upper);
      assertEquals(restored.centroids().stream().mapToDouble(c -> c.mean() * c.weight()).sum(), 2 * storedMean,
          4 * Math.ulp(max));
      byte[] inconsistent = TDigestCodec.serializeCentroids(100, lower, upper,
          new double[]{storedMean + 4 * Math.ulp(max)}, new double[]{2}, 1);
      TDigestAccumulator rejected = fromBytes(inconsistent);
      expectThrows(IllegalArgumentException.class, () -> rejected.add(0));
      assertEquals(rejected.serialize(), inconsistent);
    }
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
    TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
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
    TDigestAccumulator pending = fromBytes(empty);
    for (int phase = 0; phase < 2; phase++) {
      assertTrue(pending.isEmpty());
      assertEquals(pending.getMin(), Double.POSITIVE_INFINITY);
      assertEquals(pending.getMax(), Double.NEGATIVE_INFINITY);
      assertEquals(pending.centroidCount(), 0);
      pending.compress();
    }
    for (boolean direct : new boolean[]{false, true}) {
      TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
      digest.add(10);
      digest.add(20);
      if (direct) {
        SerializedTDigestInput input =
            new SerializedTDigestInput();
        input.reset(empty);
        digest.addSerializedTDigestDirect(input);
      } else {
        digest.addSerializedTDigest(empty);
      }
      assertEquals(digest.getTotalWeight(), 2.0);
      assertEquals(digest.quantile(0), 10.0);
      assertEquals(digest.quantile(1), 20.0);
    }
    byte[] poison = verbose(new double[]{Double.NaN}, new double[]{1});
    TDigest genericEmpty = mock(TDigest.class);
    when(genericEmpty.hasValidStatistics()).thenReturn(true);
    when(genericEmpty.getTotalWeight()).thenReturn(0.0);
    for (TDigest emptyObject : new TDigest[]{fromBytes(empty), genericEmpty}) {
      for (boolean emptyFirst : new boolean[]{false, true}) {
        TDigestAccumulator objectMerge = TDigestAccumulator.forReduction(100);
        for (TDigest input : emptyFirst ? new TDigest[]{emptyObject, fromBytes(poison)}
            : new TDigest[]{fromBytes(poison), emptyObject}) {
          objectMerge.add(input);
        }
        assertFalse(objectMerge.hasValidStatistics());
        assertEquals(objectMerge.serialize(), poison);
        TDigest cancelledCorruption = fromBytes(verbose(new double[]{0, 1}, new double[]{1, -1}));
        expectThrows(IllegalArgumentException.class, () -> objectMerge.add(cancelledCorruption));
      }
    }
    for (boolean emptyFirst : new boolean[]{false, true}) {
      for (boolean direct : new boolean[]{false, true}) {
        TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
        for (byte[] bytes : emptyFirst ? new byte[][]{empty, poison} : new byte[][]{poison, empty}) {
          if (direct) {
            SerializedTDigestInput input =
                new SerializedTDigestInput();
            input.reset(bytes);
            digest.addSerializedTDigestDirect(input);
          } else {
            digest.addSerializedTDigest(bytes);
          }
        }
        assertFalse(digest.hasValidStatistics());
        assertTrue(Double.isNaN(digest.quantile(0.5)));
        assertEquals(digest.serialize(), poison);
        assertEquals(digest.getTotalWeight(), 1.0);
        byte[] cancelledCorruption = verbose(new double[]{0, 1}, new double[]{1, -1});
        expectThrows(IllegalArgumentException.class, () -> digest.addSerializedTDigest(cancelledCorruption));
      }
    }
  }

  @Test
  public void testSerializationRetainsFieldsThatOverflowOrUnderflowFloat() {
    for (double[] centroid : new double[][]{{1e39, 1}, {-1e39, 1}}) {
      TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
      digest.add(centroid[0], centroid[1]);
      ByteBuffer encoded = ByteBuffer.wrap(digest.serialize());
      assertEquals(encoded.getInt(0), TDigestCodec.VERBOSE_ENCODING);
      TDigest restored = TDigestAccumulator.fromBytes(encoded.array());
      assertTrue(restored.hasValidStatistics());
      assertEquals(restored.getTotalWeight(), centroid[1]);
      assertEquals(restored.getMin(), centroid[0]);
      assertEquals(restored.getMax(), centroid[0]);
    }
    for (double weight : new double[]{1e-46, 1e39}) {
      TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
      digest.add(0);
      digest.add(1, weight);
      digest.add(2);
      ByteBuffer encoded = ByteBuffer.wrap(digest.serialize());
      assertEquals(encoded.getInt(0), TDigestCodec.VERBOSE_ENCODING);
      TDigest restored = TDigestAccumulator.fromBytes(encoded.array());
      assertEquals(List.copyOf(restored.centroids()).get(1).weight(), weight,
          "An interior mass must not narrow to compact zero or infinity");
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
        TDigestAccumulator digest = TDigestAccumulator.forReduction(compression);
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
    for (double compression : new double[]{100, 10_000}) {
      int capacity = 5 * TDigestCodec.getDefaultCentroidCapacity(compression);
      TDigestAccumulator legacy = TDigestAccumulator.forLegacyAggregation(compression);
      double[] raw = new double[capacity - 2];
      for (int i = 0; i < raw.length; i++) {
        raw[i] = i;
      }
      legacy.add(raw, 0, raw.length);
      assertEquals((int) field(legacy, "_mergeCount"), 0, "Legacy raw batches retain the original 5x buffer policy");
      assertEquals(((double[]) field(legacy, "_rawValues")).length, capacity);
      legacy.add(raw.length);
      assertEquals((int) field(legacy, "_mergeCount"), 0);
      legacy.add(raw.length + 1);
      assertEquals((int) field(legacy, "_mergeCount"), 1);
      assertEquals(legacy.getTotalWeight(), (double) capacity);
    }
    TDigestAccumulator extreme = TDigestAccumulator.forLegacyAggregation(Double.MAX_VALUE);
    extreme.add(new double[10_001], 0, 10_001);
    assertEquals((int) field(extreme, "_mergeCount"), 0);
    assertEquals(((double[]) field(extreme, "_rawValues")).length, 20_000);
    assertEquals(extreme.getTotalWeight(), 10_001.0);
  }

  @Test
  public void testOverflowDuringFlushRetainsIncomingPairs() throws Exception {
    TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
    // Unit endpoints keep boundary repair exact; this test targets overflow while flushing pending pairs.
    digest.addSerializedTDigest(verbose(new double[]{0, 10, 40}, new double[]{1, Double.MAX_VALUE, 1}));
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

  private static Object field(TDigestAccumulator digest, String name) throws Exception {
    Field field = TDigestAccumulator.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(digest);
  }

  @Test
  public void testCompatibleByteBoundCapsBufferedStateAndRetainsPendingBytes() throws Exception {
    TDigestAccumulator buffered = TDigestAccumulator.forLegacyAggregation(100);
    for (int i = 0; i < 1000; i++) {
      buffered.add(i, 1000);
    }
    int legacyBound = TDigestCodec.VERBOSE_HEADER_SIZE + 210 * TDigestCodec.VERBOSE_CENTROID_SIZE;
    assertEquals(buffered.maxSerializedByteSize(), legacyBound);
    assertTrue(buffered.serialize().length <= legacyBound);

    for (double sourceCompression : new double[]{100, 1000}) {
      double[] means = new double[sourceCompression == 100 ? 100 : 500];
      double[] weights = new double[means.length];
      for (int i = 0; i < means.length; i++) {
        means[i] = i;
        weights[i] = 1.0;
      }
      byte[] verbose = TDigestCodec.serializeCentroids(sourceCompression, 0, means.length - 1, means, weights,
          means.length);
      TDigestAccumulator smaller = TDigestAccumulator.forLegacyAggregation(10);
      smaller.addSerializedTDigest(verbose);
      int bound = smaller.maxSerializedByteSize();
      assertTrue(bound >= verbose.length, "Retained verbose input can exceed configured output capacity");
      assertEquals((int) field(smaller, "_mergeCount"), 0, "Sizing must not compress retained input");
      assertEquals(smaller.serialize(), verbose);
      assertTrue(smaller.serialize().length <= bound);
    }

    int count = 500;
    ByteBuffer compact = ByteBuffer.allocate(TDigestCodec.SMALL_HEADER_SIZE + count * TDigestCodec.SMALL_CENTROID_SIZE);
    compact.putInt(TDigestCodec.SMALL_ENCODING).putDouble(0).putDouble(count - 1).putFloat(100);
    compact.putShort((short) count).putShort((short) (5 * count)).putShort((short) count);
    for (int i = 0; i < count; i++) {
      compact.putFloat(1).putFloat(i);
    }
    TDigestAccumulator pending = fromBytes(compact.array());
    assertEquals(pending.maxSerializedByteSize(), compact.capacity());
    byte[] retained = pending.serialize();
    assertEquals(retained, compact.array());
    retained[0] = 0;
    assertEquals(pending.serialize(), compact.array());
    pending.quantile(0.5);
    assertEquals(pending.maxSerializedByteSize(), legacyBound);
    byte[] rewritten = pending.serialize();
    assertTrue(rewritten.length <= legacyBound);
    assertEquals(ByteBuffer.wrap(rewritten).getInt(), TDigestCodec.VERBOSE_ENCODING);
    assertEquals(fromBytes(rewritten).getTotalWeight(), (double) count);
    assertEquals(pending.getMin(), 0.0);
    assertEquals(pending.getMax(), count - 1.0);
    TDigestAccumulator copy = TDigestAccumulator.forReduction(100);
    copy.add(pending);
    assertEquals(copy.getTotalWeight(), (double) count);
    assertTrue(Double.isFinite(copy.quantile(0.5)));
    assertTrue(copy.serialize().length <= copy.maxSerializedByteSize());
  }

  @Test
  public void testSerializedResultComparisonMatchesRenderedDigest() {
    TDigestAccumulator digest = TDigestAccumulator.forReduction(10);
    TDigestAccumulator control = TDigestAccumulator.forReduction(10);
    SplittableRandom random = new SplittableRandom(0);
    for (int i = 0; i < 10000; i++) {
      double value = random.nextDouble();
      digest.add(value);
      control.add(value);
    }
    double workingMedian = digest.quantile(0.5);
    control.quantile(0.5);
    control.compress();
    double publicMedian = control.quantile(0.5);
    assertTrue(workingMedian != publicMedian, "Exercise a reducer changed by public compression");
    TDigestAccumulator nearby = new TDigestAccumulator(10);
    nearby.add((workingMedian + publicMedian) / 2.0);
    SerializedTDigest result = new SerializedTDigest(digest, 50);
    SerializedTDigest other = new SerializedTDigest(nearby, 50);
    int comparison = result.compareTo(other);
    String rendered = result.toString();
    TDigest decoded = fromBytes(BytesUtils.toBytes(rendered));
    assertEquals(comparison, Double.compare(decoded.quantile(0.5), nearby.quantile(0.5)));
    assertEquals(result.compareTo(other), comparison);
    assertEquals(result.toString(), rendered);
    assertEquals(digest.quantile(0.5), workingMedian, "Extracting a trim key must not compress the survivor");
    digest.add(2);
    assertEquals(result.compareTo(other), comparison);
    assertEquals(result.toString(), rendered);
  }

  @Test
  public void testRepeatedRawResultExtractionDoesNotChangeContinuingAggregation() {
    for (boolean wrapped : new boolean[]{false, true}) {
      TDigest control = wrapped
          ? NonFiniteAwareTDigest.forLegacyAggregation(100) : TDigestAccumulator.forReduction(100);
      TDigest trimmed = wrapped
          ? NonFiniteAwareTDigest.forLegacyAggregation(100) : TDigestAccumulator.forReduction(100);
      if (wrapped) {
        control.add(Double.NEGATIVE_INFINITY);
        control.add(Double.POSITIVE_INFINITY);
        trimmed.add(Double.NEGATIVE_INFINITY);
        trimmed.add(Double.POSITIVE_INFINITY);
      }
      Random random = new Random(7);
      for (int shard = 0; shard < 32; shard++) {
        TDigestAccumulator partial = TDigestAccumulator.forReduction(100);
        for (int i = 0; i < 2_000; i++) {
          partial.add(StrictMath.pow(random.nextDouble(), 8));
        }
        byte[] bytes = partial.serialize();
        control.add(fromBytes(bytes));
        trimmed.add(fromBytes(bytes));
        if (shard % 4 == 3 && shard < 31) {
          SerializedTDigest result = new SerializedTDigest(trimmed, 75);
          String rendered = result.toString();
          TDigestAccumulator reference = TDigestAccumulator.forReduction(100);
          reference.add(fromBytes(BytesUtils.toBytes(rendered)).quantile(0.75));
          assertEquals(result.compareTo(new SerializedTDigest(reference, 75)), 0);
          assertEquals(new SerializedTDigest(trimmed, 75).toString(), rendered);
        }
      }
      assertEquals(trimmed.serialize(), control.serialize(), "Trimming must preserve the continuing merge trajectory");
    }
  }

  @Test
  public void testSerializedResultComparisonUsesLegacyCapacityRewrite() {
    double[] means = new double[51];
    double[] weights = new double[51];
    SplittableRandom random = new SplittableRandom(0);
    for (int i = 0; i < means.length; i++) {
      means[i] = random.nextDouble();
      weights[i] = 1;
    }
    Arrays.sort(means);
    TDigest digest = TDigestAccumulator.fromBytes(
        TDigestCodec.serializeCentroids(20, means[0], means[50], means, weights, means.length));
    double originalQuantile = digest.quantile(0.53);
    SerializedTDigest result = new SerializedTDigest(digest, 53);
    TDigest rendered = fromBytes(BytesUtils.toBytes(result.toString()));
    double emittedQuantile = rendered.quantile(0.53);
    assertTrue(originalQuantile != emittedQuantile, "Exercise the legacy writer's centroid-capacity reduction");
    TDigestAccumulator nearby = new TDigestAccumulator(20);
    nearby.add((originalQuantile + emittedQuantile) / 2.0);
    assertEquals(result.compareTo(new SerializedTDigest(nearby, 53)),
        Double.compare(emittedQuantile, nearby.quantile(0.53)));
  }

  @Test
  public void testSerializedResultRetainsHistoricalStatisticsAcrossCompressionSettings() {
    byte[] boundary = verbose(new double[]{1, 5}, new double[]{2, 2});
    ByteBuffer.wrap(boundary).putDouble(Integer.BYTES, 0).putDouble(Integer.BYTES + Double.BYTES, 10);
    double[] means = new double[100];
    double[] weights = new double[100];
    SplittableRandom random = new SplittableRandom(63);
    for (int i = 0; i < means.length; i++) {
      means[i] = random.nextDouble();
      weights[i] = 1.0;
    }
    Arrays.sort(means);
    weights[0] = 0.5;
    for (byte[] bytes : new byte[][]{boundary, verbose(means, weights)}) {
      double expected = fromBytes(bytes).quantile(0.5);
      for (boolean wrapped : new boolean[]{false, true}) {
        TDigestAccumulator nativeDigest = TDigestAccumulator.forReduction(10);
        nativeDigest.addSerializedTDigest(bytes);
        TDigest digest = nativeDigest;
        if (wrapped) {
          digest = NonFiniteAwareTDigest.forLegacyAggregation(10);
          digest.add(nativeDigest);
        }
        SerializedTDigest result = new SerializedTDigest(digest, 50);
        assertEquals(digest.quantile(0.5), expected);
        assertEquals(BytesUtils.toBytes(result.toString()), bytes);
        assertEquals(digest.quantile(0.5), expected);
      }
    }
  }

  @Test
  public void testStoredCentroidSortPreservesPairsAndTotalOrdering() {
    double[] means = {Double.NaN, 1, 0.0, -0.0, 1, Double.NEGATIVE_INFINITY, Double.NaN, 99};
    double[] weights = {1, 2, 3, 4, 5, 6, 7, 8};
    TDigestAccumulator.sortCentroids(means, weights, 7);
    assertEquals(means, new double[]{Double.NEGATIVE_INFINITY, -0.0, 0.0, 1, 1, Double.NaN, Double.NaN, 99});
    assertEquals(weights, new double[]{6, 4, 3, 2, 5, 1, 7, 8});
  }

  @Test
  public void testPublicCompressionAndSerializationUseTheSameRawCompressionPolicy() {
    for (boolean legacyAggregation : new boolean[]{false, true}) {
      for (int count : new int[]{500, 2000}) {
        TDigestAccumulator direct = legacyAggregation
            ? TDigestAccumulator.forLegacyAggregation(100) : TDigestAccumulator.forReduction(100);
        TDigestAccumulator compressed = legacyAggregation
            ? TDigestAccumulator.forLegacyAggregation(100) : TDigestAccumulator.forReduction(100);
        SplittableRandom random = new SplittableRandom(42);
        for (int i = 0; i < count; i++) {
          double value = random.nextDouble(1000);
          direct.add(value);
          compressed.add(value);
        }
        compressed.compress();
        assertEquals(compressed.serialize(), direct.serialize());
        assertEquals(compressed.quantile(0.86), direct.quantile(0.86));
        assertEquals(compressed.quantile(0.95), direct.quantile(0.95));
      }
    }
  }

  @Test
  public void testSnapshotKeepsBufferedSourceUnchangedAndOwnsItsMutableArrays() throws Exception {
    for (boolean legacyAggregation : new boolean[]{false, true}) {
      TDigestAccumulator source = legacyAggregation
          ? TDigestAccumulator.forLegacyAggregation(100) : TDigestAccumulator.forReduction(100);
      TDigestAccumulator control = legacyAggregation
          ? TDigestAccumulator.forLegacyAggregation(100) : TDigestAccumulator.forReduction(100);
      TDigestAccumulator continuingCopy = null;
      SplittableRandom random = new SplittableRandom(7);
      for (int i = 0; i < 2000; i++) {
        double value = random.nextDouble(1000);
        source.add(value);
        control.add(value);
        if (continuingCopy != null) {
          continuingCopy.add(value);
        }
        if (i == 999) {
          source.add(10, 3.5);
          control.add(10, 3.5);
          int mergeCount = (int) field(source, "_mergeCount");
          int rawCount = (int) field(source, "_numRawValues");
          TDigestAccumulator copy = source.copy();
          assertEquals((int) field(source, "_mergeCount"), mergeCount);
          assertEquals((int) field(source, "_numRawValues"), rawCount);
          assertEquals(copy.getTotalWeight(), source.getTotalWeight());
          copy.add(-100, 10);
          copy.serialize();
          assertEquals(source.getTotalWeight(), control.getTotalWeight());
          continuingCopy = source.copy();
        }
      }
      assertEquals(source.serialize(), control.serialize());
      assertEquals(continuingCopy.serialize(), control.serialize(), "Copies must preserve future flush timing");
      assertEquals(source.quantile(0.86), control.quantile(0.86));
    }
  }

  @Test
  public void testPendingMergesReuseReaderWithoutReplacingTheIncomingInput() {
    TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
    for (double value : new double[]{1, 5, 9}) {
      digest.add(fromBytes(verbose(new double[]{value}, new double[]{1})));
    }
    assertEquals(digest.getTotalWeight(), 3.0);
    assertEquals(digest.getMin(), 1.0);
    assertEquals(digest.getMax(), 9.0);
    assertEquals(digest.quantile(0.5), 5.0);
  }

  @Test
  public void testNativeMergeConsumesItsSourceAtPublicCompression() {
    TDigestAccumulator empty = TDigestAccumulator.forReduction(100);
    empty.add(empty);
    assertTrue(empty.isEmpty());
    TDigestAccumulator fractional = TDigestAccumulator.forReduction(100);
    fractional.add(42.0, 1.5);
    fractional.add(fractional);
    assertEquals(fractional.getTotalWeight(), 3.0);
    assertEquals(fractional.quantile(0.5), 42.0);
    assertEquals(TDigestAccumulator.fromBytes(fractional.serialize()).getTotalWeight(), 3.0);

    TDigestAccumulator source = TDigestAccumulator.forLegacyAggregation(100);
    TDigestAccumulator expected = TDigestAccumulator.forLegacyAggregation(100);
    SplittableRandom random = new SplittableRandom(10);
    for (int i = 0; i < 94; i++) {
      double value = random.nextDouble() * 10_000;
      source.add(value);
      expected.add(value);
    }
    expected.compress();
    TDigestAccumulator parent = TDigestAccumulator.forLegacyAggregation(100);
    parent.add(source);
    assertEquals(source.centroids(), expected.centroids());
    assertEquals(source.quantile(0.86), expected.quantile(0.86));
    assertEquals(parent.getTotalWeight(), source.getTotalWeight());
  }

  @Test
  public void testDuplicatePlateauKeepsItsFullMassAndCdfAgreesAtItsEdges() {
    TDigestAccumulator digest = fromBytes(verbose(new double[]{0, 10, 10, 20, 30},
        new double[]{1, 2, 6, 2, 1}));
    for (double index : new double[]{1.25, 4, 8.75}) {
      assertEquals(digest.quantile(index / 12.0), 10.0);
    }
    assertEquals(digest.cdf(10), 5.0 / 12.0);
    assertEquals(digest.cdf(5), 1.0 / 12.0);
    assertEquals(digest.cdf(15), 9.5 / 12.0);
    assertEquals(digest.quantile(digest.cdf(15)), 15.0);

    TDigestAccumulator continuous = fromBytes(verbose(new double[]{0, 10, 20, 30},
        new double[]{1, 8, 2, 1}));
    assertTrue(continuous.quantile(4.0 / 12.0) < 10.0);
    assertTrue(continuous.quantile(8.75 / 12.0) > 10.0);
    assertEquals(continuous.quantile(continuous.cdf(15)), 15.0);

    TDigestAccumulator fractional = fromBytes(verbose(new double[]{0, 10, 10, 20},
        new double[]{1, 0.25, 0.25, 1}));
    assertEquals(fractional.quantile(1.1 / 2.5), 10.0);
    assertEquals(fractional.quantile(1.4 / 2.5), 10.0);

    TDigestAccumulator first = fromBytes(verbose(new double[]{0, 0, 10, 20},
        new double[]{1, 2, 3, 1}));
    assertEquals(first.quantile(2.75 / 7.0), 0.0);
    assertEquals(first.quantile(first.cdf(0)), 0.0);
    TDigestAccumulator last = fromBytes(verbose(new double[]{0, 10, 20, 20},
        new double[]{1, 3, 2, 1}));
    assertEquals(last.quantile(4.25 / 7.0), 20.0);
    assertEquals(last.quantile(last.cdf(20)), 20.0);

    TDigestAccumulator huge = fromBytes(verbose(new double[]{0, 10, 10, 20, 30},
        new double[]{1, 1e300, 1e300, 1e300, 1}));
    assertEquals(huge.quantile(0.6), 10.0);
    assertEquals(huge.quantile(0), 0.0);
    assertEquals(huge.quantile(1), 30.0);
  }

  @Test
  public void testDuplicateHeavyRawValuesPreservePlateauRanks() {
    TDigestAccumulator digest = TDigestAccumulator.forReduction(100);
    for (int i = 0; i < 10_000; i++) {
      digest.add(i < 7000 ? 0.1 : i < 9000 ? 0.5 : 0.9);
    }
    digest.serialize();
    assertEquals(digest.quantile(0.69), 0.1);
    assertEquals(digest.quantile(0.89), 0.5);
    assertEquals(digest.quantile(0.99), 0.9);
    assertTrue(digest.quantile(0.70) < 0.25, "Plateau mass must not leak into interpolation across the gap");
    assertTrue(digest.quantile(0.90) < 0.6, "The upper plateau's edge stays close to its repeated value");
    assertEquals(digest.getTotalWeight(), 10_000.0);
  }

  @Test
  public void testWeightedSortKeepsPairsAndEqualMeanOrderAcrossFlushes() {
    TDigestAccumulator digest = TDigestAccumulator.forReduction(1_000_000);
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

  private static TDigestAccumulator fromBytes(byte[] bytes) {
    TDigestAccumulator digest =
        TDigestAccumulator.forReduction(TDigestCodec.readCompression(bytes));
    digest.addSerializedTDigest(bytes);
    return digest;
  }

  private static byte[] verbose(double[] means, double[] weights) {
    ByteBuffer encoded = ByteBuffer.allocate(32 + 16 * means.length);
    encoded.putInt(TDigestCodec.VERBOSE_ENCODING);
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
