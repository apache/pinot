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
import org.apache.pinot.segment.local.customobject.TDigest.Centroid;
import org.apache.pinot.segment.local.utils.TDigestUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/// Regression tests for opaque legacy payloads, fractional statistics, and primitive centroid merging.
public class PercentileTDigestAccumulatorTest {
  @Test
  public void testHistoricalCorruptionRemainsOpaqueAndRejectsMutation() {
    for (byte[] poison : new byte[][]{verbose(new double[]{1, 2, 3}, new double[]{1, -0.5, 1}),
        verbose(new double[]{1, 2}, new double[]{1, -200}),
        verbose(new double[]{Double.NaN}, new double[]{1}),
        verbose(new double[]{1, 3}, new double[]{3, -3})}) {
      PercentileTDigestAccumulator digest = fromBytes(poison);
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
      ByteBuffer small = ByteBuffer.allocate(digest.smallByteSize());
      digest.asSmallBytes(small);
      assertEquals(small.array(), poison);

      PercentileTDigestAccumulator copy = PercentileTDigestAccumulator.forReduction(100);
      copy.add(digest);
      assertEquals(copy.serialize(), poison);
      PercentileTDigestAccumulator healthy = PercentileTDigestAccumulator.forReduction(100);
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
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(100);
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
    expectThrows(IllegalArgumentException.class, digest::smallByteSize);

    TDigest wideBounds = TDigestUtils.deserialize(TDigestUtils.serializeCentroids(100, 0, 3,
        new double[]{1, 2}, new double[]{0.3, 0.3}, 2));
    assertEquals(wideBounds.quantile(0.25), 1.0);
    assertEquals(wideBounds.quantile(0.75), 2.0);
    assertEquals(wideBounds.cdf(2.0), 0.75);


    for (byte[] historical : new byte[][]{verbose(new double[]{1, 2}, new double[]{0.3, 0.3}),
        verbose(new double[]{42}, new double[]{1.5}),
        TDigestUtils.serializeCentroids(100, 0, 3, new double[]{1, 0, 2, 3}, new double[]{1, 0.3, 1, 1}, 4)}) {
      PercentileTDigestAccumulator stored = fromBytes(historical);
      assertEquals(stored.serialize(), historical);
      stored.quantile(0.5);
      stored.centroids();
      assertEquals(stored.serialize(), historical, "Read-only materialization preserves unsupported original bytes");
      for (double compression : new double[]{stored.compression(), 500}) {
        PercentileTDigestAccumulator copy = PercentileTDigestAccumulator.forReduction(compression);
        copy.add(stored);
        assertEquals(copy.serialize(), historical, "Copying a queried source retains its original encoding");
        copy.quantile(0.5);
        copy.compress();
        assertEquals(copy.serialize(), historical,
            "Configured compression cannot rewrite an unchanged fractional input");
        PercentileTDigestAccumulator adopted = PercentileTDigestAccumulator.forReduction(compression);
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
    PercentileTDigestAccumulator singleton = PercentileTDigestAccumulator.forReduction(100);
    singleton.add(42, 1.5);
    assertEquals(singleton.quantile(1), 42.0);
    expectThrows(IllegalArgumentException.class, singleton::serialize);
    singleton = fromBytes(verbose(new double[]{42}, new double[]{1.5}));
    singleton.add(1);
    assertFalse(singleton.hasOriginalFractionalPayload());
    assertEquals(TDigestUtils.deserialize(singleton.serialize()).getTotalWeight(), 2.5);
  }

  @Test
  public void testMixedHistoricalFractionalBoundariesPreserveMassAndFiniteStatistics() {
    byte[] historical = verbose(new double[]{0, 5, 10}, new double[]{0.5, 4, 1});
    for (boolean historyFirst : new boolean[]{false, true}) {
      for (boolean direct : new boolean[]{false, true}) {
        PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(200);
        if (!historyFirst) {
          digest.add(7);
        }
        if (direct) {
          var input = new PercentileTDigestAccumulator.SerializedTDigestInput();
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
        assertEquals(ByteBuffer.wrap(bytes).getInt(0), TDigestUtils.VERBOSE_ENCODING);
        assertEquals(ByteBuffer.wrap(bytes).getDouble(32), 0.5);
        PercentileTDigestAccumulator restored = fromBytes(bytes);
        assertEquals(restored.getTotalWeight(), 6.5);
        assertEquals(restored.quantile(0.5), median);
        assertEquals(restored.getMin(), 0.0);
        assertEquals(restored.getMax(), 10.0);
        ByteBuffer small = ByteBuffer.allocate(digest.smallByteSize());
        digest.asSmallBytes(small);
        assertEquals(small.getInt(0), TDigestUtils.VERBOSE_ENCODING);
        assertEquals(fromBytes(small.array()).getTotalWeight(), 6.5);

        PercentileTDigestAccumulator copy = PercentileTDigestAccumulator.forReduction(500);
        copy.add(digest);
        assertEquals(fromBytes(copy.serialize()).getTotalWeight(), 6.5);
        // Historical provenance may not authorize a newly added unsupported global endpoint.
        copy.add(-1, 0.1);
        expectThrows(IllegalArgumentException.class, copy::serialize);
        expectThrows(IllegalArgumentException.class, copy::smallByteSize);
      }
    }
    for (boolean historyFirst : new boolean[]{false, true}) {
      PercentileTDigestAccumulator fractional = PercentileTDigestAccumulator.forReduction(100);
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
        PercentileTDigestAccumulator representable = fromBytes(historical);
        representable.add(newExtrema ? -1 : 0);
        ByteBuffer bytes = ByteBuffer.wrap(representable.serialize());
        assertEquals(bytes.getDouble(32), 1.0, "Representable historical boundaries use the normal unit repair");
        assertEquals(fromBytes(bytes.array()).getTotalWeight(), 6.5);
      }
    }
    PercentileTDigestAccumulator fresh = PercentileTDigestAccumulator.forReduction(100);
    fresh.add(0, 0.5);
    fresh.add(10);
    expectThrows(IllegalArgumentException.class, fresh::serialize);
  }

  @Test
  public void testQueryCompressionRetainsTheUpstreamPositiveGuard() {
    expectThrows(IllegalArgumentException.class, () -> new PercentileTDigestAccumulator(0));
    expectThrows(IllegalArgumentException.class, () -> new PercentileTDigestAccumulator(-5));
    expectThrows(IllegalArgumentException.class, () -> PercentileTDigestAccumulator.forReduction(0));
    byte[] historical = verbose(new double[]{1}, new double[]{1});
    ByteBuffer.wrap(historical).putDouble(20, -5);
    assertEquals(fromBytes(historical).compression(), 10.0);
  }

  @Test
  public void testBulkCentroidsSortWithoutChangingCallerArrays() {
    double[] means = {20, Double.NaN, 10, 30};
    double[] weights = {2, 0, 1, 1};
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forLegacyAggregation(100);
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
    PercentileTDigestAccumulator pending = PercentileTDigestAccumulator.forReduction(100);
    pending.addSerializedTDigest(TDigestUtils.serializeCentroids(100, 1, 9,
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
  }

  @Test
  public void testByteSizeCachesOneWriteAndMutationInvalidatesIt() throws Exception {
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(100);
    digest.add(10);
    int size = digest.byteSize();
    byte[] cached = (byte[]) field(digest, "_serializedBytesForWrite");
    assertEquals(digest.byteSize(), size);
    assertTrue(field(digest, "_serializedBytesForWrite") == cached);
    ByteBuffer output = ByteBuffer.allocate(size);
    digest.asBytes(output);
    assertEquals(output.array(), cached);
    assertEquals(field(digest, "_serializedBytesForWrite"), null);
    digest.byteSize();
    digest.add(20);
    assertEquals(field(digest, "_serializedBytesForWrite"), null);
    ByteBuffer changed = ByteBuffer.allocate(digest.byteSize());
    digest.asBytes(changed);
    assertEquals(TDigestUtils.deserialize(changed.array()).getTotalWeight(), 2.0);
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
    byte[] poison = verbose(new double[]{Double.NaN}, new double[]{1});
    for (boolean emptyFirst : new boolean[]{false, true}) {
      for (boolean direct : new boolean[]{false, true}) {
        PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(100);
        for (byte[] bytes : emptyFirst ? new byte[][]{empty, poison} : new byte[][]{poison, empty}) {
          if (direct) {
            PercentileTDigestAccumulator.SerializedTDigestInput input =
                new PercentileTDigestAccumulator.SerializedTDigestInput();
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
  public void testSmallEncodingRetainsFieldsThatOverflowOrUnderflowFloat() {
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
    PercentileTDigestAccumulator tiny = PercentileTDigestAccumulator.forReduction(100);
    tiny.add(0);
    tiny.add(1, 1e-46);
    tiny.add(2);
    ByteBuffer encoded = ByteBuffer.allocate(tiny.smallByteSize());
    tiny.asSmallBytes(encoded);
    assertEquals(encoded.getInt(0), TDigestUtils.VERBOSE_ENCODING);
    TDigest restored = TDigestUtils.deserialize(encoded.array());
    assertEquals(List.copyOf(restored.centroids()).get(1).weight(), 1e-46,
        "A positive interior mass must not narrow to compact zero");
  }

  @Test
  public void testCompactCapacityOnlyPreservesDeclaredSmallHeaders() {
    byte[] verbose = TDigestUtils.serializeCentroids(1000, 0, 2,
        new double[]{0, 1, 2}, new double[]{1, 1, 1}, 3);
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forReduction(100);
    digest.addSerializedTDigest(verbose);
    ByteBuffer compact = ByteBuffer.allocate(digest.smallByteSize());
    digest.asSmallBytes(compact);
    assertEquals((int) compact.getShort(24), TDigestUtils.getDefaultCentroidCapacity(100));
    assertEquals((int) compact.getShort(26), 5 * TDigestUtils.getDefaultCentroidCapacity(100));

    ByteBuffer declared = ByteBuffer.allocate(30 + 8 * 3);
    declared.putInt(2).putDouble(0).putDouble(2).putFloat(1000);
    declared.putShort((short) 4096).putShort((short) 8192).putShort((short) 3);
    for (int i = 0; i < 3; i++) {
      declared.putFloat(1).putFloat(i);
    }
    digest = PercentileTDigestAccumulator.forReduction(100);
    digest.addSerializedTDigest(declared.array());
    compact = ByteBuffer.allocate(digest.smallByteSize());
    digest.asSmallBytes(compact);
    assertEquals((int) compact.getShort(24), 4096);
    assertEquals((int) compact.getShort(26), 5 * 4096);
    assertEquals(TDigestUtils.deserialize(compact.array()).getTotalWeight(), 3.0);
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
    for (double compression : new double[]{100, 10_000}) {
      int capacity = 5 * TDigestUtils.getDefaultCentroidCapacity(compression);
      PercentileTDigestAccumulator legacy = PercentileTDigestAccumulator.forLegacyAggregation(compression);
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
    PercentileTDigestAccumulator extreme = PercentileTDigestAccumulator.forLegacyAggregation(Double.MAX_VALUE);
    extreme.add(new double[10_001], 0, 10_001);
    assertEquals((int) field(extreme, "_mergeCount"), 0);
    assertEquals(((double[]) field(extreme, "_rawValues")).length, 20_000);
    assertEquals(extreme.getTotalWeight(), 10_001.0);
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
  public void testNativeMergeConsumesItsSourceAtPublicCompression() {
    PercentileTDigestAccumulator empty = PercentileTDigestAccumulator.forReduction(100);
    empty.add(empty);
    assertTrue(empty.isEmpty());
    PercentileTDigestAccumulator fractional = PercentileTDigestAccumulator.forReduction(100);
    fractional.add(42.0, 1.5);
    fractional.add(fractional);
    assertEquals(fractional.getTotalWeight(), 3.0);
    assertEquals(fractional.quantile(0.5), 42.0);
    assertEquals(TDigestUtils.deserialize(fractional.serialize()).getTotalWeight(), 3.0);

    PercentileTDigestAccumulator source = PercentileTDigestAccumulator.forLegacyAggregation(100);
    PercentileTDigestAccumulator expected = PercentileTDigestAccumulator.forLegacyAggregation(100);
    SplittableRandom random = new SplittableRandom(10);
    for (int i = 0; i < 94; i++) {
      double value = random.nextDouble() * 10_000;
      source.add(value);
      expected.add(value);
    }
    expected.compress();
    PercentileTDigestAccumulator parent = PercentileTDigestAccumulator.forLegacyAggregation(100);
    parent.add(source);
    assertEquals(source.centroids(), expected.centroids());
    assertEquals(source.quantile(0.86), expected.quantile(0.86));
    assertEquals(parent.getTotalWeight(), source.getTotalWeight());
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
