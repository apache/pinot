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

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.ScaleFunction;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TDigestUtilsTest {
  private static final int VERBOSE_ENCODING = 1;
  private static final int SMALL_ENCODING = 2;
  private static final int VERBOSE_HEADER_SIZE = 32;

  @Test
  public void testPinotMergingDigestUsesAccuracyPreservingScaleFunction() {
    MergingDigest digest = (MergingDigest) TDigestUtils.createMergingDigest(100.0);
    assertEquals(digest.getScaleFunction(), ScaleFunction.K_1);
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }

    MergingDigest roundTripped = (MergingDigest) TDigestUtils.deserialize(TDigestUtils.serialize(digest));
    assertEquals(roundTripped.getScaleFunction(), ScaleFunction.K_1);
    assertEquals(roundTripped.size(), digest.size());
  }

  @Test
  public void testLegacyBufferFactoryLimitsHighCardinalityState() {
    MergingDigest digest = (MergingDigest) TDigestUtils.createMergingDigestWithLegacyBuffer(100.0);
    assertEquals(digest.getScaleFunction(), ScaleFunction.K_1);
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }

    ByteBuffer small = ByteBuffer.allocate(4_096);
    digest.asSmallBytes(small);
    small.flip();
    assertEquals(small.getInt(), SMALL_ENCODING);
    small.position(Integer.BYTES + 2 * Double.BYTES + Float.BYTES);
    assertEquals(small.getShort(), 210, "The main buffer must retain the t-digest 3.3 capacity");
    assertEquals(small.getShort(), 500, "The temporary buffer must retain the t-digest 3.2 footprint");
  }

  @Test
  public void testFiniteDeserializationRetainsLegacyBufferFootprint() {
    TDigest source = TDigestUtils.createMergingDigest(100.0);
    for (int i = 0; i < 1_000; i++) {
      source.add(i);
    }

    MergingDigest digest = (MergingDigest) TDigestUtils.deserializeFiniteWithLegacyBuffer(
        TDigestUtils.serialize(source));
    ByteBuffer small = ByteBuffer.allocate(4_096);
    digest.asSmallBytes(small);
    small.flip();
    assertEquals(small.getInt(), SMALL_ENCODING);
    small.position(Integer.BYTES + 2 * Double.BYTES + Float.BYTES);
    assertEquals(small.getShort(), 210);
    assertEquals(small.getShort(), 500);
    assertEquals(digest.size(), source.size());
  }

  @Test
  public void testSerializeReusesCallerScratchWithoutSharingOutput() {
    TDigest digest = TDigestUtils.createMergingDigestWithLegacyBuffer(100.0);
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }
    ByteBuffer scratch = ByteBuffer.allocate(4_096);

    byte[] bytes = TDigestUtils.serialize(digest, scratch);
    assertTrue(scratch.position() >= bytes.length);
    scratch.put(0, (byte) 0);
    assertEquals(ByteBuffer.wrap(bytes).getInt(), VERBOSE_ENCODING);
    assertEquals(TDigestUtils.deserialize(bytes).size(), digest.size());
  }

  @Test
  public void testSerializeUsesNetworkOrderWithoutChangingScratchOrder() {
    TDigest digest = TDigestUtils.createMergingDigestWithLegacyBuffer(100.0);
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
    AtomicInteger compressionCount = new AtomicInteger();
    MergingDigest digest = new MergingDigest(100.0) {
      @Override
      public void compress() {
        compressionCount.incrementAndGet();
        super.compress();
      }
    };
    for (int i = 0; i < 1_000; i++) {
      digest.add(i);
    }

    TDigestUtils.serialize(digest);
    assertEquals(compressionCount.get(), 1,
        "Sizing the output must not invoke the final destructive compression pass");
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

    assertEquals(digest.centroidCount(), 211, "The payload must fit the t-digest 3.3 default main array");
    List<Centroid> centroids = List.copyOf(digest.centroids());
    assertTrue(centroids.size() <= 211);
    assertEquals(centroids.get(0).mean(), means[0]);
    assertEquals(centroids.get(0).count(), 1L);
    assertEquals(centroids.get(centroids.size() - 1).mean(), means[centroidCount - 1]);
    assertEquals(centroids.get(centroids.size() - 1).count(), 1L);
    double actualWeight = 0.0;
    double actualFirstMoment = 0.0;
    for (Centroid centroid : centroids) {
      actualWeight += centroid.count();
      actualFirstMoment += centroid.mean() * centroid.count();
    }
    assertEquals(actualWeight, expectedWeight);
    assertEquals(actualFirstMoment, expectedFirstMoment, Math.ulp(expectedFirstMoment) * 16.0);

    ByteBuffer roundTripped = ByteBuffer.wrap(TDigestUtils.serialize(digest));
    assertEquals(roundTripped.getInt(), VERBOSE_ENCODING,
        "Non-float-exact compression must remain in the double-precision wire format");
    roundTripped.position(Integer.BYTES + 2 * Double.BYTES);
    assertEquals(roundTripped.getDouble(), compression);
    assertTrue(roundTripped.getInt() <= centroidCount,
        "The result must remain within the t-digest 3.2 verbose capacity");
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
      actualWeight += centroid.count();
      actualFirstMoment += centroid.mean() * centroid.count();
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
      assertTrue(centroid.count() > 0);
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

  private static MergingDigest craftedVerboseDigest(double compression, double[] means, double[] weights) {
    return new MergingDigest(compression) {
      @Override
      public long size() {
        return means.length;
      }

      @Override
      public double compression() {
        return compression;
      }

      @Override
      public int centroidCount() {
        return means.length;
      }

      @Override
      public void asBytes(ByteBuffer buffer) {
        buffer.putInt(VERBOSE_ENCODING);
        buffer.putDouble(means[0]);
        buffer.putDouble(means[means.length - 1]);
        buffer.putDouble(compression);
        buffer.putInt(means.length);
        for (int i = 0; i < means.length; i++) {
          buffer.putDouble(weights[i]);
          buffer.putDouble(means[i]);
        }
      }
    };
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
