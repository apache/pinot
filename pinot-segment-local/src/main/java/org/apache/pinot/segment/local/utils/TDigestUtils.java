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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.pinot.segment.local.customobject.PercentileTDigestAccumulator;
import org.apache.pinot.segment.local.customobject.TDigest;

/// Compatibility helpers for the serialized t-digest format shared by t-digest 3.2 and 3.3.
///
/// Version 3.3 requires the first and last centroid to have unit weight whenever a digest is recompressed. Version
/// 3.2 did not maintain that invariant, so an otherwise valid 3.2 digest can fail an assertion after being read by
/// 3.3. Deserialization repairs only those legacy boundary centroids, preserving their total weight and first moment.
/// Serialization uses the compact form when 3.3's larger low-compression centroid buffer would not fit in a 3.2
/// default buffer and every centroid is exactly representable in that format. Otherwise, it merges the nearest
/// interior centroids until the verbose representation fits the 3.2 capacity. This keeps intermediate values readable
/// in both directions during a rolling upgrade without silently narrowing double-precision values to floats.
///
/// Pinot owns the in-memory K1 implementation and retains these legacy bytes for stored columns and mixed-version
/// intermediate results. Decoding validates lengths and values before retaining or allocating digest state.
public final class TDigestUtils {
  private static final int VERBOSE_ENCODING = 1;
  private static final int SMALL_ENCODING = 2;
  private static final int LEGACY_CAPACITY_PADDING = 10;
  private static final int VERBOSE_HEADER_SIZE = 32;
  private static final int VERBOSE_CENTROID_SIZE = 16;
  private static final int SMALL_HEADER_SIZE = 30;
  private static final int SMALL_CENTROID_SIZE = 8;
  private static final int DEFAULT_MERGE_BUFFER_MULTIPLIER = 5;
  private static final double MAX_COMPRESSION = 1_000_000.0;

  private TDigestUtils() {
  }

  /// Creates a digest using Pinot's accuracy-preserving K1 implementation.
  public static TDigest createMergingDigest(double compression) {
    return PercentileTDigestAccumulator.forLegacyAggregation(compression);
  }

  /// Creates Pinot's buffered digest for segment aggregation.
  public static TDigest createMergingDigestWithLegacyBuffer(double compression) {
    return createMergingDigestWithLegacyBuffer(compression, 0);
  }

  /// Creates a digest whose dynamically grown arrays can hold the requested centroid capacity.
  public static TDigest createMergingDigestWithLegacyBuffer(double compression, int minimumMainCapacity) {
    if (minimumMainCapacity < 0) {
      throw new IllegalArgumentException("Minimum main capacity must not be negative: " + minimumMainCapacity);
    }
    return createMergingDigest(compression);
  }

  /// Rejects compression values that could allocate unbounded centroid state.
  public static void validateCompression(double compression) {
    if (!(compression > 0.0) || !Double.isFinite(compression) || compression > MAX_COMPRESSION) {
      throw new IllegalArgumentException("Invalid TDigest compression: " + compression);
    }
  }

  /// Serializes a digest in a representation readable by both t-digest 3.2 and 3.3.
  public static byte[] serialize(TDigest tDigest) {
    return serialize(tDigest, null);
  }

  /// Serializes a digest using `scratchBuffer` for the temporary verbose representation when it is large enough.
  ///
  /// The returned array is independently owned. The scratch buffer is cleared and may be reused by the caller after
  /// this method returns.
  public static byte[] serialize(TDigest tDigest, ByteBuffer scratchBuffer) {
    ByteOrder scratchOrder = scratchBuffer != null ? scratchBuffer.order() : null;
    try {
      int centroidCount = tDigest.centroidCount();
      long sizeBound = Math.min(Math.max(0L, tDigest.size()),
          Math.max(getDefaultCentroidCapacity(tDigest.compression()),
              getLegacyDefaultCentroidCapacity(tDigest.compression())));
      int maxCentroids = Math.max(Math.addExact(centroidCount, 2), Math.toIntExact(sizeBound));
      int requiredCapacity = Math.addExact(VERBOSE_HEADER_SIZE,
          Math.multiplyExact(VERBOSE_CENTROID_SIZE, maxCentroids));
      ByteBuffer verboseBuffer = prepareScratchBuffer(scratchBuffer, requiredCapacity);
      tDigest.asBytes(verboseBuffer);
      byte[] verboseBytes = new byte[verboseBuffer.position()];
      verboseBuffer.flip();
      verboseBuffer.get(verboseBytes);
      return ByteBuffer.wrap(verboseBytes).getInt() == VERBOSE_ENCODING
          ? makeLegacyCompatible(verboseBytes) : verboseBytes;
    } finally {
      if (scratchBuffer != null) {
        scratchBuffer.order(scratchOrder);
      }
    }
  }

  private static ByteBuffer prepareScratchBuffer(ByteBuffer scratchBuffer, int requiredCapacity) {
    if (scratchBuffer == null || scratchBuffer.isReadOnly() || scratchBuffer.capacity() < requiredCapacity) {
      return ByteBuffer.allocate(requiredCapacity);
    }
    scratchBuffer.clear();
    scratchBuffer.limit(requiredCapacity);
    scratchBuffer.order(ByteOrder.BIG_ENDIAN);
    return scratchBuffer;
  }

  /// Converts a verbose `MergingDigest` payload to a representation readable by t-digest 3.2 and 3.3.
  ///
  /// The input must use the standard verbose `MergingDigest` encoding. The returned payload stays verbose when it
  /// already fits the 3.2 default capacity, uses the compact encoding only when every narrowed field is exact, and
  /// otherwise reduces interior centroids without narrowing doubles.
  public static byte[] makeLegacyCompatible(byte[] verboseBytes) {
    ByteBuffer verbose = ByteBuffer.wrap(verboseBytes);
    validateSerialized(verbose, false);
    if (verbose.getInt() != VERBOSE_ENCODING) {
      throw new IllegalArgumentException("Expected verbose TDigest encoding");
    }
    verbose.getDouble();
    verbose.getDouble();
    double compression = verbose.getDouble();
    validateCompression(compression);
    int centroidCount = verbose.getInt();
    if (centroidCount < 0 || centroidCount > verbose.remaining() / VERBOSE_CENTROID_SIZE) {
      throw new IllegalArgumentException("Invalid TDigest centroid count: " + centroidCount);
    }

    int mainCapacity = Math.max(getDefaultCentroidCapacity(compression), centroidCount);
    long bufferCapacity = DEFAULT_MERGE_BUFFER_MULTIPLIER * (long) mainCapacity;
    int legacyCapacity = getLegacyDefaultCentroidCapacity(compression);
    boolean compactFieldsExact = true;
    ByteBuffer centroids = verbose.duplicate();
    for (int i = 0; i < centroidCount; i++) {
      double weight = centroids.getDouble();
      double mean = centroids.getDouble();
      if (!(weight > 0.0) || !Double.isFinite(weight) || Double.isNaN(mean)) {
        throw new IllegalArgumentException("Invalid TDigest centroid: mean=" + mean + ", weight=" + weight);
      }
      compactFieldsExact &= (double) (float) weight == weight && (double) (float) mean == mean;
    }
    if (centroidCount <= legacyCapacity) {
      return verboseBytes;
    }
    if ((double) (float) compression != compression || centroidCount > Short.MAX_VALUE
        || mainCapacity > Short.MAX_VALUE || bufferCapacity > Short.MAX_VALUE || !compactFieldsExact) {
      return reduceVerboseCentroids(verboseBytes, legacyCapacity);
    }

    ByteBuffer small = ByteBuffer.allocate(SMALL_HEADER_SIZE + SMALL_CENTROID_SIZE * centroidCount);
    verbose.rewind();
    verbose.getInt();
    small.putInt(SMALL_ENCODING);
    small.putDouble(verbose.getDouble());
    small.putDouble(verbose.getDouble());
    verbose.getDouble();
    verbose.getInt();
    small.putFloat((float) compression);
    small.putShort((short) mainCapacity);
    small.putShort((short) bufferCapacity);
    small.putShort((short) centroidCount);
    for (int i = 0; i < centroidCount; i++) {
      small.putFloat((float) verbose.getDouble());
      small.putFloat((float) verbose.getDouble());
    }
    return small.array();
  }

  /// Deserializes a digest and repairs boundary centroids produced by t-digest 3.2 when necessary.
  public static TDigest deserialize(byte[] bytes) {
    return deserialize(ByteBuffer.wrap(bytes));
  }

  /// Validates serialized lengths and centroid values without allocating centroid arrays.
  public static double validateSerialized(byte[] bytes) {
    return validateSerialized(ByteBuffer.wrap(bytes));
  }

  /// Validates one legacy digest without changing `input` and returns its total centroid weight.
  ///
  /// Trailing bytes remain available to the caller for intermediate values containing several objects.
  public static double validateSerialized(ByteBuffer input) {
    return validateSerialized(input, true);
  }

  private static double validateSerialized(ByteBuffer input, boolean checkCapacity) {
    ByteBuffer encoded = input.slice().order(ByteOrder.BIG_ENDIAN);
    int encoding = encoded.getInt();
    if (encoding != VERBOSE_ENCODING && encoding != SMALL_ENCODING) {
      throw new IllegalStateException("Invalid format for serialized histogram");
    }
    double min = encoded.getDouble();
    double max = encoded.getDouble();
    double compression;
    int mainCapacity;
    int centroidCount;
    int centroidSize;
    if (encoding == VERBOSE_ENCODING) {
      compression = encoded.getDouble();
      validateCompression(compression);
      mainCapacity = Math.max(getDefaultCentroidCapacity(compression), getLegacyDefaultCentroidCapacity(compression));
      centroidCount = encoded.getInt();
      centroidSize = VERBOSE_CENTROID_SIZE;
    } else {
      compression = encoded.getFloat();
      validateCompression(compression);
      mainCapacity = encoded.getShort();
      int bufferCapacity = encoded.getShort();
      checkSmallArrayCapacity(mainCapacity);
      checkSmallArrayCapacity(bufferCapacity);
      if (mainCapacity == -1) {
        mainCapacity = getDefaultCentroidCapacity(compression);
      }
      centroidCount = encoded.getShort();
      centroidSize = SMALL_CENTROID_SIZE;
    }
    if (centroidCount < 0) {
      throw new IllegalArgumentException("Invalid negative TDigest centroid count: " + centroidCount);
    }
    if (centroidCount > encoded.remaining() / centroidSize) {
      throw new BufferUnderflowException();
    }
    if (checkCapacity && centroidCount > mainCapacity) {
      throw new IllegalArgumentException("TDigest centroid count exceeds capacity: " + centroidCount);
    }
    if (Double.isNaN(min) || Double.isNaN(max) || (centroidCount > 0 && min > max)) {
      throw new IllegalArgumentException("Invalid TDigest extrema: " + min + ", " + max);
    }
    double totalWeight = 0.0;
    double previousMean = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < centroidCount; i++) {
      double weight = centroidSize == VERBOSE_CENTROID_SIZE ? encoded.getDouble() : encoded.getFloat();
      double mean = centroidSize == VERBOSE_CENTROID_SIZE ? encoded.getDouble() : encoded.getFloat();
      if (!(weight > 0.0) || !Double.isFinite(weight) || Double.isNaN(mean) || mean < previousMean) {
        throw new IllegalArgumentException("Invalid TDigest centroid: mean=" + mean + ", weight=" + weight);
      }
      totalWeight += weight;
      if (!Double.isFinite(totalWeight) || totalWeight >= 0x1p63) {
        throw new IllegalArgumentException("TDigest total weight exceeds the supported range");
      }
      previousMean = mean;
    }
    return totalWeight;
  }

  /// Deserializes a finite-valued digest into Pinot's buffered implementation.
  public static TDigest deserializeFiniteWithLegacyBuffer(byte[] bytes) {
    validateSerialized(bytes);
    ByteBuffer input = ByteBuffer.wrap(bytes);
    int encoding = input.getInt();
    input.position(Integer.BYTES + 2 * Double.BYTES);
    int count;
    boolean verbose = encoding == VERBOSE_ENCODING;
    if (verbose) {
      input.getDouble();
      count = input.getInt();
    } else {
      input.getFloat();
      input.getShort();
      input.getShort();
      count = input.getShort();
    }
    for (int i = 0; i < count; i++) {
      if (verbose) {
        input.getDouble();
      } else {
        input.getFloat();
      }
      double mean = verbose ? input.getDouble() : input.getFloat();
      if (!Double.isFinite(mean)) {
        throw new IllegalArgumentException("Expected a finite TDigest centroid mean: " + mean);
      }
    }
    TDigest digest = deserialize(bytes);
    digest.compress();
    return digest;
  }

  /// Deserializes a digest and advances `input` past its encoded bytes.
  public static TDigest deserialize(ByteBuffer input) {
    validateSerialized(input);
    ByteBuffer encoded = input.slice().order(ByteOrder.BIG_ENDIAN);
    int encoding = encoded.getInt();
    encoded.position(Integer.BYTES + 2 * Double.BYTES);
    double compression;
    int count;
    int encodedLength;
    if (encoding == VERBOSE_ENCODING) {
      compression = encoded.getDouble();
      count = encoded.getInt();
      encodedLength = VERBOSE_HEADER_SIZE + count * VERBOSE_CENTROID_SIZE;
    } else {
      compression = encoded.getFloat();
      encoded.getShort();
      encoded.getShort();
      count = encoded.getShort();
      encodedLength = SMALL_HEADER_SIZE + count * SMALL_CENTROID_SIZE;
    }
    byte[] bytes = new byte[encodedLength];
    encoded.rewind();
    encoded.get(bytes);
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forLegacyAggregation(compression);
    digest.addSerializedTDigest(bytes);
    input.position(Math.addExact(input.position(), encodedLength));
    return digest;
  }

  private static void checkSmallArrayCapacity(int capacity) {
    if (capacity < -1) {
      throw new IllegalArgumentException("Invalid TDigest array capacity: " + capacity);
    }
  }

  private static byte[] reduceVerboseCentroids(byte[] verboseBytes, int maxCentroids) {
    ByteBuffer input = ByteBuffer.wrap(verboseBytes);
    input.getInt();
    double min = input.getDouble();
    double max = input.getDouble();
    double compression = input.getDouble();
    int centroidCount = input.getInt();
    double[] weights = new double[centroidCount];
    double[] means = new double[centroidCount];
    for (int i = 0; i < centroidCount; i++) {
      weights[i] = input.getDouble();
      means[i] = input.getDouble();
    }

    while (centroidCount > maxCentroids) {
      int mergeIndex = findNearestInteriorCentroids(means, weights, centroidCount);
      double combinedWeight = weights[mergeIndex] + weights[mergeIndex + 1];
      means[mergeIndex] = weightedMean(means[mergeIndex], weights[mergeIndex], means[mergeIndex + 1],
          weights[mergeIndex + 1], combinedWeight);
      weights[mergeIndex] = combinedWeight;
      int numMoved = centroidCount - mergeIndex - 2;
      if (numMoved > 0) {
        System.arraycopy(means, mergeIndex + 2, means, mergeIndex + 1, numMoved);
        System.arraycopy(weights, mergeIndex + 2, weights, mergeIndex + 1, numMoved);
      }
      centroidCount--;
    }

    ByteBuffer reduced = ByteBuffer.allocate(VERBOSE_HEADER_SIZE + VERBOSE_CENTROID_SIZE * centroidCount);
    reduced.putInt(VERBOSE_ENCODING);
    reduced.putDouble(min);
    reduced.putDouble(max);
    reduced.putDouble(compression);
    reduced.putInt(centroidCount);
    for (int i = 0; i < centroidCount; i++) {
      reduced.putDouble(weights[i]);
      reduced.putDouble(means[i]);
    }
    return reduced.array();
  }

  private static int findNearestInteriorCentroids(double[] means, double[] weights, int centroidCount) {
    int firstFiniteIndex = -1;
    int lastFiniteIndex = -1;
    for (int i = 0; i < centroidCount; i++) {
      if (Double.isFinite(means[i])) {
        if (firstFiniteIndex == -1) {
          firstFiniteIndex = i;
        }
        lastFiniteIndex = i;
      }
    }

    int nearestIndex = -1;
    double nearestDistance = Double.POSITIVE_INFINITY;
    double nearestWeight = Double.POSITIVE_INFINITY;
    for (int i = 1; i < centroidCount - 2; i++) {
      if (i == firstFiniteIndex || i + 1 == firstFiniteIndex
          || i == lastFiniteIndex || i + 1 == lastFiniteIndex) {
        continue;
      }
      double distance = centroidDistance(means[i], means[i + 1]);
      double combinedWeight = weights[i] + weights[i + 1];
      if (distance < nearestDistance || (distance == nearestDistance && combinedWeight < nearestWeight)) {
        nearestIndex = i;
        nearestDistance = distance;
        nearestWeight = combinedWeight;
      }
    }
    if (nearestIndex == -1) {
      throw new IllegalStateException("Cannot reduce TDigest centroids without merging an endpoint");
    }
    return nearestIndex;
  }

  private static double centroidDistance(double first, double second) {
    return first == second ? 0.0 : Math.abs(second - first);
  }

  private static double weightedMean(double firstMean, double firstWeight, double secondMean,
      double secondWeight, double totalWeight) {
    if (firstMean == secondMean) {
      return firstMean;
    }
    if (!Double.isFinite(firstMean) || !Double.isFinite(secondMean)) {
      if (firstMean == Double.NEGATIVE_INFINITY || secondMean == Double.NEGATIVE_INFINITY) {
        return Double.NEGATIVE_INFINITY;
      }
      return Double.POSITIVE_INFINITY;
    }
    if (Math.copySign(1.0, firstMean) == Math.copySign(1.0, secondMean)) {
      return firstMean + (secondMean - firstMean) * secondWeight / totalWeight;
    }
    return firstMean * (firstWeight / totalWeight) + secondMean * (secondWeight / totalWeight);
  }

  private static int getLegacyDefaultCentroidCapacity(double compression) {
    return Math.addExact(Math.multiplyExact(2, (int) Math.ceil(compression)), LEGACY_CAPACITY_PADDING);
  }

  private static int getDefaultCentroidCapacity(double compression) {
    double normalizedCompression = Math.max(compression, 10.0);
    int padding = normalizedCompression < 30.0 ? 30 : 10;
    return (int) Math.ceil(2.0 * normalizedCompression + padding);
  }
}
