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
import org.apache.pinot.segment.local.customobject.PercentileTDigestAccumulator.SerializedTDigestInput;
import org.apache.pinot.segment.spi.customobject.TDigest;

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
/// intermediate results. Decoding checks lengths and numerical state before allocating centroid arrays. Historical
/// poisoned states remain byte-readable with NaN statistics because their original distribution cannot be recovered.
/// Fractional boundary masses below one, and single-centroid masses between one and two, are retained exactly:
/// t-digest 3.3's recompression assertion cannot be satisfied for these inputs without changing their mass.
public final class TDigestUtils {
  public static final int VERBOSE_ENCODING = 1;
  public static final int SMALL_ENCODING = 2;
  private static final int LEGACY_CAPACITY_PADDING = 10;
  public static final int VERBOSE_HEADER_SIZE = 32;
  public static final int VERBOSE_CENTROID_SIZE = 16;
  public static final int SMALL_HEADER_SIZE = 30;
  public static final int SMALL_CENTROID_SIZE = 8;
  public static final int DEFAULT_MERGE_BUFFER_MULTIPLIER = 5;

  private TDigestUtils() {
  }

  /// Creates a digest using Pinot's accuracy-preserving K1 implementation.
  public static TDigest createMergingDigest(double compression) {
    return PercentileTDigestAccumulator.forLegacyAggregation(compression);
  }

  /// Rejects non-finite compression while retaining the legacy clamping of finite settings below ten.
  public static void validateCompression(double compression) {
    if (!Double.isFinite(compression)) {
      throw new IllegalArgumentException("Invalid TDigest compression: " + compression);
    }
  }

  /// Applies the legacy minimum compression to query, ingestion and serialized-header settings.
  public static double normalizeCompression(double compression) {
    validateCompression(compression);
    return Math.max(compression, 10.0);
  }

  /// Serializes a digest in a representation readable by both t-digest 3.2 and 3.3.
  public static byte[] serialize(TDigest tDigest) {
    return serialize(tDigest, null);
  }

  /// Serializes a digest using `scratchBuffer` for the temporary verbose representation when it is large enough.
  ///
  /// The returned array is independently owned. Pinot-owned digests do not use the scratch buffer; callers can
  /// clear and reuse it after serialization.
  public static byte[] serialize(TDigest tDigest, ByteBuffer scratchBuffer) {
    ByteOrder scratchOrder = scratchBuffer != null ? scratchBuffer.order() : null;
    try {
      if (tDigest instanceof PercentileTDigestAccumulator) {
        // Preserve the legacy two-level raw flush before final compression. Pending serialized state answers
        // this count from cached metadata, so stored reads do not acquire an extra lossy compression pass.
        tDigest.centroidCount();
        return ((PercentileTDigestAccumulator) tDigest).serialize();
      }
      // centroidCount flushes buffered input. Boundary repair can add at most two centroids; compression and
      // total weight are not allocation sizes, so extreme historical settings need no enormous scratch array.
      int maxCentroids = Math.addExact(tDigest.centroidCount(), 2);
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
    return makeLegacyCompatible(verboseBytes, inspectSerialized(ByteBuffer.wrap(verboseBytes), false));
  }

  /// Serializes an unchanged, independently retained payload using metadata validated at ingestion.
  ///
  /// The metadata must describe these exact bytes. This avoids another centroid validation walk for lazy digests.
  public static byte[] makeLegacyCompatible(byte[] verboseBytes, SerializedTDigestMetadata metadata) {
    if (metadata.encoding() != VERBOSE_ENCODING) {
      throw new IllegalArgumentException("Expected verbose TDigest encoding");
    }
    double compression = metadata.compression();
    int centroidCount = metadata.centroidCount();
    int legacyCapacity = getLegacyDefaultCentroidCapacity(compression);
    if (centroidCount <= legacyCapacity || metadata.needsLegacyFallback()) {
      return verboseBytes;
    }
    int mainCapacity = Math.max(getDefaultCentroidCapacity(compression), centroidCount);
    long bufferCapacity = DEFAULT_MERGE_BUFFER_MULTIPLIER * (long) mainCapacity;
    if ((double) (float) compression != compression || centroidCount > Short.MAX_VALUE
        || mainCapacity > Short.MAX_VALUE || bufferCapacity > Short.MAX_VALUE) {
      return reduceVerboseCentroids(verboseBytes, legacyCapacity);
    }
    ByteBuffer verbose = ByteBuffer.wrap(verboseBytes);
    ByteBuffer centroids = verbose.duplicate();
    centroids.position(metadata.centroidOffset());
    for (int i = 0; i < centroidCount; i++) {
      double weight = centroids.getDouble();
      double mean = centroids.getDouble();
      if ((double) (float) weight != weight || (double) (float) mean != mean) {
        return reduceVerboseCentroids(verboseBytes, legacyCapacity);
      }
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
    return inspectSerialized(input, checkCapacity).totalWeight();
  }

  /// Header and numerical metadata for one legacy byte payload. The centroid bytes remain owned by the caller.
  /// Header-only inspection leaves total weight as NaN until centroid inspection validates the numerical fields.
  ///
  /// Legacy writers can emit poisoned numerical state after overflow or broken fractional boundary repair.
  /// Such state is retained with degraded quantiles, rather than being passed to the sorted K1 kernel.
  public record SerializedTDigestMetadata(int encoding, double min, double max, double compression,
      int centroidCount, int mainCapacity, int bufferCapacity, int centroidOffset, int centroidSize,
      int encodedLength, double totalWeight, boolean hasNonFiniteMeans, boolean needsLegacyFallback,
      boolean unorderedMeans, boolean fractionalWeights) {
  }

  /// Inspects one payload without mutating `input`, scanning the centroid values once.
  public static SerializedTDigestMetadata inspectSerialized(ByteBuffer input) {
    return inspectSerialized(input, true);
  }

  /// Inspects a payload, optionally enforcing the legacy decoder's declared centroid capacity.
  public static SerializedTDigestMetadata inspectSerialized(ByteBuffer input, boolean checkCapacity) {
    return inspectSerialized(input, readSerializedHeader(input, checkCapacity), null, null);
  }

  /// Reads and validates the fixed-size header without scanning or allocating centroid arrays.
  public static SerializedTDigestMetadata readSerializedHeader(ByteBuffer input) {
    return readSerializedHeader(input, true);
  }

  /// Reads a fixed-size header, optionally enforcing the declared centroid capacity.
  ///
  /// Numerical flags and total weight are computed by the subsequent inspection or decoding pass.
  public static SerializedTDigestMetadata readSerializedHeader(ByteBuffer input, boolean checkCapacity) {
    ByteBuffer encoded = input.slice().order(ByteOrder.BIG_ENDIAN);
    int encoding = encoded.getInt();
    if (encoding != VERBOSE_ENCODING && encoding != SMALL_ENCODING) {
      throw new IllegalStateException("Invalid format for serialized histogram");
    }
    double min = encoded.getDouble();
    double max = encoded.getDouble();
    double compression;
    int mainCapacity;
    int bufferCapacity = 0;
    int centroidCount;
    int centroidSize;
    if (encoding == VERBOSE_ENCODING) {
      compression = normalizeCompression(encoded.getDouble());
      mainCapacity = Math.max(getDefaultCentroidCapacity(compression), getLegacyDefaultCentroidCapacity(compression));
      centroidCount = encoded.getInt();
      centroidSize = VERBOSE_CENTROID_SIZE;
    } else {
      compression = normalizeCompression(encoded.getFloat());
      mainCapacity = encoded.getShort();
      bufferCapacity = encoded.getShort();
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
    int centroidOffset = encoded.position();
    int encodedLength = centroidOffset + centroidCount * centroidSize;
    return new SerializedTDigestMetadata(encoding, min, max, compression, centroidCount, mainCapacity,
        bufferCapacity, centroidOffset, centroidSize, encodedLength, Double.NaN, false, false, false, false);
  }

  /// Validates centroids in one pass and optionally decodes their means and weights into reusable arrays.
  ///
  /// The header must describe this exact input, and each supplied array must hold all encoded centroids. Endpoint
  /// rounding inherited from compact re-encoding or old merge arithmetic is restored to the double header bounds.
  public static SerializedTDigestMetadata inspectSerialized(ByteBuffer input, SerializedTDigestMetadata header,
      double[] means, double[] weights) {
    ByteBuffer encoded = input.slice().order(ByteOrder.BIG_ENDIAN);
    encoded.position(header.centroidOffset());
    int centroidCount = header.centroidCount();
    int centroidSize = header.centroidSize();
    double min = header.min();
    double max = header.max();
    double totalWeight = 0.0;
    double previousMean = Double.NEGATIVE_INFINITY;
    boolean hasNonFiniteMeans = false;
    boolean needsLegacyFallback = false;
    boolean unorderedMeans = false;
    boolean fractionalWeights = false;
    boolean withinBounds = true;
    double roundedMin = (double) (float) min;
    double roundedMax = (double) (float) max;
    double lowerAdjacent = Math.nextDown(min);
    double upperAdjacent = Math.nextUp(max);
    for (int i = 0; i < centroidCount; i++) {
      double weight = centroidSize == VERBOSE_CENTROID_SIZE ? encoded.getDouble() : encoded.getFloat();
      double mean = centroidSize == VERBOSE_CENTROID_SIZE ? encoded.getDouble() : encoded.getFloat();
      if (!Double.isFinite(weight)) {
        throw new IllegalArgumentException("Invalid TDigest centroid weight: " + weight);
      }
      fractionalWeights |= weight != Math.rint(weight);
      // A previous Pinot boundary repair emitted a negative residual for singleton weights between one and two.
      // Old tdunning merges could also overflow finite means to NaN or infinity. Preserve those stored bytes,
      // but never normalize or sort them into a plausible distribution.
      needsLegacyFallback |= weight <= 0.0 || Double.isNaN(mean)
          || !Double.isFinite(mean) && (mean < min || mean > max);
      hasNonFiniteMeans |= !Double.isFinite(mean);
      unorderedMeans |= mean < previousMean;
      if (!Double.isNaN(mean)) {
        // A compact digest can be re-emitted as verbose without recovering its double endpoint means. Old
        // merge arithmetic can also drift by one ULP. Permit only these identifiable endpoint roundings.
        boolean lowerRounded = mean == roundedMin || mean == lowerAdjacent;
        boolean upperRounded = mean == roundedMax || mean == upperAdjacent;
        withinBounds &= (mean >= min || lowerRounded) && (mean <= max || upperRounded);
        previousMean = mean;
      }
      if (means != null) {
        means[i] = Math.max(min, Math.min(mean, max));
      }
      if (weights != null) {
        weights[i] = weight;
      }
      totalWeight += weight;
      if (!Double.isFinite(totalWeight)) {
        throw new IllegalArgumentException("TDigest total weight exceeds the supported range");
      }
    }
    if (!needsLegacyFallback && !withinBounds) {
      throw new IllegalArgumentException("TDigest centroid mean is outside its extrema: " + min + ", " + max);
    }
    return new SerializedTDigestMetadata(header.encoding(), min, max, header.compression(), centroidCount,
        header.mainCapacity(), header.bufferCapacity(), header.centroidOffset(), centroidSize,
        header.encodedLength(), totalWeight, hasNonFiniteMeans, needsLegacyFallback, unorderedMeans, fractionalWeights);
  }

  /// Deserializes a finite-valued digest without recompressing stored centroids on each rollup generation.
  public static TDigest deserializeFinite(byte[] bytes) {
    SerializedTDigestInput input = new SerializedTDigestInput();
    input.reset(bytes);
    return deserializeFinite(input);
  }

  /// Deserializes an already validated input without scanning the same centroid bytes again.
  public static TDigest deserializeFinite(SerializedTDigestInput input) {
    SerializedTDigestMetadata metadata = input.getMetadata();
    if (metadata.hasNonFiniteMeans() && !metadata.needsLegacyFallback()) {
      throw new IllegalArgumentException("Expected finite TDigest centroid means");
    }
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forLegacyAggregation(input.getCompression());
    digest.addSerializedTDigest(input);
    return digest;
  }

  /// Deserializes a digest and advances `input` past its encoded bytes.
  public static TDigest deserialize(ByteBuffer input) {
    SerializedTDigestInput encoded = new SerializedTDigestInput();
    encoded.reset(input);
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forLegacyAggregation(encoded.getCompression());
    digest.addSerializedTDigest(encoded);
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
    double average = Math.copySign(1.0, firstMean) == Math.copySign(1.0, secondMean)
        ? firstMean + (secondMean - firstMean) * (secondWeight / totalWeight)
        : firstMean * (firstWeight / totalWeight) + secondMean * (secondWeight / totalWeight);
    return Math.max(Math.min(firstMean, secondMean), Math.min(average, Math.max(firstMean, secondMean)));
  }

  /// Default centroid capacity allocated by the verbose t-digest 3.2 reader.
  public static int getLegacyDefaultCentroidCapacity(double compression) {
    return (int) Math.min(Integer.MAX_VALUE,
        2.0 * Math.ceil(normalizeCompression(compression)) + LEGACY_CAPACITY_PADDING);
  }

  /// Default centroid capacity allocated by the t-digest 3.3 reader.
  public static int getDefaultCentroidCapacity(double compression) {
    double normalizedCompression = normalizeCompression(compression);
    int padding = normalizedCompression < 30.0 ? 30 : 10;
    return (int) Math.min(Integer.MAX_VALUE, Math.ceil(2.0 * normalizedCompression + padding));
  }
}
