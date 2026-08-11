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

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.ScaleFunction;
import com.tdunning.math.stats.TDigest;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
/// Pinot explicitly uses [ScaleFunction#K_1] for new and deserialized merging digests. T-digest 3.3 changed its default
/// to `K_2`, whose smaller middle-quantile centroid count caused a material P75 accuracy regression in Pinot's
/// deterministic 100-million-row workload. `K_1` retains the accuracy profile of t-digest 3.2 while using the 3.3
/// implementation and its two-level compression.
public final class TDigestUtils {
  private static final int VERBOSE_ENCODING = 1;
  private static final int SMALL_ENCODING = 2;
  private static final int LEGACY_CAPACITY_PADDING = 10;
  private static final int VERBOSE_HEADER_SIZE = 32;
  private static final int VERBOSE_CENTROID_SIZE = 16;
  private static final int SMALL_HEADER_SIZE = 30;
  private static final int SMALL_CENTROID_SIZE = 8;
  private static final int DEFAULT_MERGE_BUFFER_MULTIPLIER = 5;

  private TDigestUtils() {
  }

  /// Creates a merging digest with Pinot's accuracy-preserving scale function.
  public static TDigest createMergingDigest(double compression) {
    return configureScaleFunction(new MergingDigest(compression));
  }

  /// Creates a merging digest with the temporary-buffer footprint used by t-digest 3.2.
  ///
  /// Segment aggregation can keep many digests live at once. T-digest 3.3 more than doubled the default temporary
  /// buffer to support two-level compression, adding about 11 KiB to every compression-100 digest. This factory keeps
  /// the 3.3 main-buffer capacity and Pinot's K1 policy, but restores the smaller temporary buffer for that
  /// high-cardinality use case.
  public static TDigest createMergingDigestWithLegacyBuffer(double compression) {
    return createMergingDigestWithLegacyBuffer(compression, 0);
  }

  /// Creates a reduced-buffer merging digest whose main array can hold at least `minimumMainCapacity` centroids.
  public static TDigest createMergingDigestWithLegacyBuffer(double compression, int minimumMainCapacity) {
    if (minimumMainCapacity < 0) {
      throw new IllegalArgumentException("Minimum main capacity must not be negative: " + minimumMainCapacity);
    }
    double normalizedCompression = Math.max(compression, 10.0);
    int mainCapacity = Math.max(minimumMainCapacity, getDefaultCentroidCapacity(normalizedCompression));
    int legacyBufferCapacity = Math.toIntExact(Math.multiplyExact(5L, (long) Math.ceil(normalizedCompression)));
    int bufferCapacity = Math.max(legacyBufferCapacity, Math.addExact(mainCapacity, 1));
    return configureScaleFunction(new MergingDigest(compression, bufferCapacity, mainCapacity));
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
      if (!(tDigest instanceof MergingDigest)) {
        // Pinot has always decoded TDigest values as MergingDigest. Preserve the previous serialization behavior for
        // other implementations instead of interpreting their implementation-specific payload as MergingDigest
        // bytes.
        ByteBuffer buffer = prepareScratchBuffer(scratchBuffer, tDigest.byteSize());
        tDigest.asBytes(buffer);
        byte[] bytes = new byte[buffer.position()];
        buffer.flip();
        buffer.get(bytes);
        return bytes;
      }

      int defaultCapacity = getDefaultCentroidCapacity(tDigest.compression());
      int legacyCapacity = getLegacyDefaultCentroidCapacity(tDigest.compression());
      // centroidCount() drains pending values at the digest's internal (two-level) compression, but does not perform
      // the final, destructive compression used by asBytes(). It therefore gives us a safe bound for digests
      // deserialized from the compact encoding with an explicitly enlarged main array while still allowing asBytes()
      // to run once.
      int currentCentroidCount = tDigest.centroidCount();
      long sizeBound = Math.min(Math.max(0L, tDigest.size()), Math.max(defaultCapacity, legacyCapacity));
      int maxCentroids = Math.max(currentCentroidCount, (int) sizeBound);
      int requiredCapacity = Math.addExact(VERBOSE_HEADER_SIZE,
          Math.multiplyExact(VERBOSE_CENTROID_SIZE, maxCentroids));
      ByteBuffer verboseBuffer = prepareScratchBuffer(scratchBuffer, requiredCapacity);
      // MergingDigest.asBytes() force-compresses in 3.3. Calling byteSize() first would force a second, destructive
      // pass.
      tDigest.asBytes(verboseBuffer);
      int encodedSize = verboseBuffer.position();
      byte[] verboseBytes = new byte[encodedSize];
      verboseBuffer.flip();
      verboseBuffer.get(verboseBytes);

      return makeLegacyCompatible(verboseBytes);
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
    if (verbose.getInt() != VERBOSE_ENCODING) {
      throw new IllegalArgumentException("Expected verbose TDigest encoding");
    }
    verbose.getDouble();
    verbose.getDouble();
    double compression = verbose.getDouble();
    if (!(compression > 0.0) || !Double.isFinite(compression)) {
      throw new IllegalArgumentException("Invalid TDigest compression: " + compression);
    }
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

  /// Validates the structure and declared capacities of a serialized merging digest.
  public static void validateSerialized(byte[] bytes) {
    validateEncodedDigest(ByteBuffer.wrap(bytes));
  }

  /// Deserializes a finite-valued digest into a merging digest with the temporary-buffer footprint used by 3.2.
  ///
  /// Unlike the standard verbose decoder, this method does not retain the substantially larger default 3.3
  /// temporary arrays. It also repairs legacy weighted boundary centroids before the first 3.3 merge.
  public static TDigest deserializeFiniteWithLegacyBuffer(byte[] bytes) {
    ByteBuffer input = ByteBuffer.wrap(bytes);
    validateEncodedDigest(input.duplicate());

    int encoding = input.getInt();
    double min = input.getDouble();
    double max = input.getDouble();
    double compression;
    int centroidCount;
    boolean verbose;
    if (encoding == VERBOSE_ENCODING) {
      compression = input.getDouble();
      centroidCount = input.getInt();
      verbose = true;
    } else if (encoding == SMALL_ENCODING) {
      compression = input.getFloat();
      input.getShort();
      input.getShort();
      centroidCount = input.getShort();
      verbose = false;
    } else {
      throw new IllegalStateException("Invalid format for serialized histogram");
    }

    if (centroidCount == 0) {
      return createMergingDigestWithLegacyBuffer(compression);
    }
    double[] means = new double[centroidCount + 2];
    double[] weights = new double[centroidCount + 2];
    for (int i = 0; i < centroidCount; i++) {
      double weight = verbose ? input.getDouble() : input.getFloat();
      double mean = verbose ? input.getDouble() : input.getFloat();
      if (!(weight > 0.0) || !Double.isFinite(weight)) {
        throw new IllegalArgumentException("Invalid TDigest centroid weight: " + weight);
      }
      if (!Double.isFinite(mean)) {
        throw new IllegalArgumentException("Expected a finite TDigest centroid mean: " + mean);
      }
      weights[i] = weight;
      means[i] = mean;
    }

    int normalizedCount = weights[0] != 1.0 || weights[centroidCount - 1] != 1.0
        ? normalizeBoundaries(means, weights, centroidCount, min, max) : centroidCount;
    long addCount = 0L;
    for (int i = 0; i < normalizedCount; i++) {
      double weight = weights[i];
      if (weight != Math.rint(weight) || weight >= 0x1p63) {
        return copyIntoLegacyBuffer(
            deserialize(toVerboseBytes(min, max, compression, means, weights, normalizedCount)), normalizedCount);
      }
      long integralWeight = (long) weight;
      addCount = Math.addExact(addCount, 1L + (integralWeight - 1L) / Integer.MAX_VALUE);
    }

    int minimumMainCapacity = Math.addExact(Math.toIntExact(addCount), 2);
    TDigest digest = createMergingDigestWithLegacyBuffer(compression, minimumMainCapacity);
    for (int i = 0; i < normalizedCount; i++) {
      long remainingWeight = (long) weights[i];
      while (remainingWeight > 0L) {
        int weight = (int) Math.min(remainingWeight, Integer.MAX_VALUE);
        digest.add(means[i], weight);
        remainingWeight -= weight;
      }
    }
    return digest;
  }

  /// Deserializes a digest and advances `input` past its encoded bytes.
  public static TDigest deserialize(ByteBuffer input) {
    ByteBuffer encoded = input.slice();
    validateEncodedDigest(encoded.duplicate());

    int encoding = encoded.getInt();
    double min = encoded.getDouble();
    double max = encoded.getDouble();
    double compression;
    int mainCapacity;
    int bufferCapacity;
    int centroidCount;
    int centroidSize;
    if (encoding == VERBOSE_ENCODING) {
      compression = encoded.getDouble();
      centroidCount = encoded.getInt();
      mainCapacity = getDefaultCentroidCapacity(compression);
      bufferCapacity = 5 * mainCapacity;
      centroidSize = VERBOSE_CENTROID_SIZE;
    } else if (encoding == SMALL_ENCODING) {
      compression = encoded.getFloat();
      mainCapacity = encoded.getShort();
      bufferCapacity = encoded.getShort();
      centroidCount = encoded.getShort();
      centroidSize = SMALL_CENTROID_SIZE;
    } else {
      // MergingDigest.fromBytes() has already produced the canonical exception for this case.
      throw new IllegalStateException("Invalid format for serialized histogram");
    }
    int centroidOffset = encoded.position();
    if (centroidCount == 0) {
      return configureScaleFunction(MergingDigest.fromBytes(input));
    }

    double firstWeight = readWeight(encoded, centroidOffset, centroidSize);
    double lastWeight = readWeight(encoded, centroidOffset + (centroidCount - 1) * centroidSize, centroidSize);
    int defaultMainCapacity = getDefaultCentroidCapacity(compression);
    boolean needsBoundaryRepair = firstWeight != 1.0 || lastWeight != 1.0;
    boolean exceedsDefaultVerboseCapacity = encoding == VERBOSE_ENCODING && centroidCount > defaultMainCapacity;
    if (!needsBoundaryRepair && !exceedsDefaultVerboseCapacity) {
      return configureScaleFunction(MergingDigest.fromBytes(input));
    }

    double[] means = new double[centroidCount + 2];
    double[] weights = new double[centroidCount + 2];
    encoded.position(centroidOffset);
    for (int i = 0; i < centroidCount; i++) {
      if (centroidSize == VERBOSE_CENTROID_SIZE) {
        weights[i] = encoded.getDouble();
        means[i] = encoded.getDouble();
      } else {
        weights[i] = encoded.getFloat();
        means[i] = encoded.getFloat();
      }
    }
    int encodedLength = encoded.position();
    int normalizedCount = needsBoundaryRepair
        ? normalizeBoundaries(means, weights, centroidCount, min, max) : centroidCount;
    byte[] normalizedBytes = toVerboseBytes(min, max, compression, means, weights, normalizedCount);
    MergingDigest result;
    if (normalizedCount <= defaultMainCapacity) {
      result = MergingDigest.fromBytes(ByteBuffer.wrap(normalizedBytes));
    } else {
      int resolvedMainCapacity = mainCapacity == -1 ? defaultMainCapacity : mainCapacity;
      int resolvedBufferCapacity = bufferCapacity == -1
          ? DEFAULT_MERGE_BUFFER_MULTIPLIER * resolvedMainCapacity : bufferCapacity;
      int smallMainCapacity = Math.max(resolvedMainCapacity, normalizedCount);
      long smallBufferCapacity = Math.max(Math.max((long) resolvedBufferCapacity, smallMainCapacity + 1L),
          2L * smallMainCapacity + 1L);
      if ((double) (float) compression == compression && normalizedCount <= Short.MAX_VALUE
          && smallMainCapacity <= Short.MAX_VALUE && smallBufferCapacity <= Short.MAX_VALUE
          && areFloatExact(means, weights, normalizedCount)) {
        ByteBuffer normalized = ByteBuffer.allocate(SMALL_HEADER_SIZE + SMALL_CENTROID_SIZE * normalizedCount);
        normalized.putInt(SMALL_ENCODING);
        normalized.putDouble(min);
        normalized.putDouble(max);
        normalized.putFloat((float) compression);
        normalized.putShort((short) smallMainCapacity);
        normalized.putShort((short) smallBufferCapacity);
        normalized.putShort((short) normalizedCount);
        for (int i = 0; i < normalizedCount; i++) {
          normalized.putFloat((float) weights[i]);
          normalized.putFloat((float) means[i]);
        }
        normalized.flip();
        result = MergingDigest.fromBytes(normalized);
      } else {
        result = MergingDigest.fromBytes(
            ByteBuffer.wrap(reduceVerboseCentroids(normalizedBytes, defaultMainCapacity)));
      }
    }
    input.position(Math.addExact(input.position(), encodedLength));
    return configureScaleFunction(result);
  }

  private static TDigest configureScaleFunction(MergingDigest digest) {
    digest.setScaleFunction(ScaleFunction.K_1);
    return digest;
  }

  private static TDigest copyIntoLegacyBuffer(TDigest source, int centroidCount) {
    TDigest digest = createMergingDigestWithLegacyBuffer(source.compression(), Math.addExact(centroidCount, 2));
    digest.add(java.util.List.of(source));
    return digest;
  }

  private static boolean areFloatExact(double[] means, double[] weights, int count) {
    for (int i = 0; i < count; i++) {
      if ((double) (float) weights[i] != weights[i] || (double) (float) means[i] != means[i]) {
        return false;
      }
    }
    return true;
  }

  private static byte[] toVerboseBytes(double min, double max, double compression, double[] means, double[] weights,
      int count) {
    ByteBuffer normalized = ByteBuffer.allocate(VERBOSE_HEADER_SIZE + VERBOSE_CENTROID_SIZE * count);
    normalized.putInt(VERBOSE_ENCODING);
    normalized.putDouble(min);
    normalized.putDouble(max);
    normalized.putDouble(compression);
    normalized.putInt(count);
    for (int i = 0; i < count; i++) {
      normalized.putDouble(weights[i]);
      normalized.putDouble(means[i]);
    }
    return normalized.array();
  }

  private static double readWeight(ByteBuffer encoded, int offset, int centroidSize) {
    return centroidSize == VERBOSE_CENTROID_SIZE ? encoded.getDouble(offset) : encoded.getFloat(offset);
  }

  private static void validateEncodedDigest(ByteBuffer input) {
    int encoding = input.getInt();
    if (encoding != VERBOSE_ENCODING && encoding != SMALL_ENCODING) {
      throw new IllegalStateException("Invalid format for serialized histogram");
    }
    input.getDouble();
    input.getDouble();
    double compression;
    int mainCapacity;
    int centroidCount;
    int centroidSize;
    if (encoding == VERBOSE_ENCODING) {
      compression = input.getDouble();
      mainCapacity = getDefaultCentroidCapacity(compression);
      centroidCount = input.getInt();
      centroidSize = VERBOSE_CENTROID_SIZE;
    } else {
      compression = input.getFloat();
      mainCapacity = input.getShort();
      int bufferCapacity = input.getShort();
      checkSmallArrayCapacity(mainCapacity);
      checkSmallArrayCapacity(bufferCapacity);
      if (mainCapacity == -1) {
        mainCapacity = getDefaultCentroidCapacity(compression);
      }
      centroidCount = input.getShort();
      centroidSize = SMALL_CENTROID_SIZE;
    }
    if (centroidCount < 0) {
      throw new IllegalArgumentException("Invalid negative TDigest centroid count: " + centroidCount);
    }
    if (centroidCount > input.remaining() / centroidSize) {
      throw new BufferUnderflowException();
    }
    int readableMainCapacity = mainCapacity;
    if (encoding == VERBOSE_ENCODING && compression > 0.0 && Double.isFinite(compression)) {
      readableMainCapacity = Math.max(mainCapacity, getLegacyDefaultCentroidCapacity(compression));
    }
    if (centroidCount > readableMainCapacity) {
      throw new ArrayIndexOutOfBoundsException(
          "Index " + readableMainCapacity + " out of bounds for length " + readableMainCapacity);
    }
  }

  private static void checkSmallArrayCapacity(int capacity) {
    if (capacity < -1) {
      throw new NegativeArraySizeException(Integer.toString(capacity));
    }
  }

  private static int normalizeBoundaries(double[] means, double[] weights, int count, double min, double max) {
    if (count == 1) {
      double weight = weights[0];
      if (weight <= 1.0) {
        return count;
      }
      double mean = means[0];
      means[0] = min;
      weights[0] = 1.0;
      if (weight == 2.0) {
        means[1] = max;
        weights[1] = 1.0;
        return 2;
      }
      means[1] = residualMean(mean, weight, min, max, weight - 2.0, min, max);
      weights[1] = weight - 2.0;
      means[2] = max;
      weights[2] = 1.0;
      return 3;
    }

    if (weights[0] > 1.0) {
      System.arraycopy(means, 1, means, 2, count - 1);
      System.arraycopy(weights, 1, weights, 2, count - 1);
      double weight = weights[0];
      double mean = means[0];
      means[0] = min;
      weights[0] = 1.0;
      means[1] = residualMean(mean, weight, min, 0.0, weight - 1.0, min, means[2]);
      weights[1] = weight - 1.0;
      count++;
    }
    int lastIndex = count - 1;
    if (weights[lastIndex] > 1.0) {
      double weight = weights[lastIndex];
      double mean = means[lastIndex];
      means[lastIndex] = residualMean(mean, weight, max, 0.0, weight - 1.0, means[lastIndex - 1], max);
      weights[lastIndex] = weight - 1.0;
      means[count] = max;
      weights[count] = 1.0;
      count++;
    }
    return count;
  }

  private static double clamp(double value, double min, double max) {
    return Math.max(min, Math.min(value, max));
  }

  private static double residualMean(double mean, double weight, double firstRemovedValue,
      double secondRemovedValue, double residualWeight, double min, double max) {
    if (!Double.isFinite(mean) || !Double.isFinite(firstRemovedValue)
        || !Double.isFinite(secondRemovedValue)) {
      return clamp(mean, min, max);
    }
    boolean removedTwoValues = weight - residualWeight == 2.0;
    int scaleShift = removedTwoValues ? 2 : 1;
    double scaledMean = Math.scalb(mean, -scaleShift);
    double scaledCorrection = scaledMean - Math.scalb(firstRemovedValue, -scaleShift);
    if (removedTwoValues) {
      scaledCorrection += scaledMean - Math.scalb(secondRemovedValue, -scaleShift);
    }
    double correction = Math.scalb(scaledCorrection / residualWeight, scaleShift);
    return clamp(mean + correction, min, max);
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
