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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

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
/// intermediate results. Decoding checks lengths and numerical state before allocating centroid arrays.
/// NaN means at an identifiable infinite tail are restored to that infinite bound without changing their weights.
/// Other poisoned states remain byte-readable with NaN statistics because their distribution cannot be recovered.
/// They retain their original bytes; attempting to mutate them or mix them with another distribution fails explicitly.
/// Opaque poisoned payloads retain their original centroids even if an old reader's default array cannot hold them;
/// these historical exceptions remain readable by Pinot, but are not guaranteed to survive a reverse upgrade.
/// Historical fractional boundary masses below one, and singleton masses between one and two, retain their original
/// bytes. Fresh serialization rejects these boundaries: t-digest 3.3's unit-endpoint assertion cannot be satisfied
/// without changing their mass. Fractional interior weights remain supported.
public final class TDigestCodec {
  public static final int VERBOSE_ENCODING = 1;
  public static final int SMALL_ENCODING = 2;
  private static final int LEGACY_CAPACITY_PADDING = 10;
  public static final int VERBOSE_HEADER_SIZE = 32;
  public static final int VERBOSE_CENTROID_SIZE = 16;
  public static final int SMALL_HEADER_SIZE = 30;
  public static final int SMALL_CENTROID_SIZE = 8;
  public static final int DEFAULT_MERGE_BUFFER_MULTIPLIER = 5;

  private TDigestCodec() {
  }

  /// Reads the compression from a validated header without decoding centroid arrays.
  public static double readCompression(byte[] bytes) {
    return readSerializedHeader(ByteBuffer.wrap(bytes)).compression();
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

  /// Writes validated centroid arrays in the standard verbose encoding without sorting or recompression.
  /// The caller retains the arrays; only the first `count` entries are written, with normalized compression.
  public static byte[] serializeCentroids(double compression, double min, double max, double[] means,
      double[] weights, int count) {
    if (count < 0 || count > means.length || count > weights.length) {
      throw new IllegalArgumentException("Invalid TDigest centroid count: " + count);
    }
    ByteBuffer encoded = ByteBuffer.allocate(Math.addExact(VERBOSE_HEADER_SIZE,
        Math.multiplyExact(VERBOSE_CENTROID_SIZE, count)));
    encoded.putInt(VERBOSE_ENCODING).putDouble(min).putDouble(max).putDouble(normalizeCompression(compression));
    encoded.putInt(count);
    for (int i = 0; i < count; i++) {
      encoded.putDouble(weights[i]).putDouble(means[i]);
    }
    return encoded.array();
  }

  /// Encodes validated, sorted native centroids and selects a legacy-compatible representation.
  static byte[] serializeCompatibleCentroids(double compression, double min, double max, double[] means,
      double[] weights, int count, boolean inheritedFractionalBoundaries) {
    if (count < 0 || count > 0 && (count > means.length || count > weights.length || !(min <= max))) {
      throw new IllegalArgumentException("Invalid TDigest centroid count or extrema");
    }
    ByteBuffer buffer = ByteBuffer.allocate(
        Math.addExact(VERBOSE_HEADER_SIZE,
            Math.multiplyExact(VERBOSE_CENTROID_SIZE, count)));
    buffer.putInt(VERBOSE_ENCODING);
    buffer.putDouble(min);
    buffer.putDouble(max);
    buffer.putDouble(compression);
    buffer.putInt(count);
    double encodedWeight = 0.0;
    boolean hasNonFiniteMeans = false;
    boolean fractionalWeights = false;
    for (int i = 0; i < count; i++) {
      if (!(weights[i] > 0.0) || !Double.isFinite(weights[i]) || !(means[i] >= min && means[i] <= max)) {
        throw new IllegalArgumentException("Invalid TDigest centroid");
      }
      buffer.putDouble(weights[i]);
      buffer.putDouble(means[i]);
      encodedWeight += weights[i];
      hasNonFiniteMeans |= !Double.isFinite(means[i]);
      fractionalWeights |= weights[i] != Math.rint(weights[i]);
    }
    if (!Double.isFinite(encodedWeight)) {
      throw new IllegalArgumentException("TDigest total weight exceeds the supported range");
    }
    boolean weightedBoundaries = count == 1 ? weights[0] >= 2.0
        : count > 1 && (weights[0] > 1.0 || weights[count - 1] > 1.0);
    // Collect exact wire metadata during the write rather than decoding and validating our fresh bytes again.
    SerializedTDigestMetadata metadata = new SerializedTDigestMetadata(VERBOSE_ENCODING, min, max,
        compression, count, Math.max(getDefaultCentroidCapacity(compression),
        getLegacyDefaultCentroidCapacity(compression)), 0,
        VERBOSE_HEADER_SIZE, VERBOSE_CENTROID_SIZE, buffer.capacity(), encodedWeight,
        hasNonFiniteMeans, false, false, fractionalWeights, weightedBoundaries, false, compression, Double.NaN);
    return makeLegacyCompatible(buffer.array(), metadata, inheritedFractionalBoundaries);
  }

  /// Rejects fresh boundary masses that legacy readers cannot recompress without inventing mass.
  /// Call after boundary normalization and removing zero-weight centroids; fractional interior mass is supported.
  static void checkLegacyBoundaryWeights(int count, double firstWeight, double lastWeight) {
    if (count > 0 && (firstWeight < 1.0 || lastWeight < 1.0
        || count == 1 && firstWeight != 1.0 && firstWeight < 2.0)) {
      throw new IllegalArgumentException("Cannot serialize fractional TDigest boundary mass for legacy readers");
    }
  }

  /// Allows only unsupported endpoint means inherited from a validated legacy source. This is shared by native,
  /// enclosing and generic writers; new fractional global extrema do not receive the historical exception.
  static boolean hasInheritedFractionalBoundaryEncoding(TDigest source, int count, double firstMean,
      double firstWeight, double lastMean, double lastWeight) {
    if (count <= 0 || !source.hasValidStatistics() || !(firstWeight > 0.0) || !(lastWeight > 0.0)
        || !Double.isFinite(firstWeight) || !Double.isFinite(lastWeight)) {
      return false;
    }
    boolean singleton = count == 1 && firstWeight != 1.0 && firstWeight < 2.0;
    boolean firstUnsupported = firstWeight < 1.0 || singleton;
    boolean lastUnsupported = lastWeight < 1.0 || singleton;
    return (firstUnsupported || lastUnsupported)
        && (!firstUnsupported || firstMean == source.getHistoricalFractionalBoundaryMean(true))
        && (!lastUnsupported || lastMean == source.getHistoricalFractionalBoundaryMean(false));
  }

  private static void checkSerializedBoundaryWeights(byte[] bytes, SerializedTDigestMetadata metadata) {
    int count = metadata.centroidCount();
    if (count > 0) {
      ByteBuffer encoded = ByteBuffer.wrap(bytes);
      int firstOffset = metadata.centroidOffset();
      int lastOffset = firstOffset + (count - 1) * metadata.centroidSize();
      double firstWeight = metadata.encoding() == VERBOSE_ENCODING
          ? encoded.getDouble(firstOffset) : encoded.getFloat(firstOffset);
      double lastWeight = metadata.encoding() == VERBOSE_ENCODING
          ? encoded.getDouble(lastOffset) : encoded.getFloat(lastOffset);
      checkLegacyBoundaryWeights(count, firstWeight, lastWeight);
    }
  }

  /// Converts a verbose `MergingDigest` payload to a representation readable by t-digest 3.2 and 3.3.
  ///
  /// The input must use the standard verbose `MergingDigest` encoding. The returned payload stays verbose when it
  /// already fits both legacy default capacities, uses compact encoding only when every narrowed field is exact, and
  /// otherwise reduces interior centroids without narrowing doubles. Compression below ten is normalized in the
  /// output header because the 3.2 verbose reader sizes its arrays from the raw header before applying that minimum.
  /// Opaque degraded state is retained without reducing its unknown distribution, including over-capacity payloads.
  /// Fresh fractional endpoints that legacy unit-boundary repair cannot represent are rejected without reweighting.
  public static byte[] makeLegacyCompatible(byte[] verboseBytes) {
    return makeLegacyCompatible(verboseBytes, inspectSerialized(ByteBuffer.wrap(verboseBytes), false));
  }

  /// Serializes an unchanged, independently retained payload using metadata validated at ingestion.
  ///
  /// The metadata must describe these exact bytes. This avoids another centroid validation walk for lazy digests.
  public static byte[] makeLegacyCompatible(byte[] verboseBytes, SerializedTDigestMetadata metadata) {
    return makeLegacyCompatible(verboseBytes, metadata, false);
  }

  /// Retains exact verbose weights for inherited historical fractional endpoints that cannot become unit endpoints.
  /// All other compatibility checks apply. These payloads keep their historical assertion-enabled 3.3 read/merge
  /// limitation; callers must establish inherited endpoint provenance before enabling this exception.
  static byte[] makeLegacyCompatible(byte[] verboseBytes, SerializedTDigestMetadata metadata,
      boolean inheritedFractionalBoundaries) {
    if (metadata.encoding() != VERBOSE_ENCODING) {
      throw new IllegalArgumentException("Expected verbose TDigest encoding");
    }
    if (metadata.needsLegacyFallback()) {
      return verboseBytes;
    }
    if (!Double.isNaN(metadata.recoveredInfinityMean())) {
      double[] means = new double[metadata.centroidCount()];
      double[] weights = new double[means.length];
      decodeSerializedCentroids(ByteBuffer.wrap(verboseBytes), metadata, means, weights);
      if (!inheritedFractionalBoundaries) {
        return serializeRecoveredCentroids(metadata, means, weights);
      }
      verboseBytes = serializeCentroids(metadata.compression(), metadata.min(), metadata.max(), means, weights,
          means.length);
      metadata = inspectSerialized(ByteBuffer.wrap(verboseBytes), false);
    }
    if (metadata.hasZeroWeightCentroids()) {
      verboseBytes = removeZeroWeightCentroids(verboseBytes, metadata);
      metadata = inspectSerialized(ByteBuffer.wrap(verboseBytes), false);
    }
    if (!inheritedFractionalBoundaries) {
      checkSerializedBoundaryWeights(verboseBytes, metadata);
    }
    double compression = metadata.compression();
    if (metadata.encodedCompression() != compression) {
      verboseBytes = verboseBytes.clone();
      ByteBuffer.wrap(verboseBytes).putDouble(20, compression);
    }
    int centroidCount = metadata.centroidCount();
    int legacyCapacity =
        Math.min(getLegacyDefaultCentroidCapacity(compression), getDefaultCentroidCapacity(compression));
    if (centroidCount <= legacyCapacity) {
      return verboseBytes;
    }
    int mainCapacity = getDefaultCentroidCapacity(compression);
    long bufferCapacity = DEFAULT_MERGE_BUFFER_MULTIPLIER * (long) mainCapacity;
    if (centroidCount > mainCapacity || inheritedFractionalBoundaries || (double) (float) compression != compression
        || centroidCount > Short.MAX_VALUE
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

  private static byte[] serializeRecoveredCentroids(SerializedTDigestMetadata metadata, double[] means,
      double[] weights) {
    PercentileTDigestAccumulator digest = PercentileTDigestAccumulator.forLegacyAggregation(metadata.compression());
    digest.addCentroids(means, weights, means.length, metadata.min(), metadata.max(), true);
    return digest.serialize();
  }

  private static byte[] removeZeroWeightCentroids(byte[] verboseBytes, SerializedTDigestMetadata metadata) {
    ByteBuffer source = ByteBuffer.wrap(verboseBytes);
    source.position(metadata.centroidOffset());
    ByteBuffer filtered = ByteBuffer.allocate(metadata.encodedLength());
    filtered.put(verboseBytes, 0, VERBOSE_HEADER_SIZE);
    int count = 0;
    for (int i = 0; i < metadata.centroidCount(); i++) {
      double weight = source.getDouble();
      double mean = source.getDouble();
      if (weight != 0.0) {
        filtered.putDouble(weight).putDouble(mean);
        count++;
      }
    }
    filtered.putInt(28, count);
    return Arrays.copyOf(filtered.array(), filtered.position());
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
      boolean unorderedMeans, boolean fractionalWeights, boolean weightedBoundaries, boolean hasZeroWeightCentroids,
      double encodedCompression, double recoveredInfinityMean) {
  }

  /// Inspects one payload without mutating `input`, scanning the centroid values once.
  public static SerializedTDigestMetadata inspectSerialized(ByteBuffer input) {
    return inspectSerialized(input, true);
  }

  /// Inspects a payload, optionally enforcing the legacy decoder's declared centroid capacity.
  public static SerializedTDigestMetadata inspectSerialized(ByteBuffer input, boolean checkCapacity) {
    SerializedTDigestMetadata header = readSerializedHeader(input, checkCapacity);
    return Double.isNaN(header.totalWeight()) ? inspectSerialized(input, header, null, null) : header;
  }

  /// Reads the fixed-size header without allocating centroid arrays; oversized historical state is also inspected.
  public static SerializedTDigestMetadata readSerializedHeader(ByteBuffer input) {
    return readSerializedHeader(input, true);
  }

  /// Reads a fixed-size header, optionally enforcing the declared centroid capacity.
  ///
  /// NaN or inverted nonempty extrema mark legacy degraded state. Oversized payloads require numerical inspection:
  /// historical degraded state remains readable even if an old decoder's arrays could not hold its centroids.
  public static SerializedTDigestMetadata readSerializedHeader(ByteBuffer input, boolean checkCapacity) {
    ByteBuffer encoded = input.slice().order(ByteOrder.BIG_ENDIAN);
    int encoding = encoded.getInt();
    if (encoding != VERBOSE_ENCODING && encoding != SMALL_ENCODING) {
      throw new IllegalStateException("Invalid format for serialized histogram");
    }
    double min = encoded.getDouble();
    double max = encoded.getDouble();
    double compression;
    double encodedCompression;
    int mainCapacity;
    int bufferCapacity = 0;
    int centroidCount;
    int centroidSize;
    if (encoding == VERBOSE_ENCODING) {
      encodedCompression = encoded.getDouble();
      compression = normalizeCompression(encodedCompression);
      mainCapacity = Math.max(getDefaultCentroidCapacity(compression), getLegacyDefaultCentroidCapacity(compression));
      centroidCount = encoded.getInt();
      centroidSize = VERBOSE_CENTROID_SIZE;
    } else {
      encodedCompression = encoded.getFloat();
      compression = normalizeCompression(encodedCompression);
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
    int centroidOffset = encoded.position();
    int encodedLength = centroidOffset + centroidCount * centroidSize;
    SerializedTDigestMetadata header = new SerializedTDigestMetadata(encoding, min, max, compression, centroidCount,
        mainCapacity,
        bufferCapacity, centroidOffset, centroidSize, encodedLength, Double.NaN, false,
        Double.isNaN(min) || Double.isNaN(max) || centroidCount > 0 && min > max,
        false, false, false, false, encodedCompression, Double.NaN);
    if (checkCapacity && centroidCount > mainCapacity) {
      SerializedTDigestMetadata inspected = inspectSerialized(input, header, null, null);
      if (!inspected.needsLegacyFallback()) {
        throw new IllegalArgumentException("TDigest centroid count exceeds capacity: " + centroidCount);
      }
      return inspected;
    }
    return header;
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
    boolean needsLegacyFallback = header.needsLegacyFallback();
    boolean unorderedMeans = false;
    boolean fractionalWeights = false;
    boolean hasZeroWeightCentroids = false;
    int positiveCentroidCount = 0;
    double firstPositiveWeight = 0.0;
    double lastPositiveWeight = 0.0;
    boolean withinBounds = true;
    // Infer only a matching one-sided tail, or a distribution consisting entirely of one infinity. A NaN
    // between finite centroids or in a +/-Infinity mixture has lost information and remains opaque.
    double infinityMean = min == max && Double.isInfinite(min) ? min
        : Double.isFinite(min) && max == Double.POSITIVE_INFINITY ? max
        : min == Double.NEGATIVE_INFINITY && Double.isFinite(max) ? min : Double.NaN;
    boolean hasNaNMeans = false;
    boolean hasFiniteMeans = false;
    boolean positiveInfinityTailStarted = false;
    boolean negativeInfinityTailEnded = false;
    boolean identifiableInfinityTail = true;
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
      // Legacy compact writers can narrow a finite double endpoint outside the float range to infinity.
      // Recover that identifiable rounding before classifying non-finite state; verbose infinity remains opaque.
      if (header.encoding() == SMALL_ENCODING) {
        if (mean == Double.NEGATIVE_INFINITY && Double.isFinite(min) && roundedMin == mean) {
          mean = min;
        } else if (mean == Double.POSITIVE_INFINITY && Double.isFinite(max) && roundedMax == mean) {
          mean = max;
        }
      }
      if (means != null) {
        means[i] = Math.max(min, Math.min(mean, max));
      }
      if (weights != null) {
        weights[i] = weight;
      }
      // A zero-mass centroid has no distribution to merge, even when its unused mean is NaN or stale.
      if (weight == 0.0) {
        hasZeroWeightCentroids = true;
        continue;
      }
      if (weight > 0.0) {
        if (positiveCentroidCount++ == 0) {
          firstPositiveWeight = weight;
        }
        lastPositiveWeight = weight;
      }
      fractionalWeights |= weight != Math.rint(weight);
      boolean nanMean = Double.isNaN(mean);
      hasNaNMeans |= nanMean;
      hasFiniteMeans |= Double.isFinite(mean);
      if (nanMean && !Double.isNaN(infinityMean)) {
        mean = infinityMean;
      }
      if (infinityMean == Double.POSITIVE_INFINITY) {
        if (mean == infinityMean) {
          positiveInfinityTailStarted = true;
        } else if (positiveInfinityTailStarted) {
          identifiableInfinityTail = false;
        }
      } else if (infinityMean == Double.NEGATIVE_INFINITY) {
        if (mean == infinityMean && negativeInfinityTailEnded) {
          identifiableInfinityTail = false;
        } else if (mean != infinityMean) {
          negativeInfinityTailEnded = true;
        }
      }
      if (min == max && Double.isInfinite(min) && mean != infinityMean) {
        identifiableInfinityTail = false;
      }
      // Negative residuals and unidentifiable NaN means still have an unknown distribution. Never normalize
      // or sort them into plausible statistics, even if another centroid could be repaired as an infinite tail.
      needsLegacyFallback |= weight < 0.0 || Double.isNaN(mean)
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
      totalWeight += weight;
      if (!Double.isFinite(totalWeight)) {
        throw new IllegalArgumentException("TDigest total weight exceeds the supported range");
      }
    }
    // Identifiable endpoint rounding can be restored safely. Other historical out-of-extrema values have an
    // unknown distribution: retain their original bytes and NaN statistics instead of inventing clamped quantiles.
    needsLegacyFallback |= !withinBounds
        || hasNaNMeans && (!identifiableInfinityTail || min != max && !hasFiniteMeans);
    double recoveredInfinityMean = hasNaNMeans && !needsLegacyFallback ? infinityMean : Double.NaN;
    if (means != null && !Double.isNaN(recoveredInfinityMean)) {
      for (int i = 0; i < centroidCount; i++) {
        if (Double.isNaN(means[i])) {
          means[i] = recoveredInfinityMean;
        }
      }
    }
    boolean weightedBoundaries = positiveCentroidCount == 1 ? firstPositiveWeight >= 2.0
        : positiveCentroidCount > 1 && (firstPositiveWeight > 1.0 || lastPositiveWeight > 1.0);
    return new SerializedTDigestMetadata(header.encoding(), min, max, header.compression(), centroidCount,
        header.mainCapacity(), header.bufferCapacity(), header.centroidOffset(), centroidSize,
        header.encodedLength(), totalWeight, hasNonFiniteMeans, needsLegacyFallback, unorderedMeans, fractionalWeights,
        weightedBoundaries, hasZeroWeightCentroids, header.encodedCompression(), recoveredInfinityMean);
  }

  /// Materializes previously validated centroids without repeating numerical inspection. The metadata must describe
  /// these exact bytes; each supplied array must hold the encoded centroid count. Identified NaN infinity tails and
  /// compact endpoint rounding are restored in the same way as the inspecting decoder.
  public static void decodeSerializedCentroids(ByteBuffer input, SerializedTDigestMetadata metadata, double[] means,
      double[] weights) {
    ByteBuffer encoded = input.slice().order(ByteOrder.BIG_ENDIAN);
    encoded.position(metadata.centroidOffset());
    for (int i = 0; i < metadata.centroidCount(); i++) {
      weights[i] = metadata.centroidSize() == VERBOSE_CENTROID_SIZE ? encoded.getDouble() : encoded.getFloat();
      double mean = metadata.centroidSize() == VERBOSE_CENTROID_SIZE ? encoded.getDouble() : encoded.getFloat();
      means[i] = Double.isNaN(mean) ? metadata.recoveredInfinityMean()
          : Math.max(metadata.min(), Math.min(mean, metadata.max()));
    }
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
          weights[mergeIndex + 1], combinedWeight, false);
      weights[mergeIndex] = combinedWeight;
      int numMoved = centroidCount - mergeIndex - 2;
      if (numMoved > 0) {
        System.arraycopy(means, mergeIndex + 2, means, mergeIndex + 1, numMoved);
        System.arraycopy(weights, mergeIndex + 2, weights, mergeIndex + 1, numMoved);
      }
      centroidCount--;
    }

    return serializeCentroids(compression, min, max, means, weights, centroidCount);
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

  /// Computes a weighted mean without overflowing same-sign inputs or drifting outside their observed range.
  /// `sameSignMeans` skips per-mean sign checks when the caller has already established that invariant.
  public static double weightedMean(double firstMean, double firstWeight, double secondMean,
      double secondWeight, double totalWeight, boolean sameSignMeans) {
    if (firstMean == secondMean) {
      return firstMean;
    }
    if (!Double.isFinite(firstMean) || !Double.isFinite(secondMean)) {
      if (firstMean == Double.NEGATIVE_INFINITY || secondMean == Double.NEGATIVE_INFINITY) {
        return Double.NEGATIVE_INFINITY;
      }
      return Double.POSITIVE_INFINITY;
    }
    double average = sameSignMeans || Math.copySign(1.0, firstMean) == Math.copySign(1.0, secondMean)
        ? firstMean + (secondMean - firstMean) * (secondWeight / totalWeight)
        : firstMean * (firstWeight / totalWeight) + secondMean * (secondWeight / totalWeight);
    return Math.max(Math.min(firstMean, secondMean), Math.min(average, Math.max(firstMean, secondMean)));
  }

  /// Default centroid capacity derived from the raw header by the verbose t-digest 3.2 reader.
  /// Negative raw capacities are unusable and return zero; fresh headers normalize compression before writing.
  public static int getLegacyDefaultCentroidCapacity(double compression) {
    validateCompression(compression);
    return (int) Math.min(Integer.MAX_VALUE,
        Math.max(0.0, 2.0 * Math.ceil(compression) + LEGACY_CAPACITY_PADDING));
  }

  /// Default centroid capacity allocated by the t-digest 3.3 reader.
  public static int getDefaultCentroidCapacity(double compression) {
    double normalizedCompression = normalizeCompression(compression);
    int padding = normalizedCompression < 30.0 ? 30 : 10;
    return (int) Math.min(Integer.MAX_VALUE, Math.ceil(2.0 * normalizedCompression + padding));
  }

  /// Bounds newly encoded compatible bytes without flushing. Compact low-compression output fits this verbose
  /// bound; unchanged historical payloads and capacity-preserving small writes must account for their own length.
  public static int getMaxLegacyCompatibleByteSize(double compression, long centroidCountUpperBound) {
    double normalizedCompression = normalizeCompression(compression);
    long capacity = Math.min(getLegacyDefaultCentroidCapacity(normalizedCompression),
        getDefaultCentroidCapacity(normalizedCompression));
    long count = Math.min(capacity, centroidCountUpperBound);
    return Math.toIntExact(Math.addExact(VERBOSE_HEADER_SIZE, Math.multiplyExact(VERBOSE_CENTROID_SIZE, count)));
  }
}
