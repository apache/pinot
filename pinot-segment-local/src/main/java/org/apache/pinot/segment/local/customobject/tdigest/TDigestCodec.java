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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/// Compatibility helpers for the serialized t-digest format shared by t-digest 3.2 and 3.3.
///
/// Version 3.3 requires the first and last centroid to have unit weight whenever a digest is recompressed. Version
/// 3.2 did not maintain that invariant, so an otherwise valid 3.2 digest can fail an assertion after being read by
/// 3.3. Pure reads retain the legacy weighted boundary centroids. Adjacent equal means use Pinot's point-mass
/// interpolation, so quantile/CDF values can change on unchanged bytes, including intervals next to a plateau.
/// Mutation and serialization repair their boundaries only when unit endpoints can preserve their total weight and
/// first moment; otherwise unchanged historical bytes remain readable and mutation fails explicitly.
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
        compression, count,
        VERBOSE_HEADER_SIZE, VERBOSE_CENTROID_SIZE, buffer.capacity(), encodedWeight,
        hasNonFiniteMeans, false, false, fractionalWeights, weightedBoundaries, false, compression, Double.NaN,
        Double.NaN, Double.NaN);
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
  /// Exact equality is intentional: endpoint repair preserves these means, and admitting nearby means would grant
  /// the exception to fresh extrema. Fresh fractional input at or beyond a recorded endpoint permanently excludes
  /// that endpoint, even if its historical mass was merged away or the new weight equals the historical weight.
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
        && (!firstUnsupported || firstMean == source.getHistoricalFractionalBoundaryMean(true)
            && !(source.getFreshFractionalBoundaryMean(true) <= firstMean))
        && (!lastUnsupported || lastMean == source.getHistoricalFractionalBoundaryMean(false)
            && !(source.getFreshFractionalBoundaryMean(false) >= lastMean));
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
    TDigestAccumulator digest = TDigestAccumulator.forLegacyAggregation(metadata.compression());
    digest.addHistoricalCentroids(means, weights, means.length, metadata.min(), metadata.max(), true);
    digest.inheritHistoricalFractionalBoundaries(metadata.historicalFractionalMinMean(),
        metadata.historicalFractionalMaxMean());
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
      int centroidCount, int centroidOffset, int centroidSize,
      int encodedLength, double totalWeight, boolean hasNonFiniteMeans, boolean needsLegacyFallback,
      boolean unorderedMeans, boolean fractionalWeights, boolean weightedBoundaries, boolean hasZeroWeightCentroids,
      double encodedCompression, double recoveredInfinityMean, double historicalFractionalMinMean,
      double historicalFractionalMaxMean) {
    /// Whether the encoded centroid view can be used unchanged by healthy pending-state readers and writers.
    public boolean isCanonicalPayload() {
      return !needsLegacyFallback && !weightedBoundaries && !unorderedMeans && !hasZeroWeightCentroids
          && Double.isNaN(recoveredInfinityMean);
    }
  }

  /// Invocation-local parser state. Primitive byte-array reads avoid per-row buffer and metadata allocations.
  /// Public metadata and retained pending payloads receive immutable snapshots instead of sharing this state.
  static final class SerializedTDigestState {
    private static final VarHandle SHORT_VIEW =
        MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle INT_VIEW = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    int _encoding;
    double _min;
    double _max;
    double _compression;
    int _centroidCount;
    int _centroidOffset;
    int _centroidSize;
    int _encodedLength;
    double _totalWeight;
    boolean _hasNonFiniteMeans;
    boolean _needsLegacyFallback;
    boolean _unorderedMeans;
    boolean _fractionalWeights;
    boolean _weightedBoundaries;
    boolean _hasZeroWeightCentroids;
    double _encodedCompression;
    double _recoveredInfinityMean;
    double _historicalFractionalMinMean;
    double _historicalFractionalMaxMean;
    private byte[] _bytes;
    private ByteBuffer _buffer;
    private int _position;
    private int _limit;

    void reset(byte[] bytes) {
      _bytes = bytes;
      _buffer = null;
      _position = 0;
      _limit = bytes.length;
    }

    void reset(ByteBuffer input) {
      _bytes = null;
      _buffer = input.slice().order(ByteOrder.BIG_ENDIAN);
      _position = 0;
      _limit = _buffer.limit();
    }

    void copyFrom(SerializedTDigestMetadata metadata) {
      _encoding = metadata.encoding();
      _min = metadata.min();
      _max = metadata.max();
      _compression = metadata.compression();
      _centroidCount = metadata.centroidCount();
      _centroidOffset = metadata.centroidOffset();
      _centroidSize = metadata.centroidSize();
      _encodedLength = metadata.encodedLength();
      _totalWeight = metadata.totalWeight();
      _hasNonFiniteMeans = metadata.hasNonFiniteMeans();
      _needsLegacyFallback = metadata.needsLegacyFallback();
      _unorderedMeans = metadata.unorderedMeans();
      _fractionalWeights = metadata.fractionalWeights();
      _weightedBoundaries = metadata.weightedBoundaries();
      _hasZeroWeightCentroids = metadata.hasZeroWeightCentroids();
      _encodedCompression = metadata.encodedCompression();
      _recoveredInfinityMean = metadata.recoveredInfinityMean();
      _historicalFractionalMinMean = metadata.historicalFractionalMinMean();
      _historicalFractionalMaxMean = metadata.historicalFractionalMaxMean();
    }

    SerializedTDigestMetadata snapshot() {
      return new SerializedTDigestMetadata(_encoding, _min, _max, _compression, _centroidCount, _centroidOffset,
          _centroidSize, _encodedLength, _totalWeight, _hasNonFiniteMeans, _needsLegacyFallback, _unorderedMeans,
          _fractionalWeights, _weightedBoundaries, _hasZeroWeightCentroids, _encodedCompression, _recoveredInfinityMean,
          _historicalFractionalMinMean, _historicalFractionalMaxMean);
    }

    private int readPosition(int length) {
      if (length > _limit - _position) {
        throw new BufferUnderflowException();
      }
      int position = _position;
      _position += length;
      return position;
    }

    private int readInt() {
      int position = readPosition(Integer.BYTES);
      return _bytes != null ? (int) INT_VIEW.get(_bytes, position) : _buffer.getInt(position);
    }

    private short readShort() {
      int position = readPosition(Short.BYTES);
      return _bytes != null ? (short) SHORT_VIEW.get(_bytes, position) : _buffer.getShort(position);
    }

    private double readDouble() {
      int position = readPosition(Double.BYTES);
      return _bytes != null
          ? Double.longBitsToDouble((long) LONG_VIEW.get(_bytes, position))
          : _buffer.getDouble(position);
    }

    private float readFloat() {
      int position = readPosition(Float.BYTES);
      return _bytes != null ? Float.intBitsToFloat((int) INT_VIEW.get(_bytes, position)) : _buffer.getFloat(position);
    }
  }

  /// Inspects one payload without mutating `input`, scanning the centroid values once.
  public static SerializedTDigestMetadata inspectSerialized(ByteBuffer input) {
    return inspectSerialized(input, true);
  }

  /// Inspects a payload, optionally enforcing the legacy decoder's declared centroid capacity.
  public static SerializedTDigestMetadata inspectSerialized(ByteBuffer input, boolean checkCapacity) {
    SerializedTDigestState state = new SerializedTDigestState();
    state.reset(input);
    readSerializedHeader(state, checkCapacity);
    if (Double.isNaN(state._totalWeight)) {
      inspectSerialized(state, null, null);
    }
    return state.snapshot();
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
    SerializedTDigestState state = new SerializedTDigestState();
    state.reset(input);
    readSerializedHeader(state, checkCapacity);
    return state.snapshot();
  }

  static void readSerializedHeader(SerializedTDigestState state, boolean checkCapacity) {
    int encoding = state.readInt();
    if (encoding != VERBOSE_ENCODING && encoding != SMALL_ENCODING) {
      throw new IllegalStateException("Invalid format for serialized histogram");
    }
    double min = state.readDouble();
    double max = state.readDouble();
    double compression;
    double encodedCompression;
    int mainCapacity;
    int bufferCapacity = 0;
    int centroidCount;
    int centroidSize;
    if (encoding == VERBOSE_ENCODING) {
      encodedCompression = state.readDouble();
      compression = normalizeCompression(encodedCompression);
      mainCapacity = Math.max(getDefaultCentroidCapacity(compression), getLegacyDefaultCentroidCapacity(compression));
      centroidCount = state.readInt();
      centroidSize = VERBOSE_CENTROID_SIZE;
    } else {
      encodedCompression = state.readFloat();
      compression = normalizeCompression(encodedCompression);
      mainCapacity = state.readShort();
      bufferCapacity = state.readShort();
      checkSmallArrayCapacity(mainCapacity);
      checkSmallArrayCapacity(bufferCapacity);
      if (mainCapacity == -1) {
        mainCapacity = getDefaultCentroidCapacity(compression);
      }
      centroidCount = state.readShort();
      centroidSize = SMALL_CENTROID_SIZE;
    }
    if (centroidCount < 0) {
      throw new IllegalArgumentException("Invalid negative TDigest centroid count: " + centroidCount);
    }
    if (centroidCount > (state._limit - state._position) / centroidSize) {
      throw new BufferUnderflowException();
    }
    int centroidOffset = state._position;
    int encodedLength = centroidOffset + centroidCount * centroidSize;
    state._encoding = encoding;
    state._min = min;
    state._max = max;
    state._compression = compression;
    state._centroidCount = centroidCount;
    state._centroidOffset = centroidOffset;
    state._centroidSize = centroidSize;
    state._encodedLength = encodedLength;
    state._totalWeight = Double.NaN;
    state._hasNonFiniteMeans = false;
    state._needsLegacyFallback = Double.isNaN(min) || Double.isNaN(max) || centroidCount > 0 && min > max;
    state._unorderedMeans = false;
    state._fractionalWeights = false;
    state._weightedBoundaries = false;
    state._hasZeroWeightCentroids = false;
    state._encodedCompression = encodedCompression;
    state._recoveredInfinityMean = Double.NaN;
    state._historicalFractionalMinMean = Double.NaN;
    state._historicalFractionalMaxMean = Double.NaN;
    if (checkCapacity && centroidCount > mainCapacity) {
      inspectSerialized(state, null, null);
      if (!state._needsLegacyFallback) {
        throw new IllegalArgumentException("TDigest centroid count exceeds capacity: " + centroidCount);
      }
    }
  }

  /// Validates centroids in one pass and optionally decodes their means and weights into reusable arrays.
  ///
  /// The header must describe this exact input, and each supplied array must hold all encoded centroids. Endpoint
  /// rounding inherited from compact re-encoding or old merge arithmetic is restored to the double header bounds.
  public static SerializedTDigestMetadata inspectSerialized(ByteBuffer input, SerializedTDigestMetadata header,
      double[] means, double[] weights) {
    SerializedTDigestState state = new SerializedTDigestState();
    state.reset(input);
    state.copyFrom(header);
    inspectSerialized(state, means, weights);
    return state.snapshot();
  }

  static void inspectSerialized(SerializedTDigestState state, double[] means, double[] weights) {
    state._position = state._centroidOffset;
    int centroidCount = state._centroidCount;
    int centroidSize = state._centroidSize;
    double min = state._min;
    double max = state._max;
    double totalWeight = 0.0;
    double previousMean = Double.NEGATIVE_INFINITY;
    boolean hasNonFiniteMeans = false;
    boolean needsLegacyFallback = state._needsLegacyFallback;
    boolean unorderedMeans = false;
    boolean fractionalWeights = false;
    boolean hasZeroWeightCentroids = false;
    int positiveCentroidCount = 0;
    double lowerPositiveMean = Double.POSITIVE_INFINITY;
    double upperPositiveMean = Double.NEGATIVE_INFINITY;
    double lowerPositiveWeight = 0.0;
    double upperPositiveWeight = 0.0;
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
    // Legacy mean += (incoming - mean) * weight / totalWeight can cancel near a small endpoint. Bound
    // its value displacement using the finite input range's scale, rather than ULPs of the small result.
    double roundingTolerance = 4.0 * Math.max(Double.isFinite(min) ? Math.ulp(min) : 0.0,
        Double.isFinite(max) ? Math.ulp(max) : 0.0);
    for (int i = 0; i < centroidCount; i++) {
      double weight = centroidSize == VERBOSE_CENTROID_SIZE ? state.readDouble() : state.readFloat();
      double mean = centroidSize == VERBOSE_CENTROID_SIZE ? state.readDouble() : state.readFloat();
      // Legacy readers admitted raw non-finite weights. Their mass cannot be recovered, so preserve the bytes
      // as opaque state while keeping the same finite-weight validation for newly created distributions.
      needsLegacyFallback |= !Double.isFinite(weight);
      // Legacy compact writers can narrow a finite double endpoint outside the float range to infinity.
      // Recover that identifiable rounding before classifying non-finite state; verbose infinity remains opaque.
      if (state._encoding == SMALL_ENCODING) {
        if (mean == Double.NEGATIVE_INFINITY && Double.isFinite(min) && roundedMin == mean) {
          mean = min;
        } else if (mean == Double.POSITIVE_INFINITY && Double.isFinite(max) && roundedMax == mean) {
          mean = max;
        }
      }
      if (means != null) {
        means[i] = restoreCentroidMean(mean, min, max);
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
        positiveCentroidCount++;
      }
      fractionalWeights |= weight != Math.rint(weight);
      boolean nanMean = Double.isNaN(mean);
      hasNaNMeans |= nanMean;
      hasFiniteMeans |= Double.isFinite(mean);
      if (nanMean && !Double.isNaN(infinityMean)) {
        mean = infinityMean;
      }
      double boundaryMean = restoreCentroidMean(mean, min, max);
      if (weight > 0.0) {
        // Match the stored centroid sort, including signed zeros after the decoder's endpoint clamp.
        // Stable sorting keeps the first lower endpoint and the last upper endpoint among equal means.
        if (lowerPositiveWeight == 0.0 || Double.compare(boundaryMean, lowerPositiveMean) < 0) {
          lowerPositiveMean = boundaryMean;
          lowerPositiveWeight = weight;
        }
        if (upperPositiveWeight == 0.0 || Double.compare(boundaryMean, upperPositiveMean) >= 0) {
          upperPositiveMean = boundaryMean;
          upperPositiveWeight = weight;
        }
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
      if (!Double.isNaN(mean)) {
        unorderedMeans |= Double.compare(boundaryMean, previousMean) < 0;
        // A compact endpoint can move between its float-rounded value and the double bound in later merges.
        // Permit that finite interval and bounded legacy arithmetic roundoff, without widening for infinity.
        boolean lowerRounded = Double.isFinite(mean) && Double.isFinite(min)
            && (min - mean <= roundingTolerance || Double.isFinite(roundedMin) && mean >= roundedMin);
        boolean upperRounded = Double.isFinite(mean) && Double.isFinite(max)
            && (mean - max <= roundingTolerance || Double.isFinite(roundedMax) && mean <= roundedMax);
        withinBounds &= (mean >= min || lowerRounded) && (mean <= max || upperRounded);
        previousMean = boundaryMean;
      }
      totalWeight += weight;
      needsLegacyFallback |= !Double.isFinite(totalWeight);
    }
    // Identifiable endpoint rounding can be restored safely. Other historical out-of-extrema values have an
    // unknown distribution: retain their original bytes and NaN statistics instead of inventing clamped quantiles.
    needsLegacyFallback |= !withinBounds
        || hasNaNMeans && (!identifiableInfinityTail || min != max && !hasFiniteMeans);
    double recoveredInfinityMean = hasNaNMeans && !needsLegacyFallback ? infinityMean : Double.NaN;
    if (means != null && !Double.isNaN(recoveredInfinityMean)) {
      for (int i = 0; i < centroidCount; i++) {
        if (Double.isNaN(means[i])) {
          means[i] = restoreCentroidMean(means[i], min, max, recoveredInfinityMean);
        }
      }
    }
    boolean weightedBoundaries = positiveCentroidCount == 1 ? lowerPositiveWeight >= 2.0
        : positiveCentroidCount > 1 && (lowerPositiveWeight > 1.0 || upperPositiveWeight > 1.0);
    boolean fractionalSingleton = positiveCentroidCount == 1 && lowerPositiveWeight != 1.0
        && lowerPositiveWeight < 2.0;
    double historicalFractionalMinMean = !needsLegacyFallback && positiveCentroidCount > 0
        && (lowerPositiveWeight < 1.0 || fractionalSingleton) ? lowerPositiveMean : Double.NaN;
    double historicalFractionalMaxMean = !needsLegacyFallback && positiveCentroidCount > 0
        && (upperPositiveWeight < 1.0 || fractionalSingleton) ? upperPositiveMean : Double.NaN;
    state._totalWeight = totalWeight;
    state._hasNonFiniteMeans = hasNonFiniteMeans;
    state._needsLegacyFallback = needsLegacyFallback;
    state._unorderedMeans = unorderedMeans;
    state._fractionalWeights = fractionalWeights;
    state._weightedBoundaries = weightedBoundaries;
    state._hasZeroWeightCentroids = hasZeroWeightCentroids;
    state._recoveredInfinityMean = recoveredInfinityMean;
    state._historicalFractionalMinMean = historicalFractionalMinMean;
    state._historicalFractionalMaxMean = historicalFractionalMaxMean;
  }

  /// Materializes previously validated centroids without repeating numerical inspection. The metadata must describe
  /// these exact bytes; each supplied array must hold the encoded centroid count. Identified NaN infinity tails and
  /// compact endpoint rounding are restored in the same way as the inspecting decoder.
  public static void decodeSerializedCentroids(ByteBuffer input, SerializedTDigestMetadata metadata, double[] means,
      double[] weights) {
    SerializedTDigestState state = new SerializedTDigestState();
    state.reset(input);
    state.copyFrom(metadata);
    decodeSerializedCentroids(state, means, weights);
  }

  static void decodeSerializedCentroids(SerializedTDigestState state, double[] means, double[] weights) {
    state._position = state._centroidOffset;
    for (int i = 0; i < state._centroidCount; i++) {
      weights[i] = state._centroidSize == VERBOSE_CENTROID_SIZE ? state.readDouble() : state.readFloat();
      double mean = state._centroidSize == VERBOSE_CENTROID_SIZE ? state.readDouble() : state.readFloat();
      means[i] = restoreCentroidMean(mean, state._min, state._max, state._recoveredInfinityMean);
    }
  }

  private static double restoreCentroidMean(double mean, double min, double max) {
    return Math.max(min, Math.min(mean, max));
  }

  private static double restoreCentroidMean(double mean, double min, double max, double recoveredInfinityMean) {
    return Double.isNaN(mean) ? recoveredInfinityMean : restoreCentroidMean(mean, min, max);
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
