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
package org.apache.pinot.segment.spi.codec;

/**
 * Unified codec enum for the forward index codec pipeline. Each codec is classified as either a
 * {@link CodecKind#COMPRESSOR} (byte-level, size-changing) or a {@link CodecKind#TRANSFORM}
 * (typed, in-place, size-preserving).
 *
 * <p>A codec pipeline is an ordered list of {@code ChunkCodec} stages. Transforms are applied
 * left-to-right before a terminal compressor on write; the reverse order is used on read.</p>
 *
 * <p>On-disk, each codec is stored as its {@link #getValue()} int. Compressor values (0–99)
 * align with {@link ChunkCompressionType} for backward compatibility. Transform values start
 * at 100 to leave room for future compressors.</p>
 */
public enum ChunkCodec {

  // --- Compressors (byte-level, size-changing) ---
  // Values 0-99 reserved for compressors; aligned with ChunkCompressionType ordinals.
  PASS_THROUGH(0, CodecKind.COMPRESSOR, false),
  SNAPPY(1, CodecKind.COMPRESSOR, false),
  ZSTANDARD(2, CodecKind.COMPRESSOR, false),
  LZ4(3, CodecKind.COMPRESSOR, false),
  GZIP(5, CodecKind.COMPRESSOR, false),

  // Legacy compound codecs that bundle delta transform + LZ4 in a single compressor/decompressor.
  // These use a custom on-disk format (type flag byte, count metadata, first value, LZ4-compressed deltas)
  // and are NOT byte-compatible with the pipeline approach (DELTA transform + LZ4 compressor).
  // Values aligned with ChunkCompressionType.DELTA(6) and ChunkCompressionType.DELTADELTA(7).
  // Internal-only: users must use ["DELTA", "LZ4"] or ["DOUBLE_DELTA", "LZ4"] instead.
  DELTA_LZ4(6, CodecKind.COMPRESSOR, true),
  DOUBLE_DELTA_LZ4(7, CodecKind.COMPRESSOR, true),

  // --- Transforms (typed, in-place, size-preserving) ---
  // Values 100+ reserved for transforms.
  DELTA(100, CodecKind.TRANSFORM, false),
  DOUBLE_DELTA(101, CodecKind.TRANSFORM, false),
  XOR(102, CodecKind.TRANSFORM, false);

  /** Classification of a codec stage. */
  public enum CodecKind {
    /** Byte-level compression that may change the data size. */
    COMPRESSOR,
    /** Numeric transform that operates in-place without changing data size. */
    TRANSFORM
  }

  private static final ChunkCodec[] BY_VALUE;

  static {
    int maxValue = 0;
    for (ChunkCodec codec : values()) {
      maxValue = Math.max(maxValue, codec._value);
    }
    BY_VALUE = new ChunkCodec[maxValue + 1];
    for (ChunkCodec codec : values()) {
      BY_VALUE[codec._value] = codec;
    }
  }

  private final int _value;
  private final CodecKind _kind;
  private final boolean _internalOnly;

  ChunkCodec(int value, CodecKind kind, boolean internalOnly) {
    _value = value;
    _kind = kind;
    _internalOnly = internalOnly;
  }

  /** Returns the on-disk integer identifier for this codec. */
  public int getValue() {
    return _value;
  }

  /** Returns whether this codec is a compressor or a transform. */
  public CodecKind getKind() {
    return _kind;
  }

  /** Returns {@code true} if this is a byte-level compressor. */
  public boolean isCompressor() {
    return _kind == CodecKind.COMPRESSOR;
  }

  /** Returns {@code true} if this is a numeric transform. */
  public boolean isTransform() {
    return _kind == CodecKind.TRANSFORM;
  }

  /**
   * Returns {@code true} if this codec is internal-only and cannot be used in user-provided
   * codec pipelines. Internal codecs are used for backward-compatible auto-derivation from
   * legacy {@code compressionCodec} settings.
   */
  public boolean isInternalOnly() {
    return _internalOnly;
  }

  /**
   * Validates that this codec is allowed in a user-provided pipeline.
   *
   * @param userProvidedName the name as provided by the user (for error messages)
   * @throws IllegalArgumentException if this codec is internal-only
   */
  public void validateUserFacing(String userProvidedName) {
    if (_internalOnly) {
      throw new IllegalArgumentException(
          "'" + userProvidedName + "' is an internal-only legacy codec and cannot be used in codecPipeline. "
              + "Use [\"DELTA\", \"LZ4\"] or [\"DOUBLE_DELTA\", \"LZ4\"] instead.");
    }
  }

  /**
   * Look up a {@code ChunkCodec} by its on-disk int value (not ordinal).
   *
   * @throws IllegalArgumentException if the value does not map to a known codec
   */
  public static ChunkCodec fromValue(int value) {
    if (value < 0 || value >= BY_VALUE.length || BY_VALUE[value] == null) {
      throw new IllegalArgumentException("Invalid ChunkCodec value: " + value);
    }
    return BY_VALUE[value];
  }
}
