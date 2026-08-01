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
package org.apache.pinot.spi.utils;

import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nullable;


/// Pinot-owned framing for the two buffers that make up a Parquet Variant value.
///
/// The version-1 wire format is:
/// ```
/// 0        4 bytes  ASCII magic "PVAR"
/// 4        1 byte   envelope version (1)
/// 5        1 byte   flags (0)
/// 6        2 bytes  reserved (0)
/// 8        4 bytes  metadata length, unsigned range restricted to Java array sizes
/// 12       4 bytes  value length, unsigned range restricted to Java array sizes
/// 16       M bytes  Parquet Variant metadata
/// 16 + M   V bytes  Parquet Variant value
/// ```
///
/// An empty byte array is deliberately not an envelope. Pinot reserves it as the default null value for a
/// `VARIANT` field, allowing the null-value vector to distinguish SQL null from an encoded Variant null.
///
/// This class validates only Pinot's stable outer framing. Producers and consumers remain responsible for validating
/// the Parquet Variant metadata and value payloads.
public final class VariantEnvelope {
  public static final int HEADER_SIZE = 16;
  public static final byte VERSION = 1;
  public static final byte FLAGS = 0;

  private static final int MAGIC = 0x50564152; // ASCII "PVAR"

  private VariantEnvelope() {
  }

  /// Encodes the remaining bytes of the supplied metadata and value buffers without changing their positions or
  /// limits.
  ///
  /// Array-backed buffers are copied directly from their backing arrays. Other buffers, including direct and
  /// read-only buffers, are read through independent duplicate views.
  public static byte[] encode(ByteBuffer metadata, ByteBuffer value) {
    Objects.requireNonNull(metadata, "metadata must not be null");
    Objects.requireNonNull(value, "value must not be null");

    int metadataLength = metadata.remaining();
    int valueLength = value.remaining();
    byte[] envelope = allocate(metadataLength, valueLength);
    copyRemaining(metadata, envelope, HEADER_SIZE);
    copyRemaining(value, envelope, HEADER_SIZE + metadataLength);
    return envelope;
  }

  /// Encodes slices of the supplied arrays without allocating intermediate buffer views.
  public static byte[] encode(byte[] metadata, int metadataOffset, int metadataLength, byte[] value, int valueOffset,
      int valueLength) {
    Objects.requireNonNull(metadata, "metadata must not be null");
    Objects.requireNonNull(value, "value must not be null");
    requireRange(metadata, metadataOffset, metadataLength, "metadata");
    requireRange(value, valueOffset, valueLength, "value");

    byte[] envelope = allocate(metadataLength, valueLength);
    System.arraycopy(metadata, metadataOffset, envelope, HEADER_SIZE, metadataLength);
    System.arraycopy(value, valueOffset, envelope, HEADER_SIZE + metadataLength, valueLength);
    return envelope;
  }

  /// Decodes and validates an envelope, returning zero-copy, read-only views over its metadata and value buffers.
  ///
  /// The returned views alias `envelope`; this method does not copy either payload. The decoded object and any views
  /// obtained from it keep the backing array alive, so the caller does not need to retain a separate reference to
  /// `envelope`. Mutations made to the input array after this method returns are visible through the views and can
  /// corrupt the decoded payload. Callers must therefore treat the input array as immutable for as long as the decoded
  /// object or any returned view may be used.
  ///
  /// The decoded holder is safe for concurrent reads when the aliased input array is not mutated. Each accessor returns
  /// a read-only view with independent position and limit, so cursor movement by one reader does not affect another
  /// reader.
  public static Decoded decode(byte[] envelope) {
    int metadataLength = validateAndGetMetadataLength(envelope);
    int valueLength = envelope.length - HEADER_SIZE - metadataLength;
    ByteBuffer metadata =
        ByteBuffer.wrap(envelope, HEADER_SIZE, metadataLength).slice().asReadOnlyBuffer();
    ByteBuffer value =
        ByteBuffer.wrap(envelope, HEADER_SIZE + metadataLength, valueLength).slice().asReadOnlyBuffer();
    return new Decoded(metadata, value);
  }

  /// Validates the stable outer framing and returns the metadata length without allocating buffer views.
  ///
  /// The value begins at `HEADER_SIZE + metadataLength`; its length is the remaining envelope length.
  public static int validateAndGetMetadataLength(byte[] envelope) {
    Objects.requireNonNull(envelope, "envelope must not be null");
    if (envelope.length < HEADER_SIZE) {
      throw new IllegalArgumentException(
          "Variant envelope is too short: " + envelope.length + " bytes (minimum " + HEADER_SIZE + ')');
    }

    if (readInt(envelope, 0) != MAGIC) {
      throw new IllegalArgumentException("Invalid Variant envelope magic; expected PVAR");
    }
    int version = Byte.toUnsignedInt(envelope[4]);
    if (version != Byte.toUnsignedInt(VERSION)) {
      throw new IllegalArgumentException("Unsupported Variant envelope version: " + version);
    }
    int flags = Byte.toUnsignedInt(envelope[5]);
    if (flags != Byte.toUnsignedInt(FLAGS)) {
      throw new IllegalArgumentException("Unsupported Variant envelope flags: " + flags);
    }
    int reserved = (Byte.toUnsignedInt(envelope[6]) << Byte.SIZE) | Byte.toUnsignedInt(envelope[7]);
    if (reserved != 0) {
      throw new IllegalArgumentException("Variant envelope reserved field must be zero: " + reserved);
    }

    int metadataLength = readInt(envelope, 8);
    int valueLength = readInt(envelope, 12);
    if (metadataLength < 0 || valueLength < 0) {
      throw new IllegalArgumentException(
          "Variant envelope lengths exceed supported Java buffer size: metadata=" + Integer.toUnsignedLong(
              metadataLength) + ", value=" + Integer.toUnsignedLong(valueLength));
    }
    long expectedLength = (long) HEADER_SIZE + metadataLength + valueLength;
    if (expectedLength != envelope.length) {
      throw new IllegalArgumentException(
          "Variant envelope length mismatch: header declares " + expectedLength + " bytes but found "
              + envelope.length);
    }
    return metadataLength;
  }

  /// Returns whether the bytes form a complete, supported Variant envelope.
  public static boolean isEnvelope(@Nullable byte[] envelope) {
    if (envelope == null) {
      return false;
    }
    try {
      validateAndGetMetadataLength(envelope);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /// Allocates an initialized envelope with writable, zero-filled metadata and value regions.
  ///
  /// This is the zero-intermediate-copy producer API for sources that can write directly into a destination array.
  /// The metadata region begins at [#HEADER_SIZE]; the value region begins at `HEADER_SIZE + metadataLength`. Callers
  /// must completely fill both regions before publishing the envelope.
  public static byte[] allocate(int metadataLength, int valueLength) {
    if (metadataLength < 0 || valueLength < 0) {
      throw new IllegalArgumentException(
          "Variant envelope lengths must be non-negative: metadata=" + metadataLength + ", value=" + valueLength);
    }
    long envelopeLength = (long) HEADER_SIZE + metadataLength + valueLength;
    if (envelopeLength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Variant envelope exceeds maximum Java byte array size: " + envelopeLength);
    }
    byte[] envelope = new byte[(int) envelopeLength];
    writeInt(envelope, 0, MAGIC);
    envelope[4] = VERSION;
    envelope[5] = FLAGS;
    writeInt(envelope, 8, metadataLength);
    writeInt(envelope, 12, valueLength);
    return envelope;
  }

  private static void copyRemaining(ByteBuffer source, byte[] target, int targetOffset) {
    int length = source.remaining();
    if (length == 0) {
      return;
    }
    if (source.hasArray()) {
      System.arraycopy(source.array(), source.arrayOffset() + source.position(), target, targetOffset, length);
    } else {
      ByteBuffer sourceView = source.duplicate();
      sourceView.get(target, targetOffset, length);
    }
  }

  private static void requireRange(byte[] bytes, int offset, int length, String description) {
    if (offset < 0 || length < 0 || (long) offset + length > bytes.length) {
      throw new IllegalArgumentException(
          "Variant " + description + " range exceeds source bounds: offset=" + offset + ", length=" + length
              + ", sourceLength=" + bytes.length);
    }
  }

  private static int readInt(byte[] bytes, int offset) {
    return Byte.toUnsignedInt(bytes[offset]) << 24
        | Byte.toUnsignedInt(bytes[offset + 1]) << 16
        | Byte.toUnsignedInt(bytes[offset + 2]) << 8
        | Byte.toUnsignedInt(bytes[offset + 3]);
  }

  private static void writeInt(byte[] bytes, int offset, int value) {
    bytes[offset] = (byte) (value >>> 24);
    bytes[offset + 1] = (byte) (value >>> 16);
    bytes[offset + 2] = (byte) (value >>> 8);
    bytes[offset + 3] = (byte) value;
  }

  /// Read-only views of the two Parquet Variant buffers stored in an envelope.
  ///
  /// Instances retain and alias the envelope array supplied to [VariantEnvelope#decode(byte[])]. They are safe for
  /// concurrent reads only while that array remains unmodified.
  public static final class Decoded {
    private final ByteBuffer _metadata;
    private final ByteBuffer _value;

    private Decoded(ByteBuffer metadata, ByteBuffer value) {
      _metadata = metadata;
      _value = value;
    }

    /// Returns a read-only metadata view with independent position and limit.
    public ByteBuffer getMetadata() {
      return _metadata.asReadOnlyBuffer();
    }

    /// Returns a read-only value view with independent position and limit.
    public ByteBuffer getValue() {
      return _value.asReadOnlyBuffer();
    }
  }
}
