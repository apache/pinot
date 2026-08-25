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
package org.apache.pinot.segment.local.io.codec;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;


/// Compression codec backed by Zstandard (Zstd) with a configurable compression level.
///
/// DSL forms:
///
/// - `ZSTD` — uses default level 3
/// - `ZSTD(3)` — explicit level in the range `1` through [Zstd#maxCompressionLevel()]
///
/// Zstd also supports negative fast-compression levels, but the initial codec DSL deliberately
/// accepts unsigned integer arguments only. Those levels are therefore outside this version's
/// public codec contract. Level `0` (zstd's alias for "use the default level") is rejected so that
/// each behavior has exactly one canonical spelling; use `ZSTD` or an explicit level instead.
///
/// ZSTD is a [CodecKind#COMPRESSION] stage. Compression stages may be chained after all
/// transforms.
///
/// Both encode and decode use the Zstd JNI, which requires direct [ByteBuffer]s.
final class ZstdCodecDefinition implements ChunkCodecHandler<ZstdCodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "ZSTD";

  public static final ZstdCodecDefinition INSTANCE = new ZstdCodecDefinition();

  /// Default compression level when none is specified.
  public static final int DEFAULT_LEVEL = 3;

  private ZstdCodecDefinition() {
  }

  /// Typed options for [ZstdCodecDefinition].
  public static final class Options implements CodecOptions {
    private final int _level;

    public Options(int level) {
      _level = level;
    }

    /// Returns the Zstd compression level.
    public int getLevel() {
      return _level;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Options)) {
        return false;
      }
      return _level == ((Options) o)._level;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_level);
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public CodecKind kind() {
    return CodecKind.COMPRESSION;
  }

  @Override
  public Options parseOptions(List<String> args) {
    if (args.isEmpty()) {
      return new Options(DEFAULT_LEVEL);
    }
    if (args.size() != 1) {
      throw new IllegalArgumentException("ZSTD codec accepts at most one argument (compression level), got: " + args);
    }
    int level;
    try {
      level = Integer.parseInt(args.get(0));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("ZSTD codec level must be an integer, got: " + args.get(0));
    }
    // Level 0 is rejected rather than accepted: zstd treats 0 as "use the default level", which would give
    // ZSTD(0) and ZSTD(3) identical behavior under two different canonical spellings. The canonical spec is
    // frozen into segment headers, so each behavior must have exactly one spelling; use ZSTD or ZSTD(3).
    int minLevel = 1;
    int maxLevel = Zstd.maxCompressionLevel();
    if (level < minLevel || level > maxLevel) {
      throw new IllegalArgumentException(
          "ZSTD level " + level + " is out of range [" + minLevel + ", " + maxLevel + "]");
    }
    return new Options(level);
  }

  @Override
  public void validateContext(Options options, CodecContext ctx) {
    // ZSTD can compress any data type; no restriction
  }

  @Override
  public String canonicalize(Options options) {
    return NAME + "(" + options.getLevel() + ")";
  }

  @Override
  public ByteBuffer encode(Options options, CodecContext ctx, ByteBuffer src) throws IOException {
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    ByteBuffer out = null;
    boolean succeeded = false;
    try {
      long bound = Zstd.compressBound(directSrc.remaining());
      if (bound > Integer.MAX_VALUE) {
        throw new IOException("Zstd compressBound " + bound + " exceeds Integer.MAX_VALUE for input of "
            + directSrc.remaining() + " bytes");
      }
      out = ByteBuffer.allocateDirect((int) bound);
      long result = Zstd.compress(out, directSrc, options.getLevel());
      if (Zstd.isError(result)) {
        throw new IOException("Zstd compression failed: " + Zstd.getErrorName(result));
      }
      out.flip();
      succeeded = true;
      return out;
    } finally {
      CodecBufferUtils.cleanDirectCopy(src, directSrc);
      if (!succeeded) {
        CodecBufferUtils.cleanQuietly(out);
      }
    }
  }

  @Override
  public ByteBuffer decode(Options options, CodecContext ctx, ByteBuffer src) throws IOException {
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    ByteBuffer out = null;
    boolean succeeded = false;
    try {
      long decompressedSize = Zstd.getFrameContentSize(directSrc);
      // Zstd uses negative sentinel values for an unknown or invalid content size. Zero is a known,
      // valid content size for an empty frame and must be allowed through to decompression.
      if (decompressedSize < 0) {
        throw new IOException("Zstd: cannot determine decompressed size from frame header");
      }
      out = ByteBuffer.allocateDirect(
          CodecBufferUtils.checkDeclaredDecompressedSize(decompressedSize, "Zstd", "frame header"));
      long result = Zstd.decompress(out, directSrc);
      if (Zstd.isError(result)) {
        throw new IOException("Zstd decompression failed: " + Zstd.getErrorName(result));
      }
      if (result != decompressedSize) {
        throw new IOException("Zstd decoded " + result + " bytes but frame declared " + decompressedSize
            + ". Segment may be corrupt.");
      }
      out.flip();
      succeeded = true;
      return out;
    } finally {
      CodecBufferUtils.cleanDirectCopy(src, directSrc);
      if (!succeeded) {
        CodecBufferUtils.cleanQuietly(out);
      }
    }
  }

  @Override
  public void decodeInto(Options options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) throws IOException {
    dst.clear();
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    try {
      long declaredDecompressedSize = Zstd.getFrameContentSize(directSrc);
      // As in decode(), zero is a valid known size; only Zstd's negative sentinel values are errors.
      if (declaredDecompressedSize < 0) {
        throw new IOException("Zstd: cannot determine decompressed size from frame header");
      }
      int decompressedSize = CodecBufferUtils.checkDeclaredDecompressedSize(
          declaredDecompressedSize, "Zstd", "frame header");
      if (decompressedSize > dst.capacity()) {
        throw new IllegalArgumentException(
            "Zstd: decompressed size " + decompressedSize + " exceeds dst capacity " + dst.capacity());
      }
      long result = Zstd.decompress(dst, directSrc);
      if (Zstd.isError(result)) {
        throw new IOException("Zstd decompression failed: " + Zstd.getErrorName(result));
      }
      if (result != decompressedSize) {
        throw new IOException("Zstd decoded " + result + " bytes but frame declared " + decompressedSize
            + ". Segment may be corrupt.");
      }
      dst.flip();
    } finally {
      CodecBufferUtils.cleanDirectCopy(src, directSrc);
    }
  }

  @Override
  public int maxEncodedSize(Options options, int inputSize) {
    long bound = Zstd.compressBound(inputSize);
    if (bound > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Zstd compressBound " + bound + " exceeds Integer.MAX_VALUE for inputSize=" + inputSize);
    }
    return (int) bound;
  }

  @Override
  public boolean requiresDirectDstBuffer() {
    return true;
  }
}
