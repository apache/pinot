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
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecOptions;


/// Compression codec backed by Zstandard (Zstd) with a configurable compression level.
///
/// DSL forms:
///
/// - `ZSTD` — uses default level 3
/// - `ZSTD(3)` — explicit level in the range [[Zstd#minCompressionLevel()],
///       [Zstd#maxCompressionLevel()]]
///
/// ZSTD is a [CodecKind#COMPRESSION] stage and must be the last stage in any
/// pipeline that includes it.
///
/// Both encode and decode use the Zstd JNI, which requires direct [ByteBuffer]s.
public final class ZstdCodecDefinition implements ChunkCodecHandler<ZstdCodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "ZSTD";

  public static final ZstdCodecDefinition INSTANCE = new ZstdCodecDefinition();

  /// Default compression level when none is specified.
  public static final int DEFAULT_LEVEL = 3;

  /// Sanity cap on decompressedSize read from the (untrusted) Zstd frame header to prevent
  /// DoS-on-corrupt-segment via a giant pre-allocation. 1 GiB is well above any realistic chunk size.
  private static final long MAX_REASONABLE_DECOMPRESSED_SIZE = 1L << 30;

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
    int minLevel = Zstd.minCompressionLevel();
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
    long bound = Zstd.compressBound(directSrc.remaining());
    if (bound > Integer.MAX_VALUE) {
      throw new IOException("Zstd compressBound " + bound + " exceeds Integer.MAX_VALUE for input of "
          + directSrc.remaining() + " bytes");
    }
    ByteBuffer out = ByteBuffer.allocateDirect((int) bound);
    long result = Zstd.compress(out, directSrc, options.getLevel());
    if (Zstd.isError(result)) {
      throw new IOException("Zstd compression failed: " + Zstd.getErrorName(result));
    }
    out.flip();
    return out;
  }

  @Override
  public ByteBuffer decode(Options options, CodecContext ctx, ByteBuffer src) throws IOException {
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    long decompressedSize = Zstd.decompressedSize(directSrc);
    if (decompressedSize <= 0) {
      throw new IOException("Zstd: cannot determine decompressed size from frame header");
    }
    if (decompressedSize > MAX_REASONABLE_DECOMPRESSED_SIZE) {
      throw new IOException("Zstd: decompressed size " + decompressedSize + " in frame header exceeds sanity cap "
          + MAX_REASONABLE_DECOMPRESSED_SIZE + ". Segment may be corrupt.");
    }
    ByteBuffer out = ByteBuffer.allocateDirect((int) decompressedSize);
    long result = Zstd.decompress(out, directSrc);
    if (Zstd.isError(result)) {
      throw new IOException("Zstd decompression failed: " + Zstd.getErrorName(result));
    }
    out.flip();
    return out;
  }

  @Override
  public void decodeInto(Options options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) throws IOException {
    dst.clear();
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    long decompressedSize = Zstd.decompressedSize(directSrc);
    if (decompressedSize <= 0) {
      throw new IOException("Zstd: cannot determine decompressed size from frame header");
    }
    if (decompressedSize > dst.capacity()) {
      throw new IllegalArgumentException(
          "Zstd: decompressed size " + decompressedSize + " exceeds dst capacity " + dst.capacity());
    }
    long result = Zstd.decompress(dst, directSrc);
    if (Zstd.isError(result)) {
      throw new IOException("Zstd decompression failed: " + Zstd.getErrorName(result));
    }
    dst.flip();
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
