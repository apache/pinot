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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import net.jpountz.lz4.LZ4CompressorWithLength;
import net.jpountz.lz4.LZ4DecompressorWithLength;
import net.jpountz.lz4.LZ4Factory;
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecOptions;


/// Compression codec backed by LZ4 (length-prefixed variant).
///
/// DSL form: `LZ4` — no configuration options; the length-prefixed format is always used
/// so that decompression does not require the original uncompressed size out-of-band.
///
/// LZ4 is a [CodecKind#COMPRESSION] stage and must be the last stage in any pipeline
/// that includes it.
///
/// The name {@value #NAME} is a frozen on-disk API contract stored verbatim in segment file
/// headers. It must never be changed or reused for a different algorithm.
public final class Lz4CodecDefinition implements ChunkCodecHandler<Lz4CodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "LZ4";

  public static final Lz4CodecDefinition INSTANCE = new Lz4CodecDefinition();

  /// Singleton options — LZ4 has no configurable parameters.
  public static final Options OPTIONS = new Options();

  /// Sanity cap on decompressedLength read from the (untrusted) LZ4 length-prefix header to prevent
  /// DoS-on-corrupt-segment via a giant pre-allocation. 1 GiB is well above any realistic chunk size.
  private static final int MAX_REASONABLE_DECOMPRESSED_SIZE = 1 << 30;

  /// Lazy holder so a missing/broken LZ4 native library only fails when LZ4 is actually used,
  /// rather than at [CodecRegistry#DEFAULT] class-init time (which would break every
  /// consumer of the registry, including consumers that never use LZ4).
  private static final class Native {
    static final LZ4Factory FACTORY = LZ4Factory.fastestInstance();
    static final LZ4CompressorWithLength COMPRESSOR = new LZ4CompressorWithLength(FACTORY.fastCompressor());
    static final LZ4DecompressorWithLength DECOMPRESSOR = new LZ4DecompressorWithLength(FACTORY.safeDecompressor());
  }

  private Lz4CodecDefinition() {
  }

  /// Typed options for [Lz4CodecDefinition]. LZ4 has no configurable parameters.
  public static final class Options implements CodecOptions {
    private Options() {
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
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("LZ4 codec does not accept arguments, got: " + args);
    }
    return OPTIONS;
  }

  @Override
  public void validateContext(Options options, CodecContext ctx) {
    // LZ4 can compress any data type; no restriction
  }

  @Override
  public String canonicalize(Options options) {
    return NAME;
  }

  @Override
  public ByteBuffer encode(Options options, CodecContext ctx, ByteBuffer src) throws IOException {
    // LZ4 JNI requires both buffers be direct; mirror the Snappy/Zstd defensive copy.
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    int maxSize = Native.COMPRESSOR.maxCompressedLength(directSrc.remaining());
    ByteBuffer out = ByteBuffer.allocateDirect(maxSize);
    Native.COMPRESSOR.compress(directSrc, out);
    out.flip();
    return out;
  }

  @Override
  public ByteBuffer decode(Options options, CodecContext ctx, ByteBuffer src) throws IOException {
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    int decompressedLength = LZ4DecompressorWithLength.getDecompressedLength(directSrc);
    if (decompressedLength < 0 || decompressedLength > MAX_REASONABLE_DECOMPRESSED_SIZE) {
      throw new IOException("LZ4: decompressed length " + decompressedLength
          + " in length prefix is out of range [0, " + MAX_REASONABLE_DECOMPRESSED_SIZE + "]. Segment may be corrupt.");
    }
    ByteBuffer out = ByteBuffer.allocateDirect(decompressedLength);
    Native.DECOMPRESSOR.decompress(directSrc, out);
    if (out.position() != decompressedLength) {
      throw new IOException(
          "LZ4 decoded " + out.position() + " bytes but expected " + decompressedLength + ". Segment may be corrupt.");
    }
    out.flip();
    return out;
  }

  @Override
  public void decodeInto(Options options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) throws IOException {
    dst.clear();
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    int decompressedLength = LZ4DecompressorWithLength.getDecompressedLength(directSrc);
    if (decompressedLength < 0 || decompressedLength > MAX_REASONABLE_DECOMPRESSED_SIZE) {
      throw new IOException("LZ4: decompressed length " + decompressedLength
          + " in length prefix is out of range [0, " + MAX_REASONABLE_DECOMPRESSED_SIZE + "]. Segment may be corrupt.");
    }
    if (decompressedLength > dst.capacity()) {
      throw new IllegalArgumentException(
          "LZ4: decompressed size " + decompressedLength + " exceeds dst capacity " + dst.capacity());
    }
    Native.DECOMPRESSOR.decompress(directSrc, dst);
    if (dst.position() != decompressedLength) {
      throw new IOException(
          "LZ4 decoded " + dst.position() + " bytes but expected " + decompressedLength + ". Segment may be corrupt.");
    }
    dst.flip();
  }

  @Override
  public int maxEncodedSize(Options options, int inputSize) {
    // LZ4CompressorWithLength adds a 4-byte length prefix before the compressed payload
    return Native.FACTORY.fastCompressor().maxCompressedLength(inputSize) + Integer.BYTES;
  }

  @Override
  public boolean requiresDirectDstBuffer() {
    return false;
  }
}
