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
import java.nio.ByteOrder;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.apache.pinot.segment.spi.memory.CleanerUtil;


/// Legacy `GZIP`-named compression codec backed by the default [java.util.zip.Deflater], which
/// produces a zlib-wrapped DEFLATE stream rather than RFC 1952 `.gz` framing.
///
/// DSL form: `GZIP` — no configuration options.
///
/// GZIP is a [CodecKind#COMPRESSION] stage. Compression stages may be chained after all
/// transforms.
///
/// The name {@value #NAME} is a frozen on-disk API contract stored verbatim in segment file
/// headers. It must never be changed or reused for a different algorithm.
///
/// Wire format: zlib-wrapped DEFLATE payload (including the zlib checksum) followed by a 4-byte
/// big-endian footer containing the uncompressed byte count. The footer allows decompression
/// without knowing the original size out-of-band.
///
/// **Performance note:** The JDK [Deflater]/[Inflater] instances are reused per thread and
/// operate directly on [ByteBuffer] inputs and outputs. For write-intensive workloads prefer
/// LZ4 or ZSTD.
final class GzipCodecDefinition implements ChunkCodecHandler<GzipCodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "GZIP";

  /// Thread-local Deflater/Inflater: reset() between uses to amortize JNI allocation cost.
  /// Note: these hold native resources that are only released when the thread dies. For long-lived
  /// server thread pools this is bounded by the worker count and acceptable.
  private static final ThreadLocal<Deflater> DEFLATER = ThreadLocal.withInitial(Deflater::new);
  private static final ThreadLocal<Inflater> INFLATER = ThreadLocal.withInitial(Inflater::new);
  private static final ThreadLocal<byte[]> COMPLETION_PROBE = ThreadLocal.withInitial(() -> new byte[1]);

  public static final GzipCodecDefinition INSTANCE = new GzipCodecDefinition();

  /// Singleton options — GZIP has no configurable parameters.
  public static final Options OPTIONS = new Options();

  private GzipCodecDefinition() {
  }

  /// Typed options for [GzipCodecDefinition]. GZIP has no configurable parameters.
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
      throw new IllegalArgumentException("GZIP codec does not accept arguments, got: " + args);
    }
    return OPTIONS;
  }

  @Override
  public void validateContext(Options options, CodecContext ctx) {
    // GZIP can compress any data type; no restriction
  }

  @Override
  public String canonicalize(Options options) {
    return NAME;
  }

  @Override
  public ByteBuffer encode(Options options, CodecContext ctx, ByteBuffer src) throws IOException {
    int uncompressedSize = src.remaining();
    ByteBuffer out = ByteBuffer.allocateDirect(maxEncodedSize(options, uncompressedSize));
    Deflater deflater = DEFLATER.get();
    boolean succeeded = false;
    try {
      out.limit(out.capacity() - Integer.BYTES);
      deflater.reset();
      deflater.setInput(src.duplicate());
      deflater.finish();
      while (!deflater.finished()) {
        if (!out.hasRemaining()) {
          throw new IOException("GZIP encode exceeded maximum encoded size " + out.capacity()
              + " before deflater finished. Segment build aborted.");
        }
        int encoded = deflater.deflate(out);
        if (encoded == 0 && !deflater.finished()) {
          throw new IOException("GZIP deflater made no progress before finishing");
        }
      }
      out.limit(out.capacity());
      out.putInt(uncompressedSize);
      out.flip();
      succeeded = true;
      return out;
    } finally {
      deflater.reset();
      if (!succeeded) {
        CleanerUtil.cleanQuietly(out);
      }
    }
  }

  @Override
  public ByteBuffer decode(Options options, CodecContext ctx, ByteBuffer src) throws IOException {
    int decompressedSize = readDecompressedSize(src);
    ByteBuffer out = ByteBuffer.allocateDirect(decompressedSize);
    boolean succeeded = false;
    try {
      inflateInto(src, out, decompressedSize);
      succeeded = true;
      return out;
    } finally {
      if (!succeeded) {
        CleanerUtil.cleanQuietly(out);
      }
    }
  }

  @Override
  public void decodeInto(Options options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) throws IOException {
    dst.clear();
    int decompressedSize = readDecompressedSize(src);
    if (decompressedSize > dst.capacity()) {
      throw new IllegalArgumentException(
          "GZIP: decompressed size " + decompressedSize + " exceeds dst capacity " + dst.capacity());
    }
    inflateInto(src, dst, decompressedSize);
  }

  @Override
  public int maxEncodedSize(Options options, int inputSize) {
    // DEFLATE worst-case expansion + 4-byte appended uncompressed-size footer
    if (inputSize < 0) {
      throw new IllegalArgumentException("GZIP inputSize must be non-negative: " + inputSize);
    }
    long bound = (long) inputSize + (inputSize >> 12) + (inputSize >> 14) + (inputSize >> 25)
        + 13 + Integer.BYTES;
    if (bound > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("GZIP maximum encoded size exceeds Integer.MAX_VALUE: " + bound);
    }
    return (int) bound;
  }

  @Override
  public boolean requiresDirectDstBuffer() {
    return false;
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  private static int readDecompressedSize(ByteBuffer src) throws IOException {
    int payloadLimit = src.limit();
    int payloadSize = src.remaining();
    if (payloadSize < Integer.BYTES) {
      throw new IOException("GZIP payload too short to contain uncompressed-size footer: " + payloadSize + " bytes");
    }
    int decompressedSize =
        src.duplicate().order(ByteOrder.BIG_ENDIAN).getInt(payloadLimit - Integer.BYTES);
    if (decompressedSize < 0) {
      throw new IOException("GZIP: invalid decompressed size in footer: " + decompressedSize);
    }
    return CodecBufferUtils.checkDeclaredDecompressedSize(decompressedSize, "GZIP", "footer");
  }

  private static void inflateInto(ByteBuffer src, ByteBuffer dst, int decompressedSize) throws IOException {
    ByteBuffer compressed = src.duplicate();
    compressed.limit(src.limit() - Integer.BYTES);
    Inflater inflater = INFLATER.get();
    inflater.reset();
    try {
      inflater.setInput(compressed);
      dst.limit(decompressedSize);
      while (dst.position() < decompressedSize) {
        int n;
        try {
          n = inflater.inflate(dst);
        } catch (DataFormatException e) {
          throw new IOException("GZIP decompression failed", e);
        }
        if (n == 0) {
          if (inflater.finished()) {
            break;
          }
          if (inflater.needsInput()) {
            throw new IOException(
                "GZIP inflater ran out of input before producing " + decompressedSize + " bytes (produced "
                    + dst.position() + ")");
          }
          if (inflater.needsDictionary()) {
            throw new IOException("GZIP inflater requires a preset dictionary (not supported)");
          }
          throw new IOException(
              "GZIP inflater returned 0 bytes with no known cause (inflated so far: " + dst.position() + " / "
                  + decompressedSize + ")");
        }
      }
      if (dst.position() != decompressedSize) {
        throw new IOException("GZIP: inflated " + dst.position() + " bytes but expected " + decompressedSize);
      }

      // Filling the caller-declared output size does not prove the DEFLATE stream is complete: a
      // corrupt footer can under-report the real output, including zero, and the loop above would
      // otherwise accept a truncated prefix without consuming or validating the stream checksum.
      byte[] completionProbe = COMPLETION_PROBE.get();
      while (!inflater.finished()) {
        int n;
        try {
          n = inflater.inflate(completionProbe);
        } catch (DataFormatException e) {
          throw new IOException("GZIP decompression failed while validating stream completion", e);
        }
        if (n > 0) {
          throw new IOException(
              "GZIP stream expands beyond footer-declared size " + decompressedSize + ". Segment may be corrupt.");
        }
        if (inflater.finished()) {
          break;
        }
        if (inflater.needsInput()) {
          throw new IOException("GZIP stream ended before its checksum/trailer. Segment may be truncated.");
        }
        if (inflater.needsDictionary()) {
          throw new IOException("GZIP inflater requires a preset dictionary (not supported)");
        }
        throw new IOException("GZIP inflater did not finish after producing the footer-declared output size");
      }
      if (inflater.getRemaining() != 0) {
        throw new IOException(
            "GZIP stream has " + inflater.getRemaining() + " trailing compressed bytes. Segment may be corrupt.");
      }
      dst.flip();
    } finally {
      inflater.reset();
    }
  }
}
