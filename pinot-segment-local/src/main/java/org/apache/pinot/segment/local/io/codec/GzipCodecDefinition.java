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
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecOptions;


/// Compression codec backed by GZIP (DEFLATE via [java.util.zip.Deflater]).
///
/// DSL form: `GZIP` — no configuration options.
///
/// GZIP is a [CodecKind#COMPRESSION] stage and must be the last stage in any pipeline
/// that includes it.
///
/// The name {@value #NAME} is a frozen on-disk API contract stored verbatim in segment file
/// headers. It must never be changed or reused for a different algorithm.
///
/// Wire format: DEFLATE-compressed payload followed by a 4-byte big-endian footer containing
/// the uncompressed byte count. The footer allows decompression without knowing the original size
/// out-of-band.
///
/// **Performance note:** The JDK [Deflater]/[Inflater] API requires heap
/// `byte[]` staging buffers. Thread-local buffers are reused across calls within the same
/// thread to avoid per-chunk heap allocation, but a fresh [Deflater]/[Inflater] instance
/// is still created per call. For write-intensive workloads prefer LZ4 or ZSTD.
public final class GzipCodecDefinition implements ChunkCodecHandler<GzipCodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "GZIP";

  /// Thread-local staging buffer: reused for deflated output (encode) and inflated output (decode).
  private static final ThreadLocal<byte[]> STAGING_BUF = ThreadLocal.withInitial(() -> new byte[65536]);

  /// Thread-local buffer for the compressed-bytes copy in decode (separate from [#STAGING_BUF],
  /// which holds inflated output).
  private static final ThreadLocal<byte[]> COMPRESSED_BUF = ThreadLocal.withInitial(() -> new byte[65536]);

  /// Thread-local buffer for the source-bytes copy in encode (Deflater.setInput requires `byte[]`).
  /// Separate from STAGING_BUF (deflated output) and COMPRESSED_BUF (decode input).
  private static final ThreadLocal<byte[]> ENCODE_SRC_BUF = ThreadLocal.withInitial(() -> new byte[65536]);

  /// Thread-local Deflater/Inflater: reset() between uses to amortize JNI allocation cost.
  /// Note: these hold native resources that are only released when the thread dies. For long-lived
  /// server thread pools this is bounded by the worker count and acceptable.
  private static final ThreadLocal<Deflater> DEFLATER = ThreadLocal.withInitial(Deflater::new);
  private static final ThreadLocal<Inflater> INFLATER = ThreadLocal.withInitial(Inflater::new);

  /// Sanity cap on decompressedSize read from the (untrusted) GZIP trailer to prevent DoS-on-corrupt-segment
  /// via a giant pre-allocation. 1 GiB is well above any realistic chunk size.
  private static final int MAX_REASONABLE_DECOMPRESSED_SIZE = 1 << 30;

  /// Soft cap on the thread-local staging buffer's retained size. For chunks larger than this the
  /// helper allocates a one-shot buffer instead of growing (and pinning) the thread-local — this
  /// prevents an outlier large chunk from holding tens-of-MB of heap per worker for the JVM
  /// lifetime. 16 MiB is well above any realistic decompressed chunk size.
  private static final int MAX_RETAINED_STAGING_BUF = 16 * 1024 * 1024;

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
    int maxCompressed =
        uncompressedSize + (uncompressedSize >> 12) + (uncompressedSize >> 14) + (uncompressedSize >> 25) + 13;
    byte[] stagingBuf = ensureStagingCapacity(maxCompressed);
    // Reuse a thread-local source-bytes buffer instead of allocating a fresh byte[uncompressedSize]
    // on every encode call (Deflater.setInput requires byte[]).
    byte[] srcBytes = ensureCapacity(ENCODE_SRC_BUF, uncompressedSize);
    src.duplicate().get(srcBytes, 0, uncompressedSize);
    Deflater deflater = DEFLATER.get();
    deflater.reset();
    int compressedSize = 0;
    deflater.setInput(srcBytes, 0, uncompressedSize);
    deflater.finish();
    while (!deflater.finished()) {
      int free = stagingBuf.length - compressedSize;
      if (free <= 0) {
        // Defensive: stagingBuf was sized to maxCompressed, so this should be unreachable. If
        // deflater wants more space we'd otherwise spin forever returning 0 bytes.
        throw new IOException("GZIP encode exceeded staging buffer (" + stagingBuf.length
            + " bytes) before deflater finished. Segment build aborted.");
      }
      compressedSize += deflater.deflate(stagingBuf, compressedSize, free);
    }
    ByteBuffer out = ByteBuffer.allocateDirect(compressedSize + Integer.BYTES);
    out.put(stagingBuf, 0, compressedSize);
    out.putInt(uncompressedSize);
    out.flip();
    return out;
  }

  @Override
  public ByteBuffer decode(Options options, CodecContext ctx, ByteBuffer src) throws IOException {
    int decompressedSize = readDecompressedSize(src);
    int compressedLen = src.limit() - Integer.BYTES;
    byte[] compressedBytes = readCompressedBytes(src, compressedLen);
    byte[] decompressedBytes = inflateIntoStagingBuffer(compressedBytes, compressedLen, decompressedSize);
    ByteBuffer out = ByteBuffer.allocateDirect(decompressedSize);
    out.put(decompressedBytes, 0, decompressedSize);
    out.flip();
    return out;
  }

  @Override
  public void decodeInto(Options options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) throws IOException {
    dst.clear();
    int decompressedSize = readDecompressedSize(src);
    if (decompressedSize > dst.capacity()) {
      throw new IllegalArgumentException(
          "GZIP: decompressed size " + decompressedSize + " exceeds dst capacity " + dst.capacity());
    }
    int compressedLen = src.limit() - Integer.BYTES;
    byte[] compressedBytes = readCompressedBytes(src, compressedLen);
    byte[] decompressedBytes = inflateIntoStagingBuffer(compressedBytes, compressedLen, decompressedSize);
    dst.put(decompressedBytes, 0, decompressedSize);
    dst.flip();
  }

  @Override
  public int maxEncodedSize(Options options, int inputSize) {
    // DEFLATE worst-case expansion + 4-byte appended uncompressed-size footer
    return inputSize + (inputSize >> 12) + (inputSize >> 14) + (inputSize >> 25) + 13 + Integer.BYTES;
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
    if (payloadLimit < Integer.BYTES) {
      throw new IOException("GZIP payload too short to contain uncompressed-size footer: " + payloadLimit + " bytes");
    }
    int decompressedSize = src.getInt(payloadLimit - Integer.BYTES);
    if (decompressedSize < 0) {
      throw new IOException("GZIP: invalid decompressed size in footer: " + decompressedSize);
    }
    if (decompressedSize > MAX_REASONABLE_DECOMPRESSED_SIZE) {
      throw new IOException(
          "GZIP: decompressed size " + decompressedSize + " in footer exceeds sanity cap "
              + MAX_REASONABLE_DECOMPRESSED_SIZE + ". Segment may be corrupt.");
    }
    return decompressedSize;
  }

  private static byte[] readCompressedBytes(ByteBuffer src, int compressedLen) {
    // Use a thread-local buffer for the compressed input (separate from STAGING_BUF, which is
    // overwritten by inflateIntoStagingBuffer). Caller must use exactly compressedLen bytes.
    byte[] compressedBytes = ensureCompressedCapacity(compressedLen);
    ByteBuffer srcCopy = src.duplicate();
    srcCopy.position(0);
    srcCopy.limit(compressedLen);
    srcCopy.get(compressedBytes, 0, compressedLen);
    return compressedBytes;
  }

  /// Returns a buffer of at least `needed` bytes. Grows the thread-local up to
  /// [#MAX_RETAINED_STAGING_BUF]; for larger requests, returns a one-shot buffer that is NOT
  /// cached, so an outlier large chunk does not pin large heap per worker for the JVM lifetime.
  private static byte[] ensureCompressedCapacity(int needed) {
    return ensureCapacity(COMPRESSED_BUF, needed);
  }

  private static byte[] ensureStagingCapacity(int needed) {
    return ensureCapacity(STAGING_BUF, needed);
  }

  private static byte[] ensureCapacity(ThreadLocal<byte[]> tl, int needed) {
    if (needed > MAX_RETAINED_STAGING_BUF) {
      // Outlier; do not cache.
      return new byte[needed];
    }
    byte[] buf = tl.get();
    if (buf.length < needed) {
      buf = new byte[needed];
      tl.set(buf);
    }
    return buf;
  }

  /// Inflates `compressedBytes` into the thread-local staging buffer and returns the
  /// staging buffer reference. The returned array is shared and must be copied by the caller
  /// before any other code on the same thread runs another GZIP decode — the next
  /// `inflateIntoStagingBuffer` call will overwrite its contents.
  private static byte[] inflateIntoStagingBuffer(byte[] compressedBytes, int compressedLen, int decompressedSize)
      throws IOException {
    byte[] out = ensureStagingCapacity(decompressedSize);
    Inflater inflater = INFLATER.get();
    inflater.reset();
    int totalInflated = 0;
    inflater.setInput(compressedBytes, 0, compressedLen);
    while (totalInflated < decompressedSize) {
      int n;
      try {
        n = inflater.inflate(out, totalInflated, decompressedSize - totalInflated);
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
                  + totalInflated + ")");
        }
        if (inflater.needsDictionary()) {
          throw new IOException("GZIP inflater requires a preset dictionary (not supported)");
        }
        throw new IOException(
            "GZIP inflater returned 0 bytes with no known cause (inflated so far: " + totalInflated + " / "
                + decompressedSize + ")");
      }
      totalInflated += n;
    }
    if (totalInflated != decompressedSize) {
      throw new IOException("GZIP: inflated " + totalInflated + " bytes but expected " + decompressedSize);
    }
    return out;
  }
}
