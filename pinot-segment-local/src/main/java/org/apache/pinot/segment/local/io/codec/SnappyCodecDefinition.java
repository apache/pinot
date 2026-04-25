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
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecOptions;
import org.xerial.snappy.Snappy;


/// Compression codec backed by Snappy (via the `org.xerial.snappy` JNI library).
///
/// DSL form: `SNAPPY` — no configuration options.
///
/// Snappy is a [CodecKind#COMPRESSION] stage and must be the last stage in any pipeline
/// that includes it.
///
/// The name {@value #NAME} is a frozen on-disk API contract stored verbatim in segment file
/// headers. It must never be changed or reused for a different algorithm.
///
/// Note: [#decodeInto()] requires a direct [ByteBuffer] for `dst` because the
/// Snappy JNI `uncompress(ByteBuffer, ByteBuffer)` overload requires both buffers to be direct.
/// Also, that JNI overload writes at `dst.position()` but does *not* advance it; the
/// returned byte count is used to set `dst.limit()` explicitly.
public final class SnappyCodecDefinition implements ChunkCodecHandler<SnappyCodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "SNAPPY";

  public static final SnappyCodecDefinition INSTANCE = new SnappyCodecDefinition();

  /// Singleton options — Snappy has no configurable parameters.
  public static final Options OPTIONS = new Options();

  private SnappyCodecDefinition() {
  }

  /// Typed options for [SnappyCodecDefinition]. Snappy has no configurable parameters.
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
      throw new IllegalArgumentException("SNAPPY codec does not accept arguments, got: " + args);
    }
    return OPTIONS;
  }

  @Override
  public void validateContext(Options options, CodecContext ctx) {
    // Snappy can compress any data type; no restriction
  }

  @Override
  public String canonicalize(Options options) {
    return NAME;
  }

  @Override
  public ByteBuffer encode(Options options, CodecContext ctx, ByteBuffer src) throws IOException {
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    int maxSize = Snappy.maxCompressedLength(directSrc.remaining());
    ByteBuffer out = ByteBuffer.allocateDirect(maxSize);
    int compressedSize = Snappy.compress(directSrc, out);
    out.limit(compressedSize);
    out.position(0);
    return out;
  }

  @Override
  public ByteBuffer decode(Options options, CodecContext ctx, ByteBuffer src) throws IOException {
    // Snappy JNI requires direct buffers — convert before reading the size header so the heap
    // fallback path actually works (Snappy.uncompressedLength does not accept heap input).
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    int decompressedSize = Snappy.uncompressedLength(directSrc.duplicate());
    if (decompressedSize < 0) {
      throw new IOException("Snappy: invalid decompressed size in header: " + decompressedSize + ". Segment may be"
          + " corrupt.");
    }
    ByteBuffer out = ByteBuffer.allocateDirect(decompressedSize);
    // Snappy JNI writes at dst.position() but does NOT advance it; use returned count to set limit
    int written = Snappy.uncompress(directSrc, out);
    if (written != decompressedSize) {
      throw new IOException(
          "Snappy decode size mismatch: expected " + decompressedSize + ", got " + written + ". Segment may be"
              + " corrupt.");
    }
    out.limit(written);
    out.position(0);
    return out;
  }

  @Override
  public void decodeInto(Options options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) throws IOException {
    dst.clear();
    // Snappy JNI requires direct buffers — convert before reading the size header.
    ByteBuffer directSrc = CodecBufferUtils.toDirectBuffer(src);
    int decompressedSize = Snappy.uncompressedLength(directSrc.duplicate());
    if (decompressedSize < 0) {
      throw new IOException("Snappy: invalid decompressed size in header: " + decompressedSize + ". Segment may be"
          + " corrupt.");
    }
    if (decompressedSize > dst.capacity()) {
      throw new IllegalArgumentException(
          "Snappy: decompressed size " + decompressedSize + " exceeds dst capacity " + dst.capacity());
    }
    // Snappy JNI writes at dst.position() but does NOT advance it; use returned count to set limit
    int written = Snappy.uncompress(directSrc, dst);
    if (written != decompressedSize) {
      throw new IOException(
          "Snappy decode size mismatch: expected " + decompressedSize + ", got " + written + ". Segment may be"
              + " corrupt.");
    }
    dst.position(0);
    dst.limit(written);
  }

  @Override
  public int maxEncodedSize(Options options, int inputSize) {
    return Snappy.maxCompressedLength(inputSize);
  }

  @Override
  public boolean requiresDirectDstBuffer() {
    return true;
  }
}
