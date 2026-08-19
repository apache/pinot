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
package org.apache.pinot.segment.local.io.writer.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.segment.local.io.codec.CodecPipelineExecutor;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.memory.CleanerUtil;


/// Chunk-based raw (non-dictionary-encoded) forward index writer for single-value fixed-width
/// columns (INT, LONG) that uses a [CodecPipelineExecutor] for encoding.
///
/// This writer introduces **version 7** of the fixed-byte chunk raw forward index
/// format.  The on-disk layout is:
///
/// ```
/// File header:
///   version              (int, value = 7)
///   formatMagic          (int, value = 0xC0DEC0DE)
///   numChunks            (int)
///   numDocsPerChunk      (int, normalised to power-of-2)
///   sizeOfEntry          (int, bytes per logical value, e.g. 4 for INT)
///   totalDocs            (int)
///   codecSpecLength      (int, byte length of the UTF-8 encoded canonical codec spec)
///   dataHeaderStart      (int, byte offset from file start where chunk-offset table begins)
///   codecSpec            (byte[], UTF-8 encoded canonical spec, length = codecSpecLength)
///   chunkOffsets         (long[numChunks], absolute byte offset of each chunk's per-chunk header)
/// Data (per chunk):
///   encodedSize          (int, byte length of the encoded payload that follows)
///   decodedSize          (int, byte length of the original decoded chunk data)
///   payload              (byte[], encoded chunk data, length = encodedSize)
/// ```
///
/// Each chunk contains `numDocsPerChunk` values encoded by the pipeline.  Chunk offsets
/// are 8-byte longs to support files larger than 2 GB.  The per-chunk size header allows readers
/// to verify decoded output and to skip/read chunks without scanning adjacent offsets.
///
/// This class is *not* thread-safe.
@NotThreadSafe
public class FixedByteChunkForwardIndexWriterV7 implements FixedByteValueWriter {

  public static final int VERSION = ForwardIndexConfig.CODEC_PIPELINE_WRITER_VERSION;
  public static final int FORMAT_MAGIC = 0xC0DEC0DE;

  /// Upper bound for a canonical codec spec embedded in the header. This prevents a corrupt
  /// header from driving an unbounded allocation in the reader while leaving ample room for
  /// practical pipelines.
  public static final int MAX_CODEC_SPEC_LENGTH_BYTES = 64 * 1024;

  /// Maximum decoded bytes in one V7 chunk. The normal Pinot target is 1 MiB; this 64 MiB ceiling
  /// bounds per-reader direct scratch and intermediate pipeline buffers for corrupt segments.
  public static final int MAX_DECODED_CHUNK_SIZE_BYTES = 64 * 1024 * 1024;

  /// Maximum conservative encoded-size bound for every stage in a V7 pipeline. Writers reject a
  /// pipeline/chunk-size combination whose composed bound exceeds this ceiling, so readers can
  /// allocate bounded scratch without accepting a file that the same writer could not read back.
  public static final int MAX_ENCODED_CHUNK_SIZE_BYTES = 128 * 1024 * 1024;

  /// Maximum sum of all stage-output bounds for one chunk. This prevents a long pipeline of
  /// individually bounded transforms from causing unbounded allocation and CPU churn.
  public static final long MAX_PIPELINE_WORK_SIZE_BYTES = 256L * 1024 * 1024;

  /// Bytes written before each chunk payload: encodedSize (int) + decodedSize (int).
  public static final int CHUNK_HEADER_BYTES = 2 * Integer.BYTES;

  // Number of fixed int fields before the codec spec: version, formatMagic, numChunks,
  // numDocsPerChunk, sizeOfEntry, totalDocs, codecSpecLength, dataHeaderStart
  private static final int FIXED_HEADER_INT_COUNT = 8;
  public static final int FIXED_HEADER_BYTES = FIXED_HEADER_INT_COUNT * Integer.BYTES;

  // Hold both the RAF and its FileChannel: closing the channel closes the underlying FD, but
  // some JVM finalizers close the FD when the RAF becomes unreachable. Holding the RAF as a
  // field anchors it to the writer's lifetime and removes any reliance on finalizer ordering.
  private final RandomAccessFile _raf;
  private final FileChannel _dataFile;
  private final CodecPipelineExecutor _executor;
  private final int _numDocsPerChunk;
  private final int _sizeOfEntry;
  private final int _chunkFullBytes;
  private final ByteBuffer _header;
  private final ByteBuffer _chunkBuffer;
  private final ByteBuffer _chunkHeaderBuffer = ByteBuffer.allocateDirect(CHUNK_HEADER_BYTES);
  private final int _numChunks;
  private final int _totalDocs;

  private long _dataOffset;
  private int _docsWritten;
  private int _chunksWritten;
  private boolean _trackUncompressedValueSize;

  /// Creates a new writer.
  ///
  /// @param file            output file
  /// @param executor        pre-validated pipeline executor
  /// @param totalDocs       total number of documents to write
  /// @param numDocsPerChunk target documents per chunk (will be rounded up to power-of-2)
  /// @param sizeOfEntry     bytes per value (e.g. 4 for INT, 8 for LONG)
  public FixedByteChunkForwardIndexWriterV7(File file, CodecPipelineExecutor executor, int totalDocs,
      int numDocsPerChunk, int sizeOfEntry)
      throws IOException {
    if (totalDocs < 0) {
      throw new IllegalArgumentException("totalDocs must be non-negative, got: " + totalDocs);
    }
    _executor = executor;
    if (sizeOfEntry != Integer.BYTES && sizeOfEntry != Long.BYTES) {
      throw new IllegalArgumentException("sizeOfEntry must be 4 (INT) or 8 (LONG), got: " + sizeOfEntry);
    }
    _sizeOfEntry = sizeOfEntry;
    _totalDocs = totalDocs;
    _numDocsPerChunk = normalizePower2(numDocsPerChunk);
    long chunkSizeLong = (long) sizeOfEntry * _numDocsPerChunk;
    if (chunkSizeLong > MAX_DECODED_CHUNK_SIZE_BYTES) {
      throw new IllegalArgumentException(
          "Decoded chunk size " + chunkSizeLong + " bytes exceeds V7 limit "
              + MAX_DECODED_CHUNK_SIZE_BYTES + ". Reduce numDocsPerChunk.");
    }
    _chunkFullBytes = (int) chunkSizeLong;
    try {
      executor.maxEncodedSize(_chunkFullBytes, MAX_ENCODED_CHUNK_SIZE_BYTES,
          MAX_PIPELINE_WORK_SIZE_BYTES);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "Codec pipeline cannot safely bound a " + _chunkFullBytes + "-byte V7 chunk within the "
              + MAX_ENCODED_CHUNK_SIZE_BYTES + "-byte encoded/intermediate and "
              + MAX_PIPELINE_WORK_SIZE_BYTES + "-byte cumulative-work limits. Reduce numDocsPerChunk or the number"
              + " of codec stages.", e);
    }
    _numChunks = (int) (((long) totalDocs + _numDocsPerChunk - 1) / _numDocsPerChunk);
    _docsWritten = 0;
    _chunksWritten = 0;

    byte[] specBytes = executor.getCanonicalSpec().getBytes(StandardCharsets.UTF_8);
    if (specBytes.length > MAX_CODEC_SPEC_LENGTH_BYTES) {
      throw new IllegalArgumentException(
          "Canonical codec spec is " + specBytes.length + " bytes; maximum is " + MAX_CODEC_SPEC_LENGTH_BYTES);
    }

    // Header layout:
    //   8 ints of fixed fields
    //   specBytes.length bytes of codec spec
    //   numChunks longs of chunk offsets
    long fixedHeaderBytesLong = FIXED_HEADER_BYTES;
    long dataHeaderStartLong = fixedHeaderBytesLong + specBytes.length;
    long chunkOffsetTableBytesLong = (long) _numChunks * Long.BYTES;
    long totalHeaderBytesLong = dataHeaderStartLong + chunkOffsetTableBytesLong;
    if (totalHeaderBytesLong > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Header size " + totalHeaderBytesLong + " bytes exceeds Integer.MAX_VALUE. Reduce totalDocs or"
              + " increase numDocsPerChunk.");
    }
    int dataHeaderStart = (int) dataHeaderStartLong;
    int totalHeaderBytes = (int) totalHeaderBytesLong;

    _header = ByteBuffer.allocateDirect(totalHeaderBytes);
    _header.putInt(VERSION);
    _header.putInt(FORMAT_MAGIC);
    _header.putInt(_numChunks);
    _header.putInt(_numDocsPerChunk);
    _header.putInt(sizeOfEntry);
    _header.putInt(totalDocs);
    _header.putInt(specBytes.length);
    _header.putInt(dataHeaderStart);
    _header.put(specBytes);
    // chunk offsets will be filled in during writeChunk() calls

    _dataOffset = totalHeaderBytes;

    // Open file first, then allocate the direct buffer under a try/catch so that an OOM during
    // allocation closes the already-open file descriptor (the caller has no reference to a
    // partially-constructed object and cannot invoke close() itself).
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    FileChannel channel = raf.getChannel();
    try {
      _chunkBuffer = ByteBuffer.allocateDirect((int) chunkSizeLong);
    } catch (Throwable t) {
      try {
        raf.close();
      } catch (IOException closeEx) {
        t.addSuppressed(closeEx);
      }
      throw t;
    }
    _raf = raf;
    _dataFile = channel;
  }

  /// Writes a 4-byte integer value.
  @Override
  public void putInt(int value) {
    checkRoomForOneMore();
    _chunkBuffer.putInt(value);
    _docsWritten++;
    flushIfNeeded();
  }

  /// Writes an 8-byte long value.
  @Override
  public void putLong(long value) {
    checkRoomForOneMore();
    _chunkBuffer.putLong(value);
    _docsWritten++;
    flushIfNeeded();
  }

  /// The V7 codec-pipeline transforms (DELTA/DELTADELTA/T64/GORILLA) are defined for integral
  /// INT/LONG values only, so FLOAT is not supported by this writer.
  @Override
  public void putFloat(float value) {
    throw new UnsupportedOperationException("V7 codec-pipeline writer does not support FLOAT");
  }

  /// See [#putFloat] — DOUBLE is likewise unsupported by the V7 codec-pipeline writer.
  @Override
  public void putDouble(double value) {
    throw new UnsupportedOperationException("V7 codec-pipeline writer does not support DOUBLE");
  }

  @Override
  public long getRawForwardIndexUncompressedValueSizeInBytes() {
    return _trackUncompressedValueSize ? (long) _docsWritten * _sizeOfEntry : -1;
  }

  @Override
  public void enableRawForwardIndexUncompressedValueSizeTracking() {
    if (_docsWritten != 0) {
      throw new IllegalStateException("Uncompressed-size tracking must be enabled before writing values");
    }
    _trackUncompressedValueSize = true;
  }

  /// Fail fast at write time if the caller would exceed the declared `totalDocs`. Without this
  /// guard the writer keeps producing chunks past the declared length and only `close()` catches
  /// the mismatch, leaving a semantically-invalid partial file behind.
  private void checkRoomForOneMore() {
    if (_docsWritten >= _totalDocs) {
      throw new IllegalStateException(
          "Cannot write past declared totalDocs=" + _totalDocs + " (already wrote " + _docsWritten + ")");
    }
  }

  private void flushIfNeeded() {
    if (_chunkBuffer.position() == _chunkFullBytes) {
      writeChunk();
    }
  }

  private void writeChunk() {
    _chunkBuffer.flip();
    int decodedSize = _chunkBuffer.remaining();
    ByteBuffer encoded = null;
    try {
      encoded = _executor.encode(_chunkBuffer);
      int encodedSize = encoded.remaining();

      // Per-chunk header: encodedSize (int) + decodedSize (int)
      _chunkHeaderBuffer.clear();
      _chunkHeaderBuffer.putInt(encodedSize);
      _chunkHeaderBuffer.putInt(decodedSize);
      _chunkHeaderBuffer.flip();

      // Record chunk's starting offset (points to per-chunk header) in the file header
      _header.putLong(_dataOffset);
      writeFully(_chunkHeaderBuffer, _dataOffset);
      writeFully(encoded, _dataOffset + CHUNK_HEADER_BYTES);
      _chunksWritten++;
      _dataOffset += CHUNK_HEADER_BYTES + encodedSize;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode/write data chunk " + _chunksWritten, e);
    } finally {
      CleanerUtil.cleanQuietly(encoded);
    }
    _chunkBuffer.clear();
  }

  @Override
  public void close()
      throws IOException {
    try {
      if (_chunkBuffer.position() > 0) {
        writeChunk();
      }
      if (_docsWritten != _totalDocs) {
        throw new IllegalStateException(
            "Expected " + _totalDocs + " docs but only " + _docsWritten + " were written");
      }
      if (_chunksWritten != _numChunks) {
        throw new IllegalStateException(
            "Expected " + _numChunks + " chunks but wrote " + _chunksWritten);
      }
      _header.flip();
      writeFully(_header, 0);
    } finally {
      // Close the RAF (which closes its FileChannel) so the underlying file descriptor is released
      // by an explicit call rather than relying on JVM finalizers.
      try {
        _raf.close();
      } finally {
        CleanerUtil.cleanQuietly(_header);
        CleanerUtil.cleanQuietly(_chunkBuffer);
        CleanerUtil.cleanQuietly(_chunkHeaderBuffer);
      }
    }
  }

  /// Writes all remaining bytes from `buf` starting at `position`, looping on short writes.
  private void writeFully(ByteBuffer buf, long position)
      throws IOException {
    long pos = position;
    while (buf.hasRemaining()) {
      int written = _dataFile.write(buf, pos);
      pos += written;
    }
  }

  // -------------------------------------------------------------------------

  /// Rounds `n` up to the next power of two (or returns `n` if already a power of two).
  private static int normalizePower2(int n) {
    if (n <= 0) {
      throw new IllegalArgumentException("numDocsPerChunk must be positive, got: " + n);
    }
    if (n > (1 << 30)) {
      throw new IllegalArgumentException(
          "numDocsPerChunk too large (max 2^30 = " + (1 << 30) + "), got: " + n);
    }
    if ((n & (n - 1)) == 0) {
      return n;
    }
    return 1 << (32 - Integer.numberOfLeadingZeros(n - 1));
  }
}
