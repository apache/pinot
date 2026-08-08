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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.segment.local.io.codec.CodecPipelineExecutor;
import org.apache.pinot.segment.local.io.codec.CodecRegistry;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriterV7;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Chunk-based single-value raw forward index reader for version-7 files written by
/// [FixedByteChunkForwardIndexWriterV7].
///
/// Reads the canonical codec spec from the file header, instantiates a
/// [CodecPipelineExecutor], and uses it to decode each chunk on demand.
///
/// Supported data types: INT, LONG.
///
/// **Threading:** the reader instance itself is immutable after construction and is safe to
/// share across threads, but each [ChunkReaderContext] is single-threaded. The
/// [ByteBuffer] returned by `getChunkBuffer` is the context's reusable scratch buffer —
/// its position/limit are mutated on every chunk transition and it must not be retained across
/// subsequent `getInt`/`getLong` calls.
public final class FixedByteChunkSVForwardIndexReaderV7 implements ForwardIndexReader<ChunkReaderContext> {

  public static final int VERSION = ForwardIndexConfig.CODEC_PIPELINE_WRITER_VERSION;

  private final PinotDataBuffer _dataBuffer;
  private final DataType _storedType;
  private final int _numChunks;
  private final int _numDocsPerChunk;
  private final int _shift; // log2(numDocsPerChunk) for fast chunk id calc
  private final int _totalDocs;
  private final int _dataHeaderStart;
  private final CodecPipelineExecutor _executor;
  private final String _canonicalSpec;

  public FixedByteChunkSVForwardIndexReaderV7(PinotDataBuffer dataBuffer, DataType storedType) {
    _dataBuffer = dataBuffer;
    _storedType = storedType;

    if (storedType != DataType.INT && storedType != DataType.LONG) {
      throw new IllegalArgumentException(
          "FixedByteChunkSVForwardIndexReaderV7 only supports INT and LONG, got: " + storedType);
    }

    int offset = 0;
    int version = dataBuffer.getInt(offset);
    if (version != VERSION) {
      throw new IllegalArgumentException("Expected version " + VERSION + " but got " + version);
    }
    offset += Integer.BYTES;

    _numChunks = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    if (_numChunks < 0) {
      throw new IllegalArgumentException("Invalid numChunks in forward index header: " + _numChunks);
    }

    _numDocsPerChunk = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    if (_numDocsPerChunk <= 0 || (_numDocsPerChunk & (_numDocsPerChunk - 1)) != 0) {
      throw new IllegalArgumentException(
          "Invalid numDocsPerChunk in forward index header: " + _numDocsPerChunk
              + ". Expected a positive power of two.");
    }
    _shift = Integer.numberOfTrailingZeros(_numDocsPerChunk);

    int sizeOfEntry = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    if (sizeOfEntry != storedType.size()) {
      throw new IllegalArgumentException(
          "Header sizeOfEntry=" + sizeOfEntry + " does not match storedType=" + storedType
              + " (expected " + storedType.size() + " bytes). Written for a different data type?");
    }

    _totalDocs = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    if (_totalDocs < 0) {
      throw new IllegalArgumentException("Invalid totalDocs in forward index header: " + _totalDocs);
    }

    // Validate numChunks/totalDocs/numDocsPerChunk are mutually consistent. A corrupt header
    // with mismatched values would otherwise let getChunkOffset() read past the chunk-offset table.
    int expectedNumChunks = (_totalDocs + _numDocsPerChunk - 1) / _numDocsPerChunk;
    if (_numChunks != expectedNumChunks) {
      throw new IllegalArgumentException(
          "Inconsistent header: numChunks=" + _numChunks + " but totalDocs=" + _totalDocs + " / numDocsPerChunk="
              + _numDocsPerChunk + " => expected " + expectedNumChunks + ". Segment may be corrupt.");
    }

    int specLength = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    if (specLength <= 0) {
      // V7 segments always embed a non-empty canonical codec spec; zero or negative is corruption.
      throw new IllegalArgumentException(
          "Invalid specLength in forward index header: " + specLength + ". Segment may be corrupt.");
    }

    _dataHeaderStart = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    // Validate dataHeaderStart, specLength, and the chunk-offset table bounds before using them.
    long bufferSize = dataBuffer.size();
    long expectedSpecEnd = (long) offset + specLength;
    long chunkOffsetTableEnd = _dataHeaderStart + (long) _numChunks * Long.BYTES;
    if (specLength > bufferSize || _dataHeaderStart < 0 || _dataHeaderStart > bufferSize
        || expectedSpecEnd > _dataHeaderStart || chunkOffsetTableEnd > bufferSize) {
      throw new IllegalArgumentException(
          "Forward index header is corrupt: specLength=" + specLength + ", dataHeaderStart=" + _dataHeaderStart
              + ", numChunks=" + _numChunks + ", bufferSize=" + bufferSize);
    }

    // Read codec spec bytes
    byte[] specBytes = new byte[specLength];
    dataBuffer.copyTo(offset, specBytes, 0, specLength);
    _canonicalSpec = new String(specBytes, StandardCharsets.UTF_8);

    try {
      _executor = CodecPipelineExecutor.create(_canonicalSpec, new CodecContext(storedType),
          CodecRegistry.DEFAULT);
    } catch (RuntimeException e) {
      // Catch RuntimeException (not just IllegalArgumentException) so unmapped codec names,
      // native-library load failures, and any other unexpected runtime errors surface as a
      // typed segment-corruption error rather than propagating as a raw RuntimeException.
      throw new IllegalStateException(
          "Failed to initialize codec pipeline from spec '" + _canonicalSpec + "' for storedType " + storedType + ": "
              + e.getMessage(), e);
    }

    // Validate chunk-offset monotonicity: each offset must be strictly greater than the previous
    // chunk's start. A non-monotonic table indicates segment corruption (overlapping chunks would
    // silently return wrong data otherwise).
    long prevOffset = -1L;
    long dataSectionStart = _dataHeaderStart + (long) _numChunks * Long.BYTES;
    for (int i = 0; i < _numChunks; i++) {
      long chunkOffset = dataBuffer.getLong(_dataHeaderStart + (long) i * Long.BYTES);
      if (chunkOffset < dataSectionStart || chunkOffset <= prevOffset) {
        throw new IllegalArgumentException(
            "Corrupt chunkOffsets[" + i + "]=" + chunkOffset + ": expected strictly increasing offsets in ["
                + dataSectionStart + ", " + bufferSize + "); previous offset was " + prevOffset);
      }
      prevOffset = chunkOffset;
    }
  }

  /// Returns the canonical codec spec stored in the file header. Implements
  /// [ForwardIndexReader#getCodecSpec()] so `ForwardIndexHandler` can detect codec-spec
  /// changes during segment reload without casting to this concrete type.
  @Override
  public String getCodecSpec() {
    return _canonicalSpec;
  }

  @Override
  public ChunkReaderContext createContext() {
    // Context holds the decompressed chunk; initial capacity = one full chunk
    return new ChunkReaderContext(_numDocsPerChunk * _storedType.size());
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getStoredType() {
    return _storedType;
  }

  @Override
  public int getInt(int docId, ChunkReaderContext context) {
    int chunkRowId = docId & (_numDocsPerChunk - 1);
    ByteBuffer chunk = getChunkBuffer(docId, context);
    return chunk.getInt(chunkRowId * Integer.BYTES);
  }

  @Override
  public long getLong(int docId, ChunkReaderContext context) {
    int chunkRowId = docId & (_numDocsPerChunk - 1);
    ByteBuffer chunk = getChunkBuffer(docId, context);
    return chunk.getLong(chunkRowId * Long.BYTES);
  }

  @Override
  public void close()
      throws IOException {
    // PinotDataBuffer is managed by the caller; nothing to close here.
  }

  // -------------------------------------------------------------------------

  private ByteBuffer getChunkBuffer(int docId, ChunkReaderContext context) {
    if (docId < 0 || docId >= _totalDocs) {
      throw new IndexOutOfBoundsException("docId " + docId + " is out of bounds [0, " + _totalDocs + ")");
    }
    int chunkId = docId >>> _shift;
    if (context.getChunkId() == chunkId) {
      return context.getChunkBuffer();
    }
    return loadChunk(chunkId, context);
  }

  private ByteBuffer loadChunk(int chunkId, ChunkReaderContext context) {
    long chunkStart = getChunkOffset(chunkId);
    // Validate the chunk offset before using it as a buffer index. Constructor validates the
    // chunk-offset table extents, but each entry's value is a per-chunk file offset that must
    // (1) point past the end of the chunk-offset table (i.e. into the data section), and
    // (2) leave room for the per-chunk header before the buffer end.
    long bufferSize = _dataBuffer.size();
    long dataSectionStart = _dataHeaderStart + (long) _numChunks * Long.BYTES;
    if (chunkStart < dataSectionStart
        || chunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES > bufferSize) {
      throw new IllegalStateException(
          "Corrupt chunkOffsets[" + chunkId + "]: " + chunkStart + " is outside the data section ["
              + dataSectionStart + ", " + bufferSize + "]. Segment may be corrupt.");
    }

    // Read per-chunk header: compressedSize (int) + uncompressedSize (int)
    int compressedSize = _dataBuffer.getInt(chunkStart);
    int uncompressedSize = _dataBuffer.getInt(chunkStart + Integer.BYTES);
    long maxCompressedRoom = bufferSize - chunkStart - FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES;
    if (compressedSize < 0 || uncompressedSize < 0 || compressedSize > maxCompressedRoom) {
      throw new IllegalStateException(
          "Corrupt per-chunk header for chunk " + chunkId + ": compressedSize=" + compressedSize
              + ", uncompressedSize=" + uncompressedSize + ", remainingBuffer=" + maxCompressedRoom);
    }

    ByteBuffer encoded = _dataBuffer.toDirectByteBuffer(
        chunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES, compressedSize);
    ByteBuffer contextBuf = context.getChunkBuffer();
    if (uncompressedSize > contextBuf.capacity()) {
      throw new IllegalStateException(
          "Decoded chunk " + chunkId + " is " + uncompressedSize + " bytes but context buffer capacity is "
              + contextBuf.capacity());
    }
    // Invalidate chunkId BEFORE the decompress — if decompress or the size assertion throws,
    // contextBuf may be partially mutated. The next getChunkBuffer call must re-load this chunk
    // rather than seeing a stale cache hit on the partial state.
    context.setChunkId(-1);
    try {
      // Decompress directly into the reusable context buffer — avoids intermediate allocation + copy.
      _executor.decompress(encoded, contextBuf);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to decode chunk " + chunkId + " with spec '" + _canonicalSpec + "'", e);
    }
    if (contextBuf.remaining() != uncompressedSize) {
      throw new IllegalStateException(
          "Chunk " + chunkId + ": decompressed size " + contextBuf.remaining() + " does not match header value "
              + uncompressedSize);
    }
    context.setChunkId(chunkId);
    // All callers (getInt/getLong) use absolute ByteBuffer.getXxx(int) which ignores position,
    // so the reusable contextBuf can be returned directly without a per-chunk duplicate().
    return contextBuf;
  }

  private long getChunkOffset(int chunkId) {
    // Each chunk offset is a long (8 bytes)
    return _dataBuffer.getLong(_dataHeaderStart + (long) chunkId * Long.BYTES);
  }
}
