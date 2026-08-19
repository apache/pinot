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
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriterV7;
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
  private final int _chunkCapacityBytes;
  private final CodecPipelineExecutor _executor;
  private final String _canonicalSpec;
  /// Composed pipeline encoded-size bound for a full chunk, computed once at construction; every
  /// chunk except a partial final chunk reuses it instead of re-walking the pipeline stages.
  private final int _maxFullChunkEncodedSize;

  /// Returns whether the buffer has the explicit header discriminator for the codec-pipeline V7
  /// format. Keep this predicate limited to the marker: once recognized, the constructor must see
  /// and reject every corrupt structural field instead of letting the factory fall back to the
  /// legacy version-7 reader.
  public static boolean hasCodecPipelineHeader(PinotDataBuffer dataBuffer) {
    return dataBuffer.size() >= 2L * Integer.BYTES && dataBuffer.getInt(0) == VERSION
        && dataBuffer.getInt(Integer.BYTES) == FixedByteChunkForwardIndexWriterV7.FORMAT_MAGIC;
  }

  public FixedByteChunkSVForwardIndexReaderV7(PinotDataBuffer dataBuffer, DataType storedType) {
    _dataBuffer = dataBuffer;
    _storedType = storedType;

    long bufferSize = dataBuffer.size();
    if (bufferSize < FixedByteChunkForwardIndexWriterV7.FIXED_HEADER_BYTES) {
      throw new IllegalArgumentException(
          "V7 forward index is truncated: " + bufferSize + " bytes; minimum header is "
              + FixedByteChunkForwardIndexWriterV7.FIXED_HEADER_BYTES + " bytes");
    }

    if (storedType != DataType.INT && storedType != DataType.LONG) {
      throw new IllegalArgumentException(
          "FixedByteChunkSVForwardIndexReaderV7 only supports INT and LONG, got: " + storedType);
    }

    int offset = 0;
    int version = dataBuffer.getInt(offset);
    if (version != VERSION) {
      throw new IllegalArgumentException("Expected version " + VERSION + " but got " + version);
    }
    if (!hasCodecPipelineHeader(dataBuffer)) {
      throw new IllegalArgumentException(
          "Version " + VERSION + " buffer does not contain a valid codec-pipeline header discriminator");
    }
    offset += Integer.BYTES;

    int formatMagic = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    if (formatMagic != FixedByteChunkForwardIndexWriterV7.FORMAT_MAGIC) {
      throw new IllegalArgumentException("Invalid codec-pipeline format magic: " + formatMagic);
    }

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
    long chunkCapacity = (long) _numDocsPerChunk * sizeOfEntry;
    if (chunkCapacity > FixedByteChunkForwardIndexWriterV7.MAX_DECODED_CHUNK_SIZE_BYTES) {
      throw new IllegalArgumentException(
          "Decoded chunk capacity " + chunkCapacity + " bytes exceeds V7 limit "
              + FixedByteChunkForwardIndexWriterV7.MAX_DECODED_CHUNK_SIZE_BYTES + ". Segment may be corrupt.");
    }
    _chunkCapacityBytes = (int) chunkCapacity;

    _totalDocs = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    if (_totalDocs < 0) {
      throw new IllegalArgumentException("Invalid totalDocs in forward index header: " + _totalDocs);
    }

    // Validate numChunks/totalDocs/numDocsPerChunk are mutually consistent. A corrupt header
    // with mismatched values would otherwise let getChunkOffset() read past the chunk-offset table.
    int expectedNumChunks = (int) (((long) _totalDocs + _numDocsPerChunk - 1) / _numDocsPerChunk);
    if (_numChunks != expectedNumChunks) {
      throw new IllegalArgumentException(
          "Inconsistent header: numChunks=" + _numChunks + " but totalDocs=" + _totalDocs + " / numDocsPerChunk="
              + _numDocsPerChunk + " => expected " + expectedNumChunks + ". Segment may be corrupt.");
    }

    int specLength = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    if (specLength <= 0 || specLength > FixedByteChunkForwardIndexWriterV7.MAX_CODEC_SPEC_LENGTH_BYTES) {
      // V7 segments always embed a non-empty canonical codec spec; zero or negative is corruption.
      throw new IllegalArgumentException(
          "Invalid specLength in forward index header: " + specLength + "; expected [1, "
              + FixedByteChunkForwardIndexWriterV7.MAX_CODEC_SPEC_LENGTH_BYTES + "]. Segment may be corrupt.");
    }

    _dataHeaderStart = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    // Validate dataHeaderStart, specLength, and the chunk-offset table bounds before using them.
    long expectedSpecEnd = (long) offset + specLength;
    long chunkOffsetTableEnd = _dataHeaderStart + (long) _numChunks * Long.BYTES;
    if (specLength > bufferSize || _dataHeaderStart < 0 || _dataHeaderStart > bufferSize
        || expectedSpecEnd != _dataHeaderStart || chunkOffsetTableEnd > bufferSize) {
      throw new IllegalArgumentException(
          "Forward index header is corrupt: specLength=" + specLength + ", dataHeaderStart=" + _dataHeaderStart
              + ", numChunks=" + _numChunks + ", bufferSize=" + bufferSize);
    }

    // Read codec spec bytes
    byte[] specBytes = new byte[specLength];
    dataBuffer.copyTo(offset, specBytes, 0, specLength);
    _canonicalSpec = new String(specBytes, StandardCharsets.UTF_8);

    try {
      _executor = CodecPipelineExecutor.create(_canonicalSpec, storedType);
    } catch (RuntimeException e) {
      // Catch RuntimeException (not just IllegalArgumentException) so unmapped codec names,
      // native-library load failures, and any other unexpected runtime errors surface as a
      // typed segment-corruption error rather than propagating as a raw RuntimeException.
      throw new IllegalStateException(
          "Failed to initialize codec pipeline from spec '" + _canonicalSpec + "' for storedType " + storedType + ": "
              + e.getMessage(), e);
    }
    try {
      _maxFullChunkEncodedSize = _executor.maxEncodedSize(_chunkCapacityBytes,
          FixedByteChunkForwardIndexWriterV7.MAX_ENCODED_CHUNK_SIZE_BYTES,
          FixedByteChunkForwardIndexWriterV7.MAX_PIPELINE_WORK_SIZE_BYTES);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "Codec pipeline cannot safely bound a " + _chunkCapacityBytes + "-byte V7 chunk within the "
              + FixedByteChunkForwardIndexWriterV7.MAX_ENCODED_CHUNK_SIZE_BYTES
              + "-byte encoded/intermediate and "
              + FixedByteChunkForwardIndexWriterV7.MAX_PIPELINE_WORK_SIZE_BYTES
              + "-byte cumulative-work limits. Segment may be corrupt.", e);
    }

    // Validate chunk-offset monotonicity: each offset must be strictly greater than the previous
    // chunk's start. A non-monotonic table indicates segment corruption (overlapping chunks would
    // silently return wrong data otherwise).
    long prevOffset = -1L;
    long dataSectionStart = _dataHeaderStart + (long) _numChunks * Long.BYTES;
    long maxChunkOffset = bufferSize - FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES;
    for (int i = 0; i < _numChunks; i++) {
      long chunkOffset = dataBuffer.getLong(_dataHeaderStart + (long) i * Long.BYTES);
      if (chunkOffset < dataSectionStart || chunkOffset > maxChunkOffset || chunkOffset <= prevOffset) {
        throw new IllegalArgumentException(
            "Corrupt chunkOffsets[" + i + "]=" + chunkOffset + ": expected strictly increasing offsets in ["
                + dataSectionStart + ", " + maxChunkOffset + "]; previous offset was " + prevOffset);
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
    // Context holds the decoded chunk; initial capacity = one full chunk
    return new ChunkReaderContext(_chunkCapacityBytes);
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
        || chunkStart > bufferSize - FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES) {
      throw new IllegalStateException(
          "Corrupt chunkOffsets[" + chunkId + "]: " + chunkStart + " is outside the data section ["
              + dataSectionStart + ", " + bufferSize + "]. Segment may be corrupt.");
    }

    // Read per-chunk header: encodedSize (int) + decodedSize (int)
    int encodedSize = _dataBuffer.getInt(chunkStart);
    int decodedSize = _dataBuffer.getInt(chunkStart + Integer.BYTES);
    long chunkEnd = chunkId == _numChunks - 1 ? bufferSize : getChunkOffset(chunkId + 1);
    long maxEncodedRoom = chunkEnd - chunkStart - FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES;
    if (encodedSize < 0 || decodedSize < 0 || encodedSize > maxEncodedRoom) {
      throw new IllegalStateException(
          "Corrupt per-chunk header for chunk " + chunkId + ": encodedSize=" + encodedSize
              + ", decodedSize=" + decodedSize + ", remainingBuffer=" + maxEncodedRoom);
    }

    long firstDocId = (long) chunkId * _numDocsPerChunk;
    int docsInChunk = (int) Math.min(_numDocsPerChunk, _totalDocs - firstDocId);
    int expectedDecodedSize = docsInChunk * _storedType.size();
    if (decodedSize != expectedDecodedSize) {
      throw new IllegalStateException(
          "Corrupt per-chunk header for chunk " + chunkId + ": decodedSize=" + decodedSize
              + " but expected " + expectedDecodedSize + " for " + docsInChunk + " " + _storedType
              + " values");
    }

    int maxEncodedSize;
    if (expectedDecodedSize == _chunkCapacityBytes) {
      // Full chunk: reuse the bound computed (and validated) in the constructor.
      maxEncodedSize = _maxFullChunkEncodedSize;
    } else {
      try {
        maxEncodedSize = _executor.maxEncodedSize(expectedDecodedSize,
            FixedByteChunkForwardIndexWriterV7.MAX_ENCODED_CHUNK_SIZE_BYTES,
            FixedByteChunkForwardIndexWriterV7.MAX_PIPELINE_WORK_SIZE_BYTES);
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException(
            "Codec pipeline cannot represent a bounded chunk of " + expectedDecodedSize + " bytes", e);
      }
    }
    if (encodedSize > maxEncodedSize) {
      throw new IllegalStateException(
          "Corrupt per-chunk header for chunk " + chunkId + ": encodedSize=" + encodedSize
              + " exceeds pipeline bound " + maxEncodedSize + " for " + expectedDecodedSize
              + " decoded bytes");
    }

    ByteBuffer encoded = _dataBuffer.toDirectByteBuffer(
        chunkStart + FixedByteChunkForwardIndexWriterV7.CHUNK_HEADER_BYTES, encodedSize);
    ByteBuffer contextBuf = context.getChunkBuffer();
    if (decodedSize > contextBuf.capacity()) {
      throw new IllegalStateException(
          "Decoded chunk " + chunkId + " is " + decodedSize + " bytes but context buffer capacity is "
              + contextBuf.capacity());
    }
    // Invalidate chunkId BEFORE decode — if decoding or the size assertion throws,
    // contextBuf may be partially mutated. The next getChunkBuffer call must re-load this chunk
    // rather than seeing a stale cache hit on the partial state.
    context.setChunkId(-1);
    try {
      // Decode directly into the reusable context buffer — avoids intermediate allocation + copy.
      _executor.decode(encoded, contextBuf, decodedSize,
          FixedByteChunkForwardIndexWriterV7.MAX_ENCODED_CHUNK_SIZE_BYTES,
          FixedByteChunkForwardIndexWriterV7.MAX_PIPELINE_WORK_SIZE_BYTES);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to decode chunk " + chunkId + " with spec '" + _canonicalSpec + "'", e);
    }
    if (contextBuf.remaining() != decodedSize) {
      throw new IllegalStateException(
          "Chunk " + chunkId + ": decoded size " + contextBuf.remaining() + " does not match header value "
              + decodedSize);
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
