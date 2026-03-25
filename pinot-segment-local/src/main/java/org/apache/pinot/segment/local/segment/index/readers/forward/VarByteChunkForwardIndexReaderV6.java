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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV6;
import org.apache.pinot.segment.local.utils.ArraySerDeUtils;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Reader for the V6 two-stream forward index format.
 *
 * <p>Each regular chunk on disk is:
 * {@code [sizeStreamCompressedLen (4 bytes LE)][compressed size stream][compressed data stream]}
 *
 * <p>The size stream decompresses to: {@code [numDocs][size0][size1]...[sizeN-1]}
 * <p>The data stream decompresses to: {@code [bytes0][bytes1]...[bytesN-1]}
 *
 * <p>Like V5, multi-value fixed-byte arrays use implicit element count.
 *
 * @see VarByteChunkForwardIndexWriterV6
 */
public class VarByteChunkForwardIndexReaderV6 extends VarByteChunkForwardIndexReaderV4 {

  public VarByteChunkForwardIndexReaderV6(PinotDataBuffer dataBuffer, FieldSpec.DataType storedType,
      boolean isSingleValue) {
    super(dataBuffer, storedType, isSingleValue);
  }

  @Override
  public int getVersion() {
    return VarByteChunkForwardIndexWriterV6.VERSION;
  }

  @Override
  public ReaderContext createContext() {
    return _chunkCompressionType == ChunkCompressionType.PASS_THROUGH
        ? new V6UncompressedReaderContext(_metadata, _chunks, _chunksStartOffset)
        : new V6CompressedReaderContext(_metadata, _chunks, _chunksStartOffset, _chunkDecompressor,
            _chunkCompressionType, _targetDecompressedChunkSize);
  }

  // V6 uses V5's MV deserialization (without length prefix for fixed-byte types)
  @Override
  public int getIntMV(int docId, int[] valueBuffer, ReaderContext context) {
    return ArraySerDeUtils.deserializeIntArrayWithoutLength(context.getValue(docId), valueBuffer);
  }

  @Override
  public int[] getIntMV(int docId, ReaderContext context) {
    return ArraySerDeUtils.deserializeIntArrayWithoutLength(context.getValue(docId));
  }

  @Override
  public int getLongMV(int docId, long[] valueBuffer, ReaderContext context) {
    return ArraySerDeUtils.deserializeLongArrayWithoutLength(context.getValue(docId), valueBuffer);
  }

  @Override
  public long[] getLongMV(int docId, ReaderContext context) {
    return ArraySerDeUtils.deserializeLongArrayWithoutLength(context.getValue(docId));
  }

  @Override
  public int getFloatMV(int docId, float[] valueBuffer, ReaderContext context) {
    return ArraySerDeUtils.deserializeFloatArrayWithoutLength(context.getValue(docId), valueBuffer);
  }

  @Override
  public float[] getFloatMV(int docId, ReaderContext context) {
    return ArraySerDeUtils.deserializeFloatArrayWithoutLength(context.getValue(docId));
  }

  @Override
  public int getDoubleMV(int docId, double[] valueBuffer, ReaderContext context) {
    return ArraySerDeUtils.deserializeDoubleArrayWithoutLength(context.getValue(docId), valueBuffer);
  }

  @Override
  public double[] getDoubleMV(int docId, ReaderContext context) {
    return ArraySerDeUtils.deserializeDoubleArrayWithoutLength(context.getValue(docId));
  }

  @Override
  public int getNumValuesMV(int docId, ReaderContext context) {
    byte[] bytes = context.getValue(docId);
    if (getStoredType().isFixedWidth()) {
      return bytes.length / getStoredType().size();
    } else {
      return ByteBuffer.wrap(bytes).getInt();
    }
  }

  /**
   * Base class for V6 reader contexts that handles the two-stream chunk format.
   * After decompressing both streams, entries are accessed via cumulative size offsets.
   */
  private abstract static class V6ReaderContextBase extends ReaderContext {
    // Decompressed size stream: [numDocs][size0][size1]...
    protected int[] _sizes;
    // Cumulative offsets into the data stream for each entry
    protected int[] _dataOffsets;

    V6ReaderContextBase(PinotDataBuffer metadata, PinotDataBuffer chunks, long chunkStartOffset) {
      super(metadata, chunks, chunkStartOffset);
    }

    /**
     * Parse sizes from the decompressed size buffer and compute cumulative data offsets.
     */
    protected void parseSizes(ByteBuffer sizeBuffer) {
      _numDocsInCurrentChunk = sizeBuffer.getInt(0);
      _sizes = new int[_numDocsInCurrentChunk];
      _dataOffsets = new int[_numDocsInCurrentChunk + 1];
      _dataOffsets[0] = 0;
      for (int i = 0; i < _numDocsInCurrentChunk; i++) {
        _sizes[i] = sizeBuffer.getInt((i + 1) * Integer.BYTES);
        _dataOffsets[i + 1] = _dataOffsets[i] + _sizes[i];
      }
    }

    protected byte[] readValueFromDataBuffer(ByteBuffer dataBuffer, int docId) {
      int index = docId - _docIdOffset;
      int offset = _dataOffsets[index];
      int length = _sizes[index];
      byte[] value = new byte[length];
      dataBuffer.position(offset);
      dataBuffer.get(value);
      return value;
    }
  }

  /**
   * Uncompressed (PASS_THROUGH) reader context for V6 format.
   */
  private static final class V6UncompressedReaderContext extends V6ReaderContextBase {
    private ByteBuffer _sizeChunk;
    private ByteBuffer _dataChunk;

    V6UncompressedReaderContext(PinotDataBuffer metadata, PinotDataBuffer chunks, long chunkStartOffset) {
      super(metadata, chunks, chunkStartOffset);
    }

    @Override
    protected byte[] processChunkAndReadFirstValue(int docId, long offset, long limit) {
      // For PASS_THROUGH, the "compressed" data is just the raw bytes
      // Layout: [sizeStreamLen (4 bytes)][size stream bytes][data stream bytes]
      ByteBuffer chunkView = _chunks.toDirectByteBuffer(offset, (int) (limit - offset));

      if (!_regularChunk) {
        // Huge chunk: no two-stream layout, just raw bytes
        byte[] value = new byte[chunkView.remaining()];
        chunkView.get(value);
        return value;
      }

      int sizeStreamLen = chunkView.getInt(0);
      // Size stream starts at offset 4
      chunkView.position(Integer.BYTES);
      chunkView.limit(Integer.BYTES + sizeStreamLen);
      _sizeChunk = chunkView.slice().order(ByteOrder.LITTLE_ENDIAN);

      // Data stream starts after size stream
      chunkView.position(Integer.BYTES + sizeStreamLen);
      chunkView.limit((int) (limit - offset));
      _dataChunk = chunkView.slice().order(ByteOrder.LITTLE_ENDIAN);

      parseSizes(_sizeChunk);
      return readSmallUncompressedValue(docId);
    }

    @Override
    protected byte[] readSmallUncompressedValue(int docId) {
      return readValueFromDataBuffer(_dataChunk, docId);
    }

    @Override
    public void close() {
    }
  }

  /**
   * Compressed reader context for V6 format. Decompresses both size and data streams.
   * The size decompression buffer is allocated dynamically per chunk based on the actual
   * decompressed length from the compressed stream, since the size stream can be much larger
   * than the data stream (e.g., many small values each need a 4-byte size entry).
   */
  private static final class V6CompressedReaderContext extends V6ReaderContextBase {
    private ByteBuffer _sizeDecompressedBuffer;
    private final ByteBuffer _dataDecompressedBuffer;
    private final ChunkDecompressor _chunkDecompressor;
    private final ChunkCompressionType _chunkCompressionType;
    private boolean _closed;

    V6CompressedReaderContext(PinotDataBuffer metadata, PinotDataBuffer chunks, long chunkStartOffset,
        ChunkDecompressor chunkDecompressor, ChunkCompressionType chunkCompressionType,
        int targetChunkSize) {
      super(metadata, chunks, chunkStartOffset);
      _chunkDecompressor = chunkDecompressor;
      _chunkCompressionType = chunkCompressionType;
      _dataDecompressedBuffer = ByteBuffer.allocateDirect(targetChunkSize).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    protected byte[] processChunkAndReadFirstValue(int docId, long offset, long limit)
        throws IOException {
      ByteBuffer compressed = _chunks.toDirectByteBuffer(offset, (int) (limit - offset));

      if (!_regularChunk) {
        // Huge chunk: single compressed value, no two-stream layout
        return readHugeCompressedValue(compressed, _chunkDecompressor.decompressedLength(compressed));
      }

      // Read sizeStreamCompressedLen
      int sizeStreamCompressedLen = compressed.getInt(0);

      // Slice out the compressed size stream
      ByteBuffer sizeCompressed = compressed.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      sizeCompressed.position(Integer.BYTES);
      sizeCompressed.limit(Integer.BYTES + sizeStreamCompressedLen);

      // Determine actual decompressed size and (re)allocate buffer if needed
      int sizeDecompressedLen = _chunkDecompressor.decompressedLength(sizeCompressed);
      if (_sizeDecompressedBuffer == null || _sizeDecompressedBuffer.capacity() < sizeDecompressedLen) {
        CleanerUtil.cleanQuietly(_sizeDecompressedBuffer);
        _sizeDecompressedBuffer = ByteBuffer.allocateDirect(sizeDecompressedLen).order(ByteOrder.LITTLE_ENDIAN);
      } else {
        _sizeDecompressedBuffer.clear();
      }

      // Decompress size stream
      _chunkDecompressor.decompress(sizeCompressed, _sizeDecompressedBuffer);

      // Decompress data stream
      _dataDecompressedBuffer.clear();
      int dataCompressedStart = Integer.BYTES + sizeStreamCompressedLen;
      if (dataCompressedStart < compressed.limit()) {
        ByteBuffer dataCompressed = compressed.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        dataCompressed.position(dataCompressedStart);
        _chunkDecompressor.decompress(dataCompressed, _dataDecompressedBuffer);
      }

      parseSizes(_sizeDecompressedBuffer);
      return readSmallUncompressedValue(docId);
    }

    @Override
    protected byte[] readSmallUncompressedValue(int docId) {
      return readValueFromDataBuffer(_dataDecompressedBuffer, docId);
    }

    private byte[] readHugeCompressedValue(ByteBuffer compressed, int decompressedLength)
        throws IOException {
      byte[] value = new byte[decompressedLength];
      if (_chunkCompressionType == ChunkCompressionType.SNAPPY
          || _chunkCompressionType == ChunkCompressionType.ZSTANDARD) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(decompressedLength).order(ByteOrder.LITTLE_ENDIAN);
        try {
          _chunkDecompressor.decompress(compressed, buffer);
          buffer.get(value);
        } finally {
          if (CleanerUtil.UNMAP_SUPPORTED) {
            CleanerUtil.getCleaner().freeBuffer(buffer);
          }
        }
      } else {
        _chunkDecompressor.decompress(compressed, ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN));
      }
      return value;
    }

    @Override
    public void close() {
      if (_closed) {
        return;
      }
      _closed = true;
      CleanerUtil.cleanQuietly(_sizeDecompressedBuffer);
      CleanerUtil.cleanQuietly(_dataDecompressedBuffer);
    }
  }
}
