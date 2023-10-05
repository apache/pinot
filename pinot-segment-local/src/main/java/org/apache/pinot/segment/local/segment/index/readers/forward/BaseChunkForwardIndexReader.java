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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.io.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation for chunk-based raw (non-dictionary-encoded) forward index reader.
 */
public abstract class BaseChunkForwardIndexReader implements ForwardIndexReader<ChunkReaderContext> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseChunkForwardIndexReader.class);

  protected final PinotDataBuffer _dataBuffer;
  protected final DataType _storedType;
  protected final int _numChunks;
  protected final int _numDocsPerChunk;
  protected final int _lengthOfLongestEntry;
  protected final boolean _isCompressed;
  protected final ChunkCompressionType _compressionType;
  protected final ChunkDecompressor _chunkDecompressor;
  protected final PinotDataBuffer _dataHeader;
  protected final int _headerEntryChunkOffsetSize;
  protected final PinotDataBuffer _rawData;
  protected final boolean _isSingleValue;
  protected final int _dataHeaderStart;
  protected final int _rawDataStart;

  protected BaseChunkForwardIndexReader(PinotDataBuffer dataBuffer, DataType storedType, boolean isSingleValue) {
    _dataBuffer = dataBuffer;
    _storedType = storedType;

    int headerOffset = 0;
    int version = _dataBuffer.getInt(headerOffset);
    headerOffset += Integer.BYTES;

    _numChunks = _dataBuffer.getInt(headerOffset);
    headerOffset += Integer.BYTES;

    _numDocsPerChunk = _dataBuffer.getInt(headerOffset);
    headerOffset += Integer.BYTES;

    _lengthOfLongestEntry = _dataBuffer.getInt(headerOffset);
    if (storedType.isFixedWidth() && isSingleValue) {
      Preconditions.checkState(_lengthOfLongestEntry == storedType.size());
    }
    headerOffset += Integer.BYTES;

    int dataHeaderStart = headerOffset;
    if (version > 1) {
      _dataBuffer.getInt(headerOffset); // Total docs
      headerOffset += Integer.BYTES;

      _compressionType = ChunkCompressionType.valueOf(_dataBuffer.getInt(headerOffset));
      _chunkDecompressor = ChunkCompressorFactory.getDecompressor(_compressionType);
      _isCompressed = !_compressionType.equals(ChunkCompressionType.PASS_THROUGH);

      headerOffset += Integer.BYTES;
      dataHeaderStart = _dataBuffer.getInt(headerOffset);
    } else {
      _isCompressed = true;
      _compressionType = ChunkCompressionType.SNAPPY;
      _chunkDecompressor = ChunkCompressorFactory.getDecompressor(_compressionType);
    }

    _headerEntryChunkOffsetSize = version <= 2 ? Integer.BYTES : Long.BYTES;

    // Slice out the header from the data buffer.
    int dataHeaderLength = _numChunks * _headerEntryChunkOffsetSize;
    int rawDataStart = dataHeaderStart + dataHeaderLength;
    _dataHeaderStart = dataHeaderStart;
    _dataHeader = _dataBuffer.view(dataHeaderStart, rawDataStart);

    // Useful for uncompressed data.
    _rawDataStart = rawDataStart;
    _rawData = _dataBuffer.view(rawDataStart, _dataBuffer.size());

    _isSingleValue = isSingleValue;
  }

  /**
   * Helper method to return the chunk buffer that contains the value at the given document id.
   * <ul>
   *   <li> If the chunk already exists in the reader context, returns the same. </li>
   *   <li> Otherwise, loads the chunk for the row, and sets it in the reader context. </li>
   * </ul>
   * @param docId Document id
   * @param context Reader context
   * @return Chunk for the row
   */
  protected ByteBuffer getChunkBuffer(int docId, ChunkReaderContext context) {
    int chunkId = docId / _numDocsPerChunk;
    if (context.getChunkId() == chunkId) {
      return context.getChunkBuffer();
    }
    return decompressChunk(chunkId, context);
  }

  protected void recordDocIdRanges(int docId, ChunkReaderContext context, List<ValueRange> ranges) {
    int chunkId = docId / _numDocsPerChunk;
    if (context.getChunkId() == chunkId) {
      ranges.addAll(context.getRanges());
      return;
    }
    recordChunkRanges(chunkId, context, ranges);
  }

  protected void recordChunkRanges(int chunkId, ChunkReaderContext context, List<ValueRange> ranges) {
    List<ValueRange> chunkRanges = new ArrayList<>();
    int chunkSize;
    long chunkPosition = getChunkPositionAndRecordRanges(chunkId, chunkRanges);

    // Size of chunk can be determined using next chunks offset, or end of data buffer for last chunk.
    if (chunkId == (_numChunks - 1)) { // Last chunk.
      chunkSize = (int) (_dataBuffer.size() - chunkPosition);
    } else {
      long nextChunkOffset = getChunkPositionAndRecordRanges(chunkId + 1, chunkRanges);
      chunkSize = (int) (nextChunkOffset - chunkPosition);
    }
    chunkRanges.add(ValueRange.newByteRange(chunkPosition, chunkSize));
    context.setChunkId(chunkId);
    context.setRanges(chunkRanges);
    ranges.addAll(chunkRanges);
  }

  protected ByteBuffer decompressChunk(int chunkId, ChunkReaderContext context) {
    int chunkSize;
    long chunkPosition = getChunkPosition(chunkId);

    // Size of chunk can be determined using next chunks offset, or end of data buffer for last chunk.
    if (chunkId == (_numChunks - 1)) { // Last chunk.
      chunkSize = (int) (_dataBuffer.size() - chunkPosition);
    } else {
      long nextChunkOffset = getChunkPosition(chunkId + 1);
      chunkSize = (int) (nextChunkOffset - chunkPosition);
    }

    ByteBuffer decompressedBuffer = context.getChunkBuffer();
    decompressedBuffer.clear();

    try {
      _chunkDecompressor.decompress(_dataBuffer.toDirectByteBuffer(chunkPosition, chunkSize), decompressedBuffer);
    } catch (IOException e) {
      LOGGER.error("Exception caught while decompressing data chunk", e);
      throw new RuntimeException(e);
    }
    context.setChunkId(chunkId);
    return decompressedBuffer;
  }

  /**
   * Helper method to get the offset of the chunk in the data.
   * @param chunkId Id of the chunk for which to return the position.
   * @return Position (offset) of the chunk in the data.
   */
  protected long getChunkPosition(int chunkId) {
    if (_headerEntryChunkOffsetSize == Integer.BYTES) {
      return _dataHeader.getInt(chunkId * _headerEntryChunkOffsetSize);
    } else {
      return _dataHeader.getLong(chunkId * _headerEntryChunkOffsetSize);
    }
  }

  protected long getChunkPositionAndRecordRanges(int chunkId, List<ValueRange> ranges) {
    if (_headerEntryChunkOffsetSize == Integer.BYTES) {
      ranges.add(
          ValueRange.newByteRange(_dataHeaderStart + chunkId * _headerEntryChunkOffsetSize, Integer.BYTES));
      return _dataHeader.getInt(chunkId * _headerEntryChunkOffsetSize);
    } else {
      ranges.add(
          ValueRange.newByteRange(_dataHeaderStart + chunkId * _headerEntryChunkOffsetSize, Long.BYTES));
      return _dataHeader.getLong(chunkId * _headerEntryChunkOffsetSize);
    }
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return _isSingleValue;
  }

  @Override
  public DataType getStoredType() {
    return _storedType;
  }

  @Override
  public ChunkCompressionType getCompressionType() {
    return _compressionType;
  }

  @Override
  public int getLengthOfLongestEntry() {
    return _lengthOfLongestEntry;
  }

  @Override
  public void readValuesSV(int[] docIds, int length, int[] values, ChunkReaderContext context) {
    if (_storedType.isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (_storedType) {
        case INT: {
          int minOffset = docIds[0] * Integer.BYTES;
          IntBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Integer.BYTES).asIntBuffer();
          buffer.get(values, 0, length);
        }
        break;
        case LONG: {
          int minOffset = docIds[0] * Long.BYTES;
          LongBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Long.BYTES).asLongBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (int) buffer.get(i);
          }
        }
        break;
        case FLOAT: {
          int minOffset = docIds[0] * Float.BYTES;
          FloatBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Float.BYTES).asFloatBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (int) buffer.get(i);
          }
        }
        break;
        case DOUBLE: {
          int minOffset = docIds[0] * Double.BYTES;
          DoubleBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Double.BYTES).asDoubleBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (int) buffer.get(i);
          }
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      ForwardIndexReader.super.readValuesSV(docIds, length, values, context);
    }
  }

  @Override
  public void readValuesSV(int[] docIds, int length, long[] values, ChunkReaderContext context) {
    if (_storedType.isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (_storedType) {
        case INT: {
          int minOffset = docIds[0] * Integer.BYTES;
          IntBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Integer.BYTES).asIntBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case LONG: {
          int minOffset = docIds[0] * Long.BYTES;
          LongBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Long.BYTES).asLongBuffer();
          buffer.get(values, 0, length);
        }
        break;
        case FLOAT: {
          int minOffset = docIds[0] * Float.BYTES;
          FloatBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Float.BYTES).asFloatBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (long) buffer.get(i);
          }
        }
        break;
        case DOUBLE: {
          int minOffset = docIds[0] * Double.BYTES;
          DoubleBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Double.BYTES).asDoubleBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (long) buffer.get(i);
          }
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      ForwardIndexReader.super.readValuesSV(docIds, length, values, context);
    }
  }

  @Override
  public void readValuesSV(int[] docIds, int length, float[] values, ChunkReaderContext context) {
    if (_storedType.isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (_storedType) {
        case INT: {
          int minOffset = docIds[0] * Integer.BYTES;
          IntBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Integer.BYTES).asIntBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case LONG: {
          int minOffset = docIds[0] * Long.BYTES;
          LongBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Long.BYTES).asLongBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case FLOAT: {
          int minOffset = docIds[0] * Float.BYTES;
          FloatBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Float.BYTES).asFloatBuffer();
          buffer.get(values, 0, length);
        }
        break;
        case DOUBLE: {
          int minOffset = docIds[0] * Double.BYTES;
          DoubleBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Double.BYTES).asDoubleBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = (float) buffer.get(i);
          }
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      ForwardIndexReader.super.readValuesSV(docIds, length, values, context);
    }
  }

  @Override
  public void readValuesSV(int[] docIds, int length, double[] values, ChunkReaderContext context) {
    if (_storedType.isFixedWidth() && !_isCompressed && isContiguousRange(docIds, length)) {
      switch (_storedType) {
        case INT: {
          int minOffset = docIds[0] * Integer.BYTES;
          IntBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Integer.BYTES).asIntBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case LONG: {
          int minOffset = docIds[0] * Long.BYTES;
          getLong(0, context);
          LongBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Long.BYTES).asLongBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case FLOAT: {
          int minOffset = docIds[0] * Float.BYTES;
          FloatBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Float.BYTES).asFloatBuffer();
          for (int i = 0; i < buffer.limit(); i++) {
            values[i] = buffer.get(i);
          }
        }
        break;
        case DOUBLE: {
          int minOffset = docIds[0] * Double.BYTES;
          DoubleBuffer buffer = _rawData.toDirectByteBuffer(minOffset, length * Double.BYTES).asDoubleBuffer();
          buffer.get(values, 0, length);
        }
        break;
        default:
          throw new IllegalArgumentException();
      }
    } else {
      ForwardIndexReader.super.readValuesSV(docIds, length, values, context);
    }
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }

  private boolean isContiguousRange(int[] docIds, int length) {
    return docIds[length - 1] - docIds[0] == length - 1;
  }
}
