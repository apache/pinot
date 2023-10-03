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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriter;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Chunk-based multi-value raw (non-dictionary-encoded) forward index reader for values of fixed length data type (INT,
 * LONG, FLOAT, DOUBLE).
 * <p>For data layout, please refer to the documentation for {@link VarByteChunkForwardIndexWriter}
 */
public final class FixedByteChunkMVForwardIndexReader extends BaseChunkForwardIndexReader
    implements ForwardIndexReader.ValueRangeProvider<ChunkReaderContext> {
  private static final int ROW_OFFSET_SIZE = VarByteChunkForwardIndexWriter.CHUNK_HEADER_ENTRY_ROW_OFFSET_SIZE;

  private final int _maxChunkSize;

  public FixedByteChunkMVForwardIndexReader(PinotDataBuffer dataBuffer, DataType storedType) {
    super(dataBuffer, storedType, false);
    _maxChunkSize = _numDocsPerChunk * (ROW_OFFSET_SIZE + _lengthOfLongestEntry);
  }

  @Nullable
  @Override
  public ChunkReaderContext createContext() {
    if (_isCompressed) {
      return new ChunkReaderContext(_maxChunkSize);
    } else {
      return null;
    }
  }

  @Override
  public int getIntMV(int docId, int[] valueBuffer, ChunkReaderContext context) {
    ByteBuffer byteBuffer = slice(docId, context);
    int numValues = byteBuffer.getInt();
    for (int i = 0; i < numValues; i++) {
      valueBuffer[i] = byteBuffer.getInt();
    }
    return numValues;
  }

  @Override
  public int[] getIntMV(int docId, ChunkReaderContext context) {
    ByteBuffer byteBuffer = slice(docId, context);
    int numValues = byteBuffer.getInt();
    int[] valueBuffer = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      valueBuffer[i] = byteBuffer.getInt();
    }
    return valueBuffer;
  }

  @Override
  public int getLongMV(int docId, long[] valueBuffer, ChunkReaderContext context) {
    ByteBuffer byteBuffer = slice(docId, context);
    int numValues = byteBuffer.getInt();
    for (int i = 0; i < numValues; i++) {
      valueBuffer[i] = byteBuffer.getLong();
    }
    return numValues;
  }

  @Override
  public long[] getLongMV(int docId, ChunkReaderContext context) {
    ByteBuffer byteBuffer = slice(docId, context);
    int numValues = byteBuffer.getInt();
    long[] valueBuffer = new long[numValues];
    for (int i = 0; i < numValues; i++) {
      valueBuffer[i] = byteBuffer.getLong();
    }
    return valueBuffer;
  }

  @Override
  public int getFloatMV(int docId, float[] valueBuffer, ChunkReaderContext context) {
    ByteBuffer byteBuffer = slice(docId, context);
    int numValues = byteBuffer.getInt();
    for (int i = 0; i < numValues; i++) {
      valueBuffer[i] = byteBuffer.getFloat();
    }
    return numValues;
  }

  @Override
  public float[] getFloatMV(int docId, ChunkReaderContext context) {
    ByteBuffer byteBuffer = slice(docId, context);
    int numValues = byteBuffer.getInt();
    float[] valueBuffer = new float[numValues];
    for (int i = 0; i < numValues; i++) {
      valueBuffer[i] = byteBuffer.getFloat();
    }
    return valueBuffer;
  }

  @Override
  public int getDoubleMV(int docId, double[] valueBuffer, ChunkReaderContext context) {
    ByteBuffer byteBuffer = slice(docId, context);
    int numValues = byteBuffer.getInt();
    for (int i = 0; i < numValues; i++) {
      valueBuffer[i] = byteBuffer.getDouble();
    }
    return numValues;
  }

  @Override
  public double[] getDoubleMV(int docId, ChunkReaderContext context) {
    ByteBuffer byteBuffer = slice(docId, context);
    int numValues = byteBuffer.getInt();
    double[] valueBuffer = new double[numValues];
    for (int i = 0; i < numValues; i++) {
      valueBuffer[i] = byteBuffer.getDouble();
    }
    return valueBuffer;
  }

  @Override
  public int getNumValuesMV(int docId, ChunkReaderContext context) {
    ByteBuffer byteBuffer = slice(docId, context);
    return byteBuffer.getInt();
  }

  private ByteBuffer slice(int docId, ChunkReaderContext context) {
    if (_isCompressed) {
      return sliceBytesCompressed(docId, context);
    } else {
      return sliceBytesUncompressed(docId);
    }
  }

  /**
   * Helper method to read BYTES value from the compressed index.
   */
  private void sliceBytesCompressedAndRecordRanges(int docId, ChunkReaderContext context,
      List<ValueRange> ranges) {
    recordDocIdRanges(docId, context, ranges);
  }

  private ByteBuffer sliceBytesCompressed(int docId, ChunkReaderContext context) {
    int chunkRowId = docId % _numDocsPerChunk;
    ByteBuffer chunkBuffer = getChunkBuffer(docId, context);

    // These offsets are offset in the chunk buffer
    int valueStartOffset = chunkBuffer.getInt(chunkRowId * ROW_OFFSET_SIZE);
    int valueEndOffset = getValueEndOffset(chunkRowId, chunkBuffer);
    // cast only for JDK8 compilation profile
    return (ByteBuffer) chunkBuffer.duplicate().position(valueStartOffset).limit(valueEndOffset);
  }

  private void sliceBytesUncompressedAndRecordRanges(int docId, List<ValueRange> ranges) {
    int chunkId = docId / _numDocsPerChunk;
    int chunkRowId = docId % _numDocsPerChunk;

    // These offsets are offset in the data buffer
    long chunkStartOffset = getChunkPositionAndRecordRanges(chunkId, ranges);

    ranges.add(
        ValueRange.newByteRange(chunkStartOffset + (long) chunkRowId * ROW_OFFSET_SIZE, Integer.BYTES));
    long valueStartOffset =
        chunkStartOffset + _dataBuffer.getInt(chunkStartOffset + (long) chunkRowId * ROW_OFFSET_SIZE);
    long valueEndOffset = getValueEndOffsetAndRecordRanges(chunkId, chunkRowId, chunkStartOffset, ranges);

    ranges.add(ValueRange.newByteRange(valueStartOffset, (int) (valueEndOffset - valueStartOffset)));
  }

  /**
   * Helper method to read BYTES value from the uncompressed index.
   */
  private ByteBuffer sliceBytesUncompressed(int docId) {
    int chunkId = docId / _numDocsPerChunk;
    int chunkRowId = docId % _numDocsPerChunk;

    // These offsets are offset in the data buffer
    long chunkStartOffset = getChunkPosition(chunkId);
    long valueStartOffset =
        chunkStartOffset + _dataBuffer.getInt(chunkStartOffset + (long) chunkRowId * ROW_OFFSET_SIZE);
    long valueEndOffset = getValueEndOffset(chunkId, chunkRowId, chunkStartOffset);
    return _dataBuffer.toDirectByteBuffer(valueStartOffset, (int) (valueEndOffset - valueStartOffset));
  }

  /**
   * Helper method to compute the end offset of the value in the chunk buffer.
   */
  private int getValueEndOffset(int rowId, ByteBuffer chunkBuffer) {
    if (rowId == _numDocsPerChunk - 1) {
      // Last row in the chunk
      return chunkBuffer.limit();
    } else {
      int valueEndOffset = chunkBuffer.getInt((rowId + 1) * ROW_OFFSET_SIZE);
      if (valueEndOffset == 0) {
        // Last row in the last chunk (chunk is incomplete, which stores 0 as the offset for the absent rows)
        return chunkBuffer.limit();
      } else {
        return valueEndOffset;
      }
    }
  }

  private long getValueEndOffsetAndRecordRanges(int chunkId, int chunkRowId, long chunkStartOffset,
      List<ValueRange> ranges) {
    if (chunkId == _numChunks - 1) {
      // Last chunk
      if (chunkRowId == _numDocsPerChunk - 1) {
        // Last row in the last chunk
        return _dataBuffer.size();
      } else {
        ranges.add(ValueRange.newByteRange(chunkStartOffset + (long) (chunkRowId + 1) * ROW_OFFSET_SIZE,
            Integer.BYTES));
        int valueEndOffsetInChunk = _dataBuffer
            .getInt(chunkStartOffset + (long) (chunkRowId + 1) * ROW_OFFSET_SIZE);
        if (valueEndOffsetInChunk == 0) {
          // Last row in the last chunk (chunk is incomplete, which stores 0 as the offset for the absent rows)
          return _dataBuffer.size();
        } else {
          return chunkStartOffset + valueEndOffsetInChunk;
        }
      }
    } else {
      if (chunkRowId == _numDocsPerChunk - 1) {
        // Last row in the chunk
        return getChunkPositionAndRecordRanges(chunkId + 1, ranges);
      } else {
        ranges.add(ValueRange.newByteRange(chunkStartOffset + (long) (chunkRowId + 1) * ROW_OFFSET_SIZE,
            Integer.BYTES));
        return chunkStartOffset + _dataBuffer
            .getInt(chunkStartOffset + (long) (chunkRowId + 1) * ROW_OFFSET_SIZE);
      }
    }
  }

  /**
   * Helper method to compute the end offset of the value in the data buffer.
   */
  private long getValueEndOffset(int chunkId, int chunkRowId, long chunkStartOffset) {
    if (chunkId == _numChunks - 1) {
      // Last chunk
      if (chunkRowId == _numDocsPerChunk - 1) {
        // Last row in the last chunk
        return _dataBuffer.size();
      } else {
        int valueEndOffsetInChunk = _dataBuffer.getInt(chunkStartOffset + (long) (chunkRowId + 1) * ROW_OFFSET_SIZE);
        if (valueEndOffsetInChunk == 0) {
          // Last row in the last chunk (chunk is incomplete, which stores 0 as the offset for the absent rows)
          return _dataBuffer.size();
        } else {
          return chunkStartOffset + valueEndOffsetInChunk;
        }
      }
    } else {
      if (chunkRowId == _numDocsPerChunk - 1) {
        // Last row in the chunk
        return getChunkPosition(chunkId + 1);
      } else {
        return chunkStartOffset + _dataBuffer.getInt(chunkStartOffset + (long) (chunkRowId + 1) * ROW_OFFSET_SIZE);
      }
    }
  }

  @Override
  public List<ValueRange> getDocIdRange(int docId, ChunkReaderContext context, @Nullable List<ValueRange> ranges) {
    if (ranges == null) {
      ranges = new ArrayList<>();
    }
    if (_isCompressed) {
      sliceBytesCompressedAndRecordRanges(docId, context, ranges);
    } else {
      sliceBytesUncompressedAndRecordRanges(docId, ranges);
    }
    return ranges;
  }

  @Override
  public boolean isFixedLengthType() {
    return false;
  }

  @Override
  public long getBaseOffset() {
    throw new IllegalStateException("Operation not supported since the forward index is not fixed length type");
  }

  @Override
  public int getDocLength() {
    throw new IllegalStateException("Operation not supported since the forward index is not fixed length type");
  }
}
