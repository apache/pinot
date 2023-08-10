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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexByteRange;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Bit-compressed dictionary-encoded forward index reader for multi-value columns. The values returned are dictionary
 * ids.
 * <p>Storage Layout:
 * <ul>
 *   <li>
 *     There will be three sections: CHUNK OFFSET, BITMAP and RAW DATA.
 *   </li>
 *   <li>
 *     CHUNK OFFSET contains the start offset of each chunk.
 *   </li>
 *   <li>
 *     BITMAP contains sequence of bits. The number of bits equals to the total number of values. The number of set bits
 *     equals to the number of rows. A bit is set if it is the start of a new row.
 *   </li>
 *   <li>
 *     RAW DATA contains the bit compressed values. We divide RAW DATA into chunks, where each chunk has the same number
 *     of rows.
 *   </li>
 * </ul>
 */
public final class FixedBitMVForwardIndexReader implements ForwardIndexReader<FixedBitMVForwardIndexReader.Context>,
                                                           ForwardIndexReader.DocIdRangeProvider
                                                               <FixedBitMVForwardIndexReader.Context> {
  private static final int PREFERRED_NUM_VALUES_PER_CHUNK = 2048;

  private final FixedByteValueReaderWriter _chunkOffsetReader;
  private final PinotDataBitSet _bitmapReader;
  private final FixedBitIntReaderWriter _rawDataReader;
  private final int _numDocs;
  private final int _numValues;
  private final int _numDocsPerChunk;

  private final long _bitmapReaderStartOffset;
  private final long _rawDataReaderStartOffset;

  public FixedBitMVForwardIndexReader(PinotDataBuffer dataBuffer, int numDocs, int numValues, int numBitsPerValue) {
    _numDocs = numDocs;
    _numValues = numValues;
    _numDocsPerChunk = (int) (Math.ceil((float) PREFERRED_NUM_VALUES_PER_CHUNK / (numValues / numDocs)));
    int numChunks = (numDocs + _numDocsPerChunk - 1) / _numDocsPerChunk;
    long endOffset = numChunks * Integer.BYTES;
    _bitmapReaderStartOffset = endOffset;
    _chunkOffsetReader = new FixedByteValueReaderWriter(dataBuffer.view(0L, endOffset));
    int bitmapSize = (numValues + Byte.SIZE - 1) / Byte.SIZE;
    _bitmapReader = new PinotDataBitSet(dataBuffer.view(endOffset, endOffset + bitmapSize));
    endOffset += bitmapSize;
    _rawDataReaderStartOffset = endOffset;
    int rawDataSize = (int) (((long) numValues * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE);
    _rawDataReader =
        new FixedBitIntReaderWriter(dataBuffer.view(endOffset, endOffset + rawDataSize), numValues, numBitsPerValue);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return false;
  }

  @Override
  public DataType getStoredType() {
    return DataType.INT;
  }

  @Override
  public Context createContext() {
    return new Context();
  }

  @Override
  public int getDictIdMV(int docId, int[] dictIdBuffer, Context context) {
    int contextDocId = context._docId;
    int contextEndOffset = context._endOffset;
    int startIndex;
    if (docId == contextDocId + 1) {
      startIndex = contextEndOffset;
    } else {
      int chunkId = docId / _numDocsPerChunk;
      if (docId > contextDocId && chunkId == contextDocId / _numDocsPerChunk) {
        // Same chunk
        startIndex = _bitmapReader.getNextNthSetBitOffset(contextEndOffset + 1, docId - contextDocId - 1);
      } else {
        // Different chunk
        int chunkOffset = _chunkOffsetReader.getInt(chunkId);
        int indexInChunk = docId % _numDocsPerChunk;
        if (indexInChunk == 0) {
          startIndex = chunkOffset;
        } else {
          startIndex = _bitmapReader.getNextNthSetBitOffset(chunkOffset + 1, indexInChunk);
        }
      }
    }
    int endIndex;
    if (docId == _numDocs - 1) {
      endIndex = _numValues;
    } else {
      endIndex = _bitmapReader.getNextSetBitOffset(startIndex + 1);
    }
    int numValues = endIndex - startIndex;
    _rawDataReader.readInt(startIndex, numValues, dictIdBuffer);

    // Update context
    context._docId = docId;
    context._endOffset = endIndex;

    return numValues;
  }

  @Override
  public int[] getDictIdMV(int docId, Context context) {
    int contextDocId = context._docId;
    int contextEndOffset = context._endOffset;
    int startIndex;
    if (docId == contextDocId + 1) {
      startIndex = contextEndOffset;
    } else {
      int chunkId = docId / _numDocsPerChunk;
      if (docId > contextDocId && chunkId == contextDocId / _numDocsPerChunk) {
        // Same chunk
        startIndex = _bitmapReader.getNextNthSetBitOffset(contextEndOffset + 1, docId - contextDocId - 1);
      } else {
        // Different chunk
        int chunkOffset = _chunkOffsetReader.getInt(chunkId);
        int indexInChunk = docId % _numDocsPerChunk;
        if (indexInChunk == 0) {
          startIndex = chunkOffset;
        } else {
          startIndex = _bitmapReader.getNextNthSetBitOffset(chunkOffset + 1, indexInChunk);
        }
      }
    }
    int endIndex;
    if (docId == _numDocs - 1) {
      endIndex = _numValues;
    } else {
      endIndex = _bitmapReader.getNextSetBitOffset(startIndex + 1);
    }
    int numValues = endIndex - startIndex;
    int[] dictIdBuffer = new int[numValues];
    _rawDataReader.readInt(startIndex, numValues, dictIdBuffer);

    // Update context
    context._docId = docId;
    context._endOffset = endIndex;

    return dictIdBuffer;
  }

  @Override
  public int getNumValuesMV(int docId, Context context) {
    int contextDocId = context._docId;
    int contextEndOffset = context._endOffset;
    int startIndex;
    if (docId == contextDocId + 1) {
      startIndex = contextEndOffset;
    } else {
      int chunkId = docId / _numDocsPerChunk;
      if (docId > contextDocId && chunkId == contextDocId / _numDocsPerChunk) {
        // Same chunk
        startIndex = _bitmapReader.getNextNthSetBitOffset(contextEndOffset + 1, docId - contextDocId - 1);
      } else {
        // Different chunk
        int chunkOffset = _chunkOffsetReader.getInt(chunkId);
        int indexInChunk = docId % _numDocsPerChunk;
        if (indexInChunk == 0) {
          startIndex = chunkOffset;
        } else {
          startIndex = _bitmapReader.getNextNthSetBitOffset(chunkOffset + 1, indexInChunk);
        }
      }
    }
    int endIndex;
    if (docId == _numDocs - 1) {
      endIndex = _numValues;
    } else {
      endIndex = _bitmapReader.getNextSetBitOffset(startIndex + 1);
    }
    return endIndex - startIndex;
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
    _chunkOffsetReader.close();
    _bitmapReader.close();
    _rawDataReader.close();
  }

  @Override
  public List<ForwardIndexByteRange> getDocIdRange(int docId, Context context) {
    List<ForwardIndexByteRange> ranges = new ArrayList<>();
    int contextDocId = context._docId;
    int contextEndOffset = context._endOffset;
    int startIndex;
    if (docId == contextDocId + 1) {
      startIndex = contextEndOffset;
    } else {
      int chunkId = docId / _numDocsPerChunk;
      if (docId > contextDocId && chunkId == contextDocId / _numDocsPerChunk) {
        // Same chunk
        startIndex =
            _bitmapReader.getNextNthSetBitOffsetOffsetAndRecordRanges(contextEndOffset + 1, docId - contextDocId - 1,
                _bitmapReaderStartOffset, ranges);
      } else {
        // Different chunk
        ranges.add(ForwardIndexByteRange.newByteRange(chunkId, Integer.BYTES));
        int chunkOffset = _chunkOffsetReader.getInt(chunkId);
        int indexInChunk = docId % _numDocsPerChunk;
        if (indexInChunk == 0) {
          startIndex = chunkOffset;
        } else {
          startIndex = _bitmapReader.getNextNthSetBitOffsetOffsetAndRecordRanges(chunkOffset + 1, indexInChunk,
              _bitmapReaderStartOffset, ranges);
        }
      }
    }
    int endIndex;
    if (docId == _numDocs - 1) {
      endIndex = _numValues;
    } else {
      endIndex = _bitmapReader.getNextSetBitOffsetRanges(startIndex + 1, _bitmapReaderStartOffset, ranges);
    }
    int numValues = endIndex - startIndex;
    int[] dictIdBuffer = new int[numValues];
    _rawDataReader.readIntAndRecordRanges(startIndex, numValues, dictIdBuffer, _rawDataReaderStartOffset, ranges);

    // Update context
    context._docId = docId;
    context._endOffset = endIndex;

    return ranges;
  }

  @Override
  public boolean isFixedOffsetType() {
    return false;
  }

  @Override
  public long getBaseOffset() {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public int getDocLength() {
    throw new UnsupportedOperationException("Unsupported");
  }

  public static class Context implements ForwardIndexReaderContext {
    private int _docId = -1;
    // Exclusive
    private int _endOffset = 0;

    @Override
    public void close() {
    }
  }
}
