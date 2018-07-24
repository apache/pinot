/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.io.reader.impl.v1;

import com.linkedin.pinot.core.io.reader.BaseSingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.util.FixedBitIntReaderWriter;
import com.linkedin.pinot.core.io.util.FixedByteValueReaderWriter;
import com.linkedin.pinot.core.io.util.PinotDataBitSet;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;


/**
 * Storage Layout:
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
public final class FixedBitMultiValueReader extends BaseSingleColumnMultiValueReader<FixedBitMultiValueReader.Context> {
  private static final int PREFERRED_NUM_VALUES_PER_CHUNK = 2048;

  private final PinotDataBuffer _dataBuffer;
  private final FixedByteValueReaderWriter _chunkOffsetReader;
  private final PinotDataBitSet _bitmapReader;
  private final FixedBitIntReaderWriter _rawDataReader;
  private final int _numRows;
  private final int _numValues;
  private final int _numRowsPerChunk;

  public FixedBitMultiValueReader(PinotDataBuffer dataBuffer, int numRows, int numValues, int numBitsPerValue) {
    _dataBuffer = dataBuffer;
    _numRows = numRows;
    _numValues = numValues;
    _numRowsPerChunk = (int) (Math.ceil((float) PREFERRED_NUM_VALUES_PER_CHUNK / (numValues / numRows)));
    int numChunks = (numRows + _numRowsPerChunk - 1) / _numRowsPerChunk;
    long endOffset = numChunks * Integer.BYTES;
    _chunkOffsetReader = new FixedByteValueReaderWriter(dataBuffer.view(0L, endOffset));
    int bitmapSize = (numValues + Byte.SIZE - 1) / Byte.SIZE;
    _bitmapReader = new PinotDataBitSet(dataBuffer.view(endOffset, endOffset + bitmapSize));
    endOffset += bitmapSize;
    int rawDataSize = (int) (((long) numValues * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE);
    _rawDataReader =
        new FixedBitIntReaderWriter(dataBuffer.view(endOffset, endOffset + rawDataSize), numValues, numBitsPerValue);
  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    int chunkId = row / _numRowsPerChunk;
    int chunkOffset = _chunkOffsetReader.getInt(chunkId);
    int indexInChunk = row % _numRowsPerChunk;
    int startIndex;
    if (indexInChunk == 0) {
      startIndex = chunkOffset;
    } else {
      startIndex = _bitmapReader.getNextNthSetBitOffset(chunkOffset + 1, indexInChunk);
    }
    int endIndex;
    if (row == _numRows - 1) {
      endIndex = _numValues;
    } else {
      endIndex = _bitmapReader.getNextSetBitOffset(startIndex + 1);
    }
    int numValues = endIndex - startIndex;
    _rawDataReader.readInt(startIndex, numValues, intArray);
    return numValues;
  }

  @Override
  public int getIntArray(int row, int[] intArray, Context context) {
    int contextRow = context._row;
    int contextEndOffset = context._endOffset;
    int startIndex;
    if (row == contextRow + 1) {
      startIndex = contextEndOffset;
    } else {
      int chunkId = row / _numRowsPerChunk;
      if (row > contextRow && chunkId == contextRow / _numRowsPerChunk) {
        // Same chunk
        startIndex = _bitmapReader.getNextNthSetBitOffset(contextEndOffset + 1, row - contextRow - 1);
      } else {
        // Different chunk
        int chunkOffset = _chunkOffsetReader.getInt(chunkId);
        int indexInChunk = row % _numRowsPerChunk;
        if (indexInChunk == 0) {
          startIndex = chunkOffset;
        } else {
          startIndex = _bitmapReader.getNextNthSetBitOffset(chunkOffset + 1, indexInChunk);
        }
      }
    }
    int endIndex;
    if (row == _numRows - 1) {
      endIndex = _numValues;
    } else {
      endIndex = _bitmapReader.getNextSetBitOffset(startIndex + 1);
    }
    int numValues = endIndex - startIndex;
    _rawDataReader.readInt(startIndex, numValues, intArray);

    // Update context
    context._row = row;
    context._endOffset = endIndex;

    return numValues;
  }

  @Override
  public Context createContext() {
    return new Context();
  }

  @Override
  public void close() throws IOException {
    _chunkOffsetReader.close();
    _bitmapReader.close();
    _rawDataReader.close();
    _dataBuffer.close();
  }

  public static class Context implements ReaderContext {
    public int _row = -1;
    // Exclusive
    public int _endOffset = 0;
  }
}
