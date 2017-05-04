/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.FixedBitSingleValueMultiColReader;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.util.PinotDataCustomBitSet;
import java.io.IOException;

/**
 * Storage Layout
 * ==============
 * There will be three sections HEADER section, BITMAP and RAW DATA
 * CHUNK OFFSET HEADER will contain one line per chunk, each line corresponding to the start offset
 * and length of the chunk
 * BITMAP This will contain sequence of bits. The number of bits will be equal to the
 * totalNumberOfValues.A bit is set to 1 if its start of a new docId. The number of bits set to 1
 * will be equal to the number of docs.
 * RAWDATA This simply has the actual multi valued data stored in sequence of int's. The number of
 * ints is equal to the totalNumberOfValues
 * We divide all the documents into groups refered to as CHUNK. Each CHUNK will
 * - Have the same number of documents.
 * - Started Offset of each CHUNK in the BITMAP will stored in the HEADER section. This is to speed
 * the look up.
 * Over all each look up will take log(NUM CHUNKS) for binary search + CHUNK to linear scan on the
 * bitmap to find the right offset in the raw data section
 */
public class FixedBitMultiValueReader
    extends BaseSingleColumnMultiValueReader<MultiValueReaderContext> {

  private static final int SIZE_OF_INT = 4;
  private static final int NUM_COLS_IN_HEADER = 1;
  private static final int PREFERRED_NUM_VALUES_PER_CHUNK = 2048;

  private PinotDataBuffer indexDataBuffer;
  private PinotDataBuffer chunkOffsetsBuffer;
  private PinotDataBuffer bitsetBuffer;
  private PinotDataBuffer rawDataBuffer;
  private FixedByteSingleValueMultiColReader chunkOffsetsReader;
  private PinotDataCustomBitSet customBitSet;
  private FixedBitSingleValueMultiColReader rawDataReader;
  private int numChunks;
  private int chunkOffsetHeaderSize;
  private int bitsetSize;
  private int rawDataSize;
  private int totalSize;
  private int totalNumValues;
  private int docsPerChunk;
  private final int numDocs;

  public FixedBitMultiValueReader(PinotDataBuffer indexDataBuffer, int numDocs, int totalNumValues,
      int columnSizeInBits, boolean signed) {

    this.indexDataBuffer = indexDataBuffer;
    this.numDocs = numDocs;
    this.totalNumValues = totalNumValues;
    float averageValuesPerDoc = totalNumValues / numDocs;
    this.docsPerChunk = (int) (Math.ceil(PREFERRED_NUM_VALUES_PER_CHUNK / averageValuesPerDoc));
    this.numChunks = (numDocs + docsPerChunk - 1) / docsPerChunk;
    chunkOffsetHeaderSize = numChunks * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    bitsetSize = (totalNumValues + 7) / 8;
    rawDataSize = Ints.checkedCast(((long) totalNumValues * columnSizeInBits + 7) / 8);
    totalSize = chunkOffsetHeaderSize + bitsetSize + rawDataSize;
    Preconditions.checkState(totalSize > 0 && totalSize < Integer.MAX_VALUE, "Total size should not exceed 2GB");
    chunkOffsetsBuffer = indexDataBuffer.view(0, chunkOffsetHeaderSize);
    int bitsetEndPos = chunkOffsetHeaderSize + bitsetSize;
    bitsetBuffer = indexDataBuffer.view(chunkOffsetHeaderSize, bitsetEndPos);
    rawDataBuffer = indexDataBuffer.view(bitsetEndPos, bitsetEndPos + rawDataSize);
    chunkOffsetsReader = new FixedByteSingleValueMultiColReader(chunkOffsetsBuffer, numChunks, new int[] {
        SIZE_OF_INT
    });

    customBitSet = PinotDataCustomBitSet.withDataBuffer(bitsetSize, bitsetBuffer);
    rawDataReader = new FixedBitSingleValueMultiColReader(rawDataBuffer, totalNumValues,
        1, new int[] {
            columnSizeInBits
        }, new boolean[] {
            signed
        });
  }

  public int getTotalSize() {
    return totalSize;
  }

  @Override
  public void close() throws IOException {
    customBitSet.close();
    customBitSet = null;
    rawDataReader.close();
    rawDataReader = null;
    chunkOffsetsReader.close();
    indexDataBuffer.close();
  }

  private int computeLength(int rowOffSetStart) {
    long rowOffSetEnd = customBitSet.nextSetBitAfter(rowOffSetStart);
    if (rowOffSetEnd < 0) {
      return (totalNumValues - rowOffSetStart);
    }
    return (int) (rowOffSetEnd - rowOffSetStart);
  }

  private int computeStartOffset(int row) {
    int chunkId = row / docsPerChunk;
    int chunkIdOffset = chunkOffsetsReader.getInt(chunkId, 0);
    if (row % docsPerChunk == 0) {
      return chunkIdOffset;
    }
    return customBitSet.findNthBitSetAfter(chunkIdOffset, row - chunkId * docsPerChunk);
  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    int startOffset = computeStartOffset(row);
    int length = computeLength(startOffset);
    for (int i = 0; i < length; i++) {
      intArray[i] = rawDataReader.getInt(startOffset + i, 0);
    }
    return length;
  }

  @Override
  public int getIntArray(int row, int[] intArray, MultiValueReaderContext readerContext) {
    int startOffset = 0;
    int endOffset = 0;
    int chunkId = row / docsPerChunk;
    int length;
    if (readerContext.chunkId == chunkId && row > readerContext.rowId ) {
      if (row == readerContext.rowId + 1) {
        startOffset = readerContext.endPosition;
      } else  {
        startOffset = customBitSet.findNthBitSetAfter(readerContext.endPosition,
            (row - readerContext.rowId - 1));
      }
    } else {
      int chunkIdOffset = chunkOffsetsReader.getInt(chunkId, 0);
      if (row % docsPerChunk == 0) {
        startOffset = chunkIdOffset;
      } else {
        startOffset =
            customBitSet.findNthBitSetAfter(chunkIdOffset, row - chunkId * docsPerChunk);
      }
    }
    endOffset = customBitSet.findNthBitSetAfter(startOffset, 1);
    if (endOffset < 0) {
      length = totalNumValues - startOffset;
    } else {
      length = endOffset - startOffset;
    }
    rawDataReader.getInt(startOffset, length, 0, intArray);
    readerContext.rowId = row;
    readerContext.chunkId = chunkId;
    readerContext.startPosition = startOffset;
    readerContext.endPosition = endOffset;
    return length;

  }

  // for testing reader context
  private void validate(int row, int[] intArray, int length) {
    int[] temp = new int[intArray.length];
    int tempLength = getIntArray(row, temp);
    if (tempLength != length) {
      System.out.println("ERROR");
    } else {
      for (int i = 0; i < length; i++) {
        if (temp[i] != intArray[i]) {
          System.out.println("ERROR expected startOffset:" + computeStartOffset(row));
        }
      }
    }
  }

  @Override
  public MultiValueReaderContext createContext() {
    MultiValueReaderContext readerContext = new MultiValueReaderContext();
    return readerContext;
  }
}
