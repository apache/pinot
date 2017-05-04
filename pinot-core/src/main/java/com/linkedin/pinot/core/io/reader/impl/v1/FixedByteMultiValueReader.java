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
import com.linkedin.pinot.core.io.reader.BaseSingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.util.PinotDataCustomBitSet;
import java.io.IOException;


/**
 * Storage Layout
 * ==============
 * There will be three sections HEADER section, BITMAP and  RAW DATA
 * CHUNK OFFSET HEADER will contain one line per chunk, each line corresponding to the start offset and length of the chunk
 * BITMAP This will contain sequence of bits. The number of bits will be equal to the totalNumberOfValues.A bit is set to 1 if its start of a new docId. The number of bits set to 1 will be equal to the number of docs.
 * RAWDATA This section contains the multi valued data stored in sequence of int's. The number of ints is equal to the totalNumberOfValues
 * We divide all the documents into groups referred to as CHUNK. Each CHUNK will
 * - Have the same number of documents.
 * - Started Offset of each CHUNK in the BITMAP will stored in the HEADER section. This is to speed the look up.
 * Over all each look up will take log(NUM CHUNKS) for binary search + CHUNK to linear scan on the bitmap to find the right offset in the raw data section
 *
 */
public class FixedByteMultiValueReader extends BaseSingleColumnMultiValueReader {
  private static final int SIZE_OF_INT = 4;
  private static final int NUM_COLS_IN_HEADER = 1;
  //THIS is HARDCODED in THE FixedByteSkipListSCMVWriter class as well.
  //If you are changing PREFERRED_NUM_VALUES_PER_CHUNK, make sure you change this in FixedByteSkipListSCMVWriter class as well
  private static final int PREFERRED_NUM_VALUES_PER_CHUNK = 2048;

  private PinotDataBuffer indexDataBuffer;
  private PinotDataBuffer chunkOffsetsBuffer;
  private PinotDataBuffer bitsetBuffer;
  private PinotDataBuffer rawDataBuffer;
  private FixedByteSingleValueMultiColReader chunkOffsetsReader;
  private PinotDataCustomBitSet customBitSet;
  private FixedByteSingleValueMultiColReader rawDataReader;
  private int numChunks;
  private int chunkOffsetHeaderSize;
  private int bitsetSize;
  private int rawDataSize;
  private int totalSize;
  private int totalNumValues;
  private int docsPerChunk;

  public FixedByteMultiValueReader(PinotDataBuffer indexDataBuffer,
      int numDocs, int totalNumValues, int columnSizeInBytes) {
    this.indexDataBuffer = indexDataBuffer;
    this.totalNumValues = totalNumValues;
    float averageValuesPerDoc = totalNumValues / numDocs;
    this.docsPerChunk = (int) (Math.ceil(PREFERRED_NUM_VALUES_PER_CHUNK / averageValuesPerDoc));
    this.numChunks = (numDocs + docsPerChunk - 1) / docsPerChunk;
    chunkOffsetHeaderSize = numChunks * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    bitsetSize = (totalNumValues + 7) / 8;
    rawDataSize = totalNumValues * columnSizeInBytes;
    totalSize = chunkOffsetHeaderSize + bitsetSize + rawDataSize;
    Preconditions.checkState(totalSize > 0 && totalSize < Integer.MAX_VALUE, "Total size can not exceed 2GB");
    chunkOffsetsBuffer = indexDataBuffer.view(0, chunkOffsetHeaderSize);
    chunkOffsetsReader = new FixedByteSingleValueMultiColReader(chunkOffsetsBuffer, numChunks, new int[] { SIZE_OF_INT });
    int bitsetEndPos = chunkOffsetHeaderSize + bitsetSize;
    bitsetBuffer = indexDataBuffer.view(chunkOffsetHeaderSize, bitsetEndPos);
    rawDataBuffer = indexDataBuffer.view(bitsetEndPos, bitsetEndPos + rawDataSize);
    customBitSet = PinotDataCustomBitSet.withDataBuffer(bitsetSize, bitsetBuffer);
    rawDataReader = new FixedByteSingleValueMultiColReader(rawDataBuffer, totalNumValues, new int[] { columnSizeInBytes });
  }


  public int getTotalSize() {
    return totalSize;
  }

  private int computeLength(int rowOffSetStart) {
    long rowOffSetEnd = customBitSet.nextSetBitAfter(rowOffSetStart);
    if (rowOffSetEnd < 0) {
      return totalNumValues - rowOffSetStart;
    }
    return (int)(rowOffSetEnd - rowOffSetStart);
  }

  private int computeStartOffset(int row) {
    int chunkId = row / docsPerChunk;
    int chunkIdOffset = chunkOffsetsReader.getInt(chunkId, 0);
    if (row % docsPerChunk == 0) {
      return chunkIdOffset;
    }
    long rowOffSetStart = customBitSet.findNthBitSetAfter(chunkIdOffset, row - chunkId * docsPerChunk);
    return (int)(rowOffSetStart);
  }

  @Override
  public void close() throws IOException {
    chunkOffsetsReader.close();
    rawDataReader.close();
    customBitSet.close();
    indexDataBuffer.close();
  }

  public int getDocsPerChunk() {
    return docsPerChunk;
  }

  @Override
  public int getCharArray(int row, char[] charArray) {
    int startOffset = computeStartOffset(row);
    int length = computeLength(startOffset);
    for (int i = 0; i < length; i++) {
      charArray[i] = rawDataReader.getChar(startOffset + i, 0);
    }
    return length;
  }

  @Override
  public int getShortArray(int row, short[] shortsArray) {
    int startOffset = computeStartOffset(row);
    int length = computeLength(startOffset);
    for (int i = 0; i < length; i++) {
      shortsArray[i] = rawDataReader.getShort(startOffset + i, 0);
    }
    return length;
  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    int startOffset = computeStartOffset(row);
    int length = computeLength(startOffset);
    //System.out.println("row:" + row + " startOffset:" + startOffset + " length:" + length);
    for (int i = 0; i < length; i++) {
      intArray[i] = rawDataReader.getInt(startOffset + i, 0);
    }
    return length;
  }

  @Override
  public int getLongArray(int row, long[] longArray) {
    int startOffset = computeStartOffset(row);
    int length = computeLength(startOffset);
    for (int i = 0; i < length; i++) {
      longArray[i] = rawDataReader.getLong(startOffset + i, 0);
    }
    return length;
  }

  @Override
  public int getFloatArray(int row, float[] floatArray) {
    int startOffset = computeStartOffset(row);
    int length = computeLength(startOffset);
    for (int i = 0; i < length; i++) {
      floatArray[i] = rawDataReader.getFloat(startOffset + i, 0);
    }
    return length;
  }

  @Override
  public int getDoubleArray(int row, double[] doubleArray) {
    int startOffset = computeStartOffset(row);
    int length = computeLength(startOffset);
    for (int i = 0; i < length; i++) {
      doubleArray[i] = rawDataReader.getDouble(startOffset + i, 0);
    }
    return length;
  }

  @Override
  public int getStringArray(int row, String[] stringArray) {
    int startOffset = computeStartOffset(row);
    int length = computeLength(startOffset);
    for (int i = 0; i < length; i++) {
      stringArray[i] = rawDataReader.getString(startOffset + i, 0);
    }
    return length;
  }

  @Override
  public int getBytesArray(int row, byte[][] bytesArray) {
    int startOffset = computeStartOffset(row);
    int length = computeLength(startOffset);
    for (int i = 0; i < length; i++) {
      bytesArray[i] = rawDataReader.getBytes(startOffset + i, 0);
    }
    return length;
  }

}
