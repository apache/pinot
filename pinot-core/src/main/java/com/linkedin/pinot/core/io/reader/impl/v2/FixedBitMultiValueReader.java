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
package com.linkedin.pinot.core.io.reader.impl.v2;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.util.PinotDataCustomBitSet;
import com.linkedin.pinot.core.util.SizeUtil;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class FixedBitMultiValueReader extends BaseSingleColumnMultiValueReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedBitMultiValueReader.class);

  private static int SIZE_OF_INT = 4;
  private static int NUM_COLS_IN_HEADER = 1;
  private int PREFERRED_NUM_VALUES_PER_CHUNK = 2048;

  private PinotDataBuffer indexDataBuffer;
  private PinotDataBuffer chunkOffsetsBuffer;
  private PinotDataBuffer bitsetBuffer;
  private PinotDataBuffer rawDataBuffer;
  private FixedByteSingleValueMultiColReader chunkOffsetsReader;
  private PinotDataCustomBitSet customBitSet;
  private FixedBitSingleValueReader rawDataReader;
  private int numChunks;
  private int chunkOffsetHeaderSize;
  private int bitsetSize;
  private int rawDataSize;
  private int totalSize;
  private int totalNumValues;
  private int docsPerChunk;
  private int numDocs;

  public FixedBitMultiValueReader(PinotDataBuffer indexDataBuffer, int numDocs,
      int totalNumValues, int columnSizeInBits, boolean signed) {
    this.indexDataBuffer = indexDataBuffer;
    this.numDocs = numDocs;
    this.totalNumValues = totalNumValues;
    float averageValuesPerDoc = totalNumValues / numDocs;
    this.docsPerChunk = (int) (Math.ceil(PREFERRED_NUM_VALUES_PER_CHUNK / averageValuesPerDoc));
    LOGGER.debug(
        " Loading index data buffer of size:{} totalDocs:{} totalNumOfEntries:{} docsPerChunk:{}",
        indexDataBuffer.size(), numDocs, totalNumValues, docsPerChunk);
    this.numChunks = (numDocs + docsPerChunk - 1) / docsPerChunk;
    chunkOffsetHeaderSize = numChunks * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    bitsetSize = (totalNumValues + 7) / 8;

    rawDataSize = SizeUtil.computeBytesRequired(totalNumValues, columnSizeInBits,
        SizeUtil.BIT_UNPACK_BATCH_SIZE);

    totalSize = chunkOffsetHeaderSize + bitsetSize + rawDataSize;
    Preconditions.checkState(totalSize > 0 && totalSize < Integer.MAX_VALUE, "Total size can not exceed 2GB");
    chunkOffsetsBuffer = indexDataBuffer.view(0, chunkOffsetHeaderSize);
    int bitsetEndPos = chunkOffsetHeaderSize + bitsetSize;
    bitsetBuffer = indexDataBuffer.view(chunkOffsetHeaderSize, bitsetEndPos);
    rawDataBuffer = indexDataBuffer.view(bitsetEndPos, bitsetEndPos+rawDataSize);
    chunkOffsetsReader = new FixedByteSingleValueMultiColReader(chunkOffsetsBuffer, numChunks, new int[] {
        SIZE_OF_INT
    });

    customBitSet = PinotDataCustomBitSet.withDataBuffer(bitsetSize, bitsetBuffer);
    rawDataReader = new FixedBitSingleValueReader(rawDataBuffer, totalNumValues,
        columnSizeInBits, signed);
  }

  public int getTotalSize() {
    return totalSize;
  }


  @Override
  public void close() throws IOException {
    rawDataReader.close();
    chunkOffsetsReader.close();
    customBitSet.close();
    indexDataBuffer.close();

    chunkOffsetsBuffer = null;
    bitsetBuffer = null;
    rawDataBuffer = null;
    customBitSet = null;
    rawDataReader = null;
    indexDataBuffer = null;
  }

  private int computeLength(int rowOffSetStart) {
    long rowOffSetEnd = customBitSet.nextSetBitAfter(rowOffSetStart);
    if (rowOffSetEnd < 0) {
      return (int) (totalNumValues - rowOffSetStart);
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
  public int getCharArray(int row, char[] charArray) {
    throw new UnsupportedOperationException("Only int data type is supported in fixedbit format");
  }

  @Override
  public int getShortArray(int row, short[] shortsArray) {
    throw new UnsupportedOperationException("Only int data type is supported in fixedbit format");
  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    int startOffset = computeStartOffset(row);
    int length = computeLength(startOffset);
    for (int i = 0; i < length; i++) {
      intArray[i] = rawDataReader.getInt(startOffset + i);
    }
    return length;
  }

  @Override
  public int getLongArray(int row, long[] longArray) {
    throw new UnsupportedOperationException("Only int data type is supported in fixedbit format");
  }

  @Override
  public int getFloatArray(int row, float[] floatArray) {
    throw new UnsupportedOperationException("Only int data type is supported in fixedbit format");
  }

  @Override
  public int getDoubleArray(int row, double[] doubleArray) {
    throw new UnsupportedOperationException("Only int data type is supported in fixedbit format");
  }

  @Override
  public int getStringArray(int row, String[] stringArray) {
    throw new UnsupportedOperationException("Only int data type is supported in fixedbit format");
  }

  @Override
  public int getBytesArray(int row, byte[][] bytesArray) {
    throw new UnsupportedOperationException("Only int data type is supported in fixedbit format");
  }

}
