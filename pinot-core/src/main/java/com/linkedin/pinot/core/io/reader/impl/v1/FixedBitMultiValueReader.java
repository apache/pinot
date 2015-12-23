/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import com.google.common.primitives.Ints;
import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.FixedBitSingleValueMultiColReader;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.util.CustomBitSet;

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
  private static int SIZE_OF_INT = 4;
  private static int NUM_COLS_IN_HEADER = 1;
  private int PREFERRED_NUM_VALUES_PER_CHUNK = 2048;
  private ByteBuffer chunkOffsetsBuffer;
  private ByteBuffer bitsetBuffer;
  private ByteBuffer rawDataBuffer;
  private RandomAccessFile raf;
  private FixedByteSingleValueMultiColReader chunkOffsetsReader;
  private CustomBitSet customBitSet;
  private FixedBitSingleValueMultiColReader rawDataReader;
  private int numChunks;
  int prevRowStartIndex = 0;
  int prevRowLength = 0;
  int prevRowId = -1;
  private int chunkOffsetHeaderSize;
  private int bitsetSize;
  private int rawDataSize;
  private int totalSize;
  private int totalNumValues;
  private int docsPerChunk;
  private int numDocs;
  private boolean isMmap;

  public FixedBitMultiValueReader(File file, int numDocs, int totalNumValues, int columnSizeInBits,
      boolean signed, boolean isMmap) throws Exception {
    this.numDocs = numDocs;
    this.totalNumValues = totalNumValues;
    float averageValuesPerDoc = totalNumValues / numDocs;
    this.docsPerChunk = (int) (Math.ceil(PREFERRED_NUM_VALUES_PER_CHUNK / averageValuesPerDoc));
    this.numChunks = (numDocs + docsPerChunk - 1) / docsPerChunk;
    chunkOffsetHeaderSize = numChunks * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    bitsetSize = (totalNumValues + 7) / 8;
    rawDataSize = Ints.checkedCast(((long) totalNumValues * columnSizeInBits + 7) / 8);
    totalSize = chunkOffsetHeaderSize + bitsetSize + rawDataSize;
    raf = new RandomAccessFile(file, "rw");
    this.isMmap = isMmap;
    if (isMmap) {
      chunkOffsetsBuffer = MmapUtils.mmapFile(raf, FileChannel.MapMode.READ_WRITE, 0,
          chunkOffsetHeaderSize, file, this.getClass().getSimpleName() + " chunkOffsetsBuffer");
      bitsetBuffer = MmapUtils.mmapFile(raf, FileChannel.MapMode.READ_WRITE, chunkOffsetHeaderSize,
          bitsetSize, file, this.getClass().getSimpleName() + " bitsetBuffer");
      rawDataBuffer = MmapUtils.mmapFile(raf, FileChannel.MapMode.READ_WRITE,
          chunkOffsetHeaderSize + bitsetSize, rawDataSize, file,
          this.getClass().getSimpleName() + " rawDataBuffer");

      chunkOffsetsReader = new FixedByteSingleValueMultiColReader(chunkOffsetsBuffer, numDocs,
          NUM_COLS_IN_HEADER, new int[] {
              SIZE_OF_INT
      });

      customBitSet = CustomBitSet.withByteBuffer(bitsetSize, bitsetBuffer);
      rawDataReader = FixedBitSingleValueMultiColReader.forByteBuffer(rawDataBuffer, totalNumValues,
          1, new int[] {
              columnSizeInBits
      }, new boolean[] {
          signed
      });
    } else {
      chunkOffsetsBuffer = MmapUtils.allocateDirectByteBuffer(chunkOffsetHeaderSize, file,
          this.getClass().getSimpleName() + " chunkOffsetsBuffer");
      raf.getChannel().read(chunkOffsetsBuffer);
      chunkOffsetsReader = new FixedByteSingleValueMultiColReader(chunkOffsetsBuffer, numDocs,
          NUM_COLS_IN_HEADER, new int[] {
              SIZE_OF_INT
      });
      bitsetBuffer = MmapUtils.allocateDirectByteBuffer(bitsetSize, file,
          this.getClass().getSimpleName() + " bitsetBuffer");
      raf.getChannel().read(bitsetBuffer);
      customBitSet = CustomBitSet.withByteBuffer(bitsetSize, bitsetBuffer);
      rawDataBuffer = MmapUtils.allocateDirectByteBuffer(rawDataSize, file,
          this.getClass().getSimpleName() + " rawDataBuffer");
      raf.getChannel().read(rawDataBuffer);
      rawDataReader = FixedBitSingleValueMultiColReader.forByteBuffer(rawDataBuffer, totalNumValues,
          1, new int[] {
              columnSizeInBits
      }, new boolean[] {
          signed
      });
      raf.close();
    }
  }

  public int getChunkOffsetHeaderSize() {
    return chunkOffsetHeaderSize;
  }

  public int getBitsetSize() {
    return bitsetSize;
  }

  public int getRawDataSize() {
    return rawDataSize;
  }

  public int getTotalSize() {
    return totalSize;
  }

  public ByteBuffer getChunkOffsetsBuffer() {
    return chunkOffsetsBuffer;
  }

  public ByteBuffer getBitsetBuffer() {
    return bitsetBuffer;
  }

  public ByteBuffer getRawDataBuffer() {
    return rawDataBuffer;
  }

  public int getNumChunks() {
    return numChunks;
  }

  public int getRowsPerChunk() {
    return docsPerChunk;
  }

  @Override
  public void close() throws IOException {
    MmapUtils.unloadByteBuffer(chunkOffsetsBuffer);
    chunkOffsetsBuffer = null;
    MmapUtils.unloadByteBuffer(bitsetBuffer);
    bitsetBuffer = null;
    MmapUtils.unloadByteBuffer(rawDataBuffer);
    rawDataBuffer = null;
    customBitSet.close();
    customBitSet = null;
    rawDataReader.close();
    rawDataReader = null;

    if (isMmap) {
      raf.close();
    }
  }

  public int getDocsPerChunk() {
    return docsPerChunk;
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
    return (int) customBitSet.findNthBitSetAfter(chunkIdOffset, row - chunkId * docsPerChunk);
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
    if (readerContext.chunkId == chunkId) {
      if (row == readerContext.rowId + 1) {
        startOffset = readerContext.endPosition;
      } else if (row >= readerContext.rowId) {
        startOffset = (int) customBitSet.findNthBitSetAfter(readerContext.endPosition,
            (row - readerContext.rowId - 1));
      }
    } else {
      int chunkIdOffset = chunkOffsetsReader.getInt(chunkId, 0);
      if (row % docsPerChunk == 0) {
        startOffset = chunkIdOffset;
      } else {
        startOffset =
            (int) customBitSet.findNthBitSetAfter(chunkIdOffset, row - chunkId * docsPerChunk);
      }
    }
    endOffset = customBitSet.findNthBitSetAfter(startOffset, 1);
    if (endOffset < 0) {
      length = (int) (totalNumValues - startOffset);
    } else {
      length = (int) (endOffset - startOffset);
    }
    rawDataReader.getInt(startOffset, length, 0, intArray);
    readerContext.rowId = row;
    readerContext.chunkId = chunkId;
    readerContext.startPosition = startOffset;
    readerContext.endPosition = endOffset;
    //validate(row, intArray, length);
    return length;

  }
  
  //for testing reader context
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
