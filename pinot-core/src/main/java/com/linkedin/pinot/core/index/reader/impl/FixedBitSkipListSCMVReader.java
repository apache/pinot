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
package com.linkedin.pinot.core.index.reader.impl;

import com.google.common.primitives.Ints;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.util.CustomBitSet;


/**
 * Storage Layout
 * ==============
 * There will be three sections HEADER section, BITMAP and  RAW DATA
 * CHUNK OFFSET HEADER will contain one line per chunk, each line corresponding to the start offset and length of the chunk
 * BITMAP This will contain sequence of bits. The number of bits will be equal to the totalNumberOfValues.A bit is set to 1 if its start of a new docId. The number of bits set to 1 will be equal to the number of docs.
 * RAWDATA This simply has the actual multi valued data stored in sequence of int's. The number of ints is equal to the totalNumberOfValues
 * We divide all the documents into groups refered to as CHUNK. Each CHUNK will
 * - Have the same number of documents.
 * - Started Offset of each CHUNK in the BITMAP will stored in the HEADER section. This is to speed the look up.
 * Over all each look up will take log(NUM CHUNKS) for binary search + CHUNK to linear scan on the bitmap to find the right offset in the raw data section
 *
 */
public class FixedBitSkipListSCMVReader implements SingleColumnMultiValueReader {
  private static int SIZE_OF_INT = 4;
  private static int NUM_COLS_IN_HEADER = 1;
  private int PREFERRED_NUM_VALUES_PER_CHUNK = 2048;
  private ByteBuffer chunkOffsetsBuffer;
  private ByteBuffer bitsetBuffer;
  private ByteBuffer rawDataBuffer;
  private RandomAccessFile raf;
  private FixedByteWidthRowColDataFileReader chunkOffsetsReader;
  private CustomBitSet customBitSet;
  private FixedBitWidthRowColDataFileReader rawDataReader;
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

  public FixedBitSkipListSCMVReader(File file, int numDocs, int totalNumValues, int columnSizeInBits, boolean signed,
      boolean isMmap) throws Exception {
    this.numDocs = numDocs;
    this.totalNumValues = totalNumValues;
    float averageValuesPerDoc = totalNumValues / numDocs;
    this.docsPerChunk = (int) (Math.ceil(PREFERRED_NUM_VALUES_PER_CHUNK / averageValuesPerDoc));
    this.numChunks = (numDocs + docsPerChunk - 1) / docsPerChunk;
    chunkOffsetHeaderSize = numChunks * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    bitsetSize = (totalNumValues + 7) / 8;
    rawDataSize = (totalNumValues * columnSizeInBits + 7) / 8;
    totalSize = chunkOffsetHeaderSize + bitsetSize + rawDataSize;
    raf = new RandomAccessFile(file, "rw");
    this.isMmap = isMmap;
    if (isMmap) {
      chunkOffsetsBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, chunkOffsetHeaderSize);
      bitsetBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, chunkOffsetHeaderSize, bitsetSize);
      rawDataBuffer =
          raf.getChannel().map(FileChannel.MapMode.READ_WRITE, chunkOffsetHeaderSize + bitsetSize, rawDataSize);

      chunkOffsetsReader =
          new FixedByteWidthRowColDataFileReader(chunkOffsetsBuffer, numDocs, NUM_COLS_IN_HEADER,
              new int[] { SIZE_OF_INT });

      customBitSet = CustomBitSet.withByteBuffer(bitsetSize, bitsetBuffer);
      rawDataReader =
          FixedBitWidthRowColDataFileReader.forByteBuffer(rawDataBuffer, totalNumValues, 1,
              new int[] { columnSizeInBits }, new boolean[] { signed });
    } else {
      chunkOffsetsBuffer = ByteBuffer.allocateDirect(chunkOffsetHeaderSize);
      raf.getChannel().read(chunkOffsetsBuffer);
      chunkOffsetsReader =
          new FixedByteWidthRowColDataFileReader(chunkOffsetsBuffer, numDocs, NUM_COLS_IN_HEADER,
              new int[] { SIZE_OF_INT });
      bitsetBuffer = ByteBuffer.allocateDirect(bitsetSize);
      raf.getChannel().read(bitsetBuffer);
      customBitSet = CustomBitSet.withByteBuffer(bitsetSize, bitsetBuffer);
      rawDataBuffer = ByteBuffer.allocateDirect(rawDataSize);
      raf.getChannel().read(rawDataBuffer);
      rawDataReader =
          FixedBitWidthRowColDataFileReader.forByteBuffer(rawDataBuffer, totalNumValues, 1,
              new int[] { columnSizeInBits }, new boolean[] { signed });
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
    if (isMmap) {
      MmapUtils.unloadByteBuffer(chunkOffsetsBuffer);
      MmapUtils.unloadByteBuffer(bitsetBuffer);
      MmapUtils.unloadByteBuffer(rawDataBuffer);
      raf.close();
    } else {
      chunkOffsetsBuffer.clear();
      bitsetBuffer.clear();
      rawDataBuffer.clear();
    }
  }

  public int getDocsPerChunk() {
    return docsPerChunk;
  }

  private int computeLength(int rowOffSetStart) {
    int rowOffSetEnd = Ints.checkedCast(customBitSet.nextSetBitAfter(rowOffSetStart));
    if (rowOffSetEnd < 0) {
      return totalNumValues - rowOffSetStart;
    }
    return rowOffSetEnd - rowOffSetStart;
  }

  private int computeStartOffset(int row) {
    int chunkId = row / docsPerChunk;
    int chunkIdOffset = chunkOffsetsReader.getInt(chunkId, 0);
    if (row % docsPerChunk == 0) {
      return chunkIdOffset;
    }
    long rowOffSetStart = customBitSet.findNthBitSetAfter(chunkIdOffset, row - chunkId * docsPerChunk);
    return Ints.checkedCast(rowOffSetStart);
  }

  @Override
  public DataFileMetadata getMetadata() {
    throw new UnsupportedOperationException("Storing metadata in the file is not yet supported");
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
      intArray[i] = rawDataReader.getInt(startOffset + i, 0);
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
