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
package com.linkedin.pinot.core.index.writer.impl;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.io.IOUtils;

import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.writer.SingleColumnMultiValueWriter;
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
public class FixedByteSkipListSCMVWriter implements SingleColumnMultiValueWriter {
  private static int SIZE_OF_INT = 4;
  private static int NUM_COLS_IN_HEADER = 1;
  private int PREFERRED_NUM_VALUES_PER_CHUNK = 2048;
  private ByteBuffer chunkOffsetsBuffer;
  private ByteBuffer bitsetBuffer;
  private ByteBuffer rawDataBuffer;
  private RandomAccessFile raf;
  private FixedByteWidthRowColDataFileWriter chunkOffsetsWriter;
  private CustomBitSet customBitSet;
  private FixedByteWidthRowColDataFileWriter rawDataWriter;
  private int numChunks;
  int prevRowStartIndex = 0;
  int prevRowLength = 0;
  int prevRowId = -1;
  private int chunkOffsetHeaderSize;
  private int bitsetSize;
  private int rawDataSize;
  private int totalSize;
  private int docsPerChunk;

  public FixedByteSkipListSCMVWriter(File file, int numDocs, int totalNumValues, int columnSizeInBytes)
      throws Exception {
    float averageValuesPerDoc = totalNumValues / numDocs;
    this.docsPerChunk = (int) (Math.ceil(PREFERRED_NUM_VALUES_PER_CHUNK / averageValuesPerDoc));
    this.numChunks = (numDocs + docsPerChunk - 1) / docsPerChunk;
    chunkOffsetHeaderSize = numChunks * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    bitsetSize = (totalNumValues + 7) / 8;
    rawDataSize = totalNumValues * columnSizeInBytes;
    totalSize = chunkOffsetHeaderSize + bitsetSize + rawDataSize;
    raf = new RandomAccessFile(file, "rw");
    chunkOffsetsBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, chunkOffsetHeaderSize);
    bitsetBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, chunkOffsetHeaderSize, bitsetSize);
    rawDataBuffer =
        raf.getChannel().map(FileChannel.MapMode.READ_WRITE, chunkOffsetHeaderSize + bitsetSize, rawDataSize);

    chunkOffsetsWriter =
        new FixedByteWidthRowColDataFileWriter(chunkOffsetsBuffer, numDocs, NUM_COLS_IN_HEADER,
            new int[] { SIZE_OF_INT });

    customBitSet = CustomBitSet.withByteBuffer(bitsetSize, bitsetBuffer);
    rawDataWriter =
        new FixedByteWidthRowColDataFileWriter(rawDataBuffer, totalNumValues, 1, new int[] { columnSizeInBytes });

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

  @Override
  public boolean setMetadata(DataFileMetadata metadata) {
    return false;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(raf);
    raf = null;
    MmapUtils.unloadByteBuffer(chunkOffsetsBuffer);
    MmapUtils.unloadByteBuffer(bitsetBuffer);
    MmapUtils.unloadByteBuffer(rawDataBuffer);
  }

  private int updateHeader(int rowId, int length) {
    assert (rowId == prevRowId + 1);
    int newStartIndex = prevRowStartIndex + prevRowLength;
    if (rowId % docsPerChunk == 0) {
      int chunkId = rowId / docsPerChunk;
      //System.out.println("chunkId:" + chunkId + "rowId:" + rowId + " startIndex:" + newStartIndex);
      chunkOffsetsWriter.setInt(chunkId, 0, newStartIndex);
    }
    customBitSet.setBit(newStartIndex);
    prevRowStartIndex = newStartIndex;
    prevRowLength = length;
    prevRowId = rowId;
    return newStartIndex;
  }

  @Override
  public void setCharArray(int row, char[] charArray) {
    int newStartIndex = updateHeader(row, charArray.length);
    for (int i = 0; i < charArray.length; i++) {
      rawDataWriter.setChar(newStartIndex + i, 0, charArray[i]);
    }
  }

  @Override
  public void setShortArray(int row, short[] shortsArray) {
    int newStartIndex = updateHeader(row, shortsArray.length);
    for (int i = 0; i < shortsArray.length; i++) {
      rawDataWriter.setShort(newStartIndex + i, 0, shortsArray[i]);
    }
  }

  @Override
  public void setIntArray(int row, int[] intArray) {
    int newStartIndex = updateHeader(row, intArray.length);
    for (int i = 0; i < intArray.length; i++) {
      rawDataWriter.setInt(newStartIndex + i, 0, intArray[i]);
    }
  }

  @Override
  public void setLongArray(int row, long[] longArray) {
    int newStartIndex = updateHeader(row, longArray.length);
    for (int i = 0; i < longArray.length; i++) {
      rawDataWriter.setLong(newStartIndex + i, 0, longArray[i]);
    }
  }

  @Override
  public void setFloatArray(int row, float[] floatArray) {
    int newStartIndex = updateHeader(row, floatArray.length);
    for (int i = 0; i < floatArray.length; i++) {
      rawDataWriter.setFloat(newStartIndex + i, 0, floatArray[i]);
    }
  }

  @Override
  public void setDoubleArray(int row, double[] doubleArray) {
    int newStartIndex = updateHeader(row, doubleArray.length);
    for (int i = 0; i < doubleArray.length; i++) {
      rawDataWriter.setDouble(newStartIndex + i, 0, doubleArray[i]);
    }
  }

  @Override
  public void setStringArray(int row, String[] stringArray) {
    int newStartIndex = updateHeader(row, stringArray.length);
    for (int i = 0; i < stringArray.length; i++) {
      rawDataWriter.setString(newStartIndex + i, 0, stringArray[i]);
    }
  }

  @Override
  public void setBytesArray(int row, byte[][] bytesArray) {
    int newStartIndex = updateHeader(row, bytesArray.length);
    for (int i = 0; i < bytesArray.length; i++) {
      rawDataWriter.setBytes(newStartIndex + i, 0, bytesArray[i]);
    }
  }

  public int getDocsPerChunk() {
    return docsPerChunk;
  }

}
