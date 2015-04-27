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

import com.linkedin.pinot.common.utils.MmapUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthRowColDataFileReader;
import com.linkedin.pinot.core.index.writer.SingleColumnMultiValueWriter;

import org.apache.commons.io.IOUtils;


public class FixedByteWidthSingleColumnMultiValueReaderWriter implements SingleColumnMultiValueWriter{
  private static int SIZE_OF_INT = 4;
  private static int NUM_COLS_IN_HEADER = 2;
  private ByteBuffer headerBuffer;
  private List<ByteBuffer> dataBuffers;
  private RandomAccessFile raf;
  private FixedByteWidthRowColDataFileWriter headerWriter;
  private FixedByteWidthRowColDataFileReader headerReader;
  
  private List<FixedByteWidthRowColDataFileWriter> dataWriters;
  private FixedByteWidthRowColDataFileWriter currentDataWriter;
  int currentDataWriterIndex = 0;
  int currentCapacity = 0;
  private int headerSize;
  private int incrementalCapacity;
  private int columnSizeInBytes;

  public FixedByteWidthSingleColumnMultiValueReaderWriter(File file, int numDocs, int initialCapacity,
      int incrementalCapacity, int columnSizeInBytes) throws Exception {
    this.incrementalCapacity = incrementalCapacity;
    this.columnSizeInBytes = columnSizeInBytes;
    headerSize = numDocs * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    raf = new RandomAccessFile(file, "rw");
    headerBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, headerSize);
    headerWriter =
        new FixedByteWidthRowColDataFileWriter(headerBuffer, numDocs, 3,
            new int[] { SIZE_OF_INT, SIZE_OF_INT, SIZE_OF_INT });
    headerReader =
        new FixedByteWidthRowColDataFileReader(headerBuffer, numDocs, 3,
            new int[] { SIZE_OF_INT, SIZE_OF_INT, SIZE_OF_INT });
    addCapacity(initialCapacity);
  }

  private void addCapacity(int capacity) throws RuntimeException {
    ByteBuffer dataBuffer;
    try {
      dataBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, headerSize + currentCapacity, capacity);
      dataBuffers.add(dataBuffer);
      currentDataWriter =
          new FixedByteWidthRowColDataFileWriter(dataBuffer, capacity, 1, new int[] { columnSizeInBytes });
      dataWriters.add(currentDataWriter);
      currentCapacity = currentCapacity  + capacity;
      currentDataWriterIndex = currentDataWriterIndex + 1;
    } catch (Exception e) {
      throw new RuntimeException("Error while expanding the capacity", e);
    }
  }

  @Override
  public boolean setMetadata(DataFileMetadata metadata) {
    return false;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(raf);
    raf = null;
    for (ByteBuffer dataBuffer : dataBuffers) {
      MmapUtils.unloadByteBuffer(dataBuffer);
    }
    MmapUtils.unloadByteBuffer(headerBuffer);
  }

  private int updateHeader(int row, int length) {
    int prevRowStartIndex = 0;
    int prevRowLength = 0;
    if (row > 0) {
      prevRowStartIndex = headerReader.getInt(row - 1, 0);
      prevRowLength = headerReader.getInt(row - 1, 1);
    }
    int newStartIndex = prevRowStartIndex + prevRowLength;
    if(newStartIndex + length > currentCapacity){
      addCapacity(incrementalCapacity);
      prevRowStartIndex = 0;
      prevRowLength = 0 ;
      newStartIndex = prevRowStartIndex + prevRowLength;
    }
    headerWriter.setInt(row, 0, newStartIndex);
    headerWriter.setInt(row, 1, length);
    headerWriter.setInt(row, 2, currentDataWriterIndex);
    return newStartIndex;
  }

  @Override
  public void setCharArray(int row, char[] charArray) {
    int newStartIndex = updateHeader(row, charArray.length);
    for (int i = 0; i < charArray.length; i++) {
      currentDataWriter.setChar(newStartIndex + i, 0, charArray[i]);
    }
  }

  @Override
  public void setShortArray(int row, short[] shortsArray) {
    int newStartIndex = updateHeader(row, shortsArray.length);
    for (int i = 0; i < shortsArray.length; i++) {
      currentDataWriter.setShort(newStartIndex + i, 0, shortsArray[i]);
    }
  }

  @Override
  public void setIntArray(int row, int[] intArray) {
    int newStartIndex = updateHeader(row, intArray.length);
    for (int i = 0; i < intArray.length; i++) {
      currentDataWriter.setInt(newStartIndex + i, 0, intArray[i]);
    }
  }

  @Override
  public void setLongArray(int row, long[] longArray) {
    int newStartIndex = updateHeader(row, longArray.length);
    for (int i = 0; i < longArray.length; i++) {
      currentDataWriter.setLong(newStartIndex + i, 0, longArray[i]);
    }
  }

  @Override
  public void setFloatArray(int row, float[] floatArray) {
    int newStartIndex = updateHeader(row, floatArray.length);
    for (int i = 0; i < floatArray.length; i++) {
      currentDataWriter.setFloat(newStartIndex + i, 0, floatArray[i]);
    }
  }

  @Override
  public void setDoubleArray(int row, double[] doubleArray) {
    int newStartIndex = updateHeader(row, doubleArray.length);
    for (int i = 0; i < doubleArray.length; i++) {
      currentDataWriter.setDouble(newStartIndex + i, 0, doubleArray[i]);
    }
  }

  @Override
  public void setStringArray(int row, String[] stringArray) {
    int newStartIndex = updateHeader(row, stringArray.length);
    for (int i = 0; i < stringArray.length; i++) {
      currentDataWriter.setString(newStartIndex + i, 0, stringArray[i]);
    }
  }

  @Override
  public void setBytesArray(int row, byte[][] bytesArray) {
    int newStartIndex = updateHeader(row, bytesArray.length);
    for (int i = 0; i < bytesArray.length; i++) {
      currentDataWriter.setBytes(newStartIndex + i, 0, bytesArray[i]);
    }
  }

}
