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
package com.linkedin.pinot.core.io.writer.impl;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.io.writer.SingleColumnMultiValueWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;


public class FixedBitSingleColumnMultiValueWriter implements
    SingleColumnMultiValueWriter {
  private static final int SIZE_OF_INT = 4;
  private static final int NUM_COLS_IN_HEADER = 2;

  private PinotDataBuffer indexDataBuffer;
  private PinotDataBuffer headerBuffer;
  private PinotDataBuffer dataBuffer;

  private FixedByteSingleValueMultiColWriter headerWriter;
  private FixedBitSingleValueMultiColWriter dataWriter;
  private FixedByteSingleValueMultiColReader headerReader;

  public FixedBitSingleColumnMultiValueWriter(File file, int numDocs,
      int totalNumValues, int columnSizeInBits) throws Exception {
    // there will be two sections header and data
    // header will contain N lines, each line corresponding to the
    int headerSize = numDocs * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    int dataSize = (totalNumValues * columnSizeInBits + 7) / 8;
    int totalSize = headerSize + dataSize;
    this.indexDataBuffer = PinotDataBuffer.fromFile(file, 0, totalSize, ReadMode.mmap, FileChannel.MapMode.READ_WRITE,
        file.getAbsolutePath() + this.getClass().getCanonicalName());
    headerBuffer = indexDataBuffer.view(0, headerSize);
    dataBuffer = indexDataBuffer.view(headerSize, totalSize);
    headerWriter = new FixedByteSingleValueMultiColWriter(headerBuffer,
        numDocs, 2, new int[] { SIZE_OF_INT, SIZE_OF_INT });
    headerReader = new FixedByteSingleValueMultiColReader(headerBuffer,
        numDocs, new int[] { SIZE_OF_INT, SIZE_OF_INT });

    dataWriter = new FixedBitSingleValueMultiColWriter(dataBuffer,
        totalNumValues, 1, new int[] { columnSizeInBits });
  }

  @Override
  public void close() throws IOException {
    dataWriter.close();
    headerReader.close();
    headerWriter.close();
    indexDataBuffer.close();

    indexDataBuffer = null;
    dataBuffer = null;
    headerBuffer = null;
    headerWriter = null;
    dataWriter = null;
  }

  private int updateHeader(int row, int length) {
    int prevRowStartIndex = 0;
    int prevRowLength = 0;
    if (row > 0) {
      prevRowStartIndex = headerReader.getInt(row - 1, 0);
      prevRowLength = headerReader.getInt(row - 1, 1);
    }
    int newStartIndex = prevRowStartIndex + prevRowLength;
    headerWriter.setInt(row, 0, newStartIndex);
    headerWriter.setInt(row, 1, length);
    return newStartIndex;
  }

  @Override
  public void setCharArray(int row, char[] charArray) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");

  }

  @Override
  public void setShortArray(int row, short[] shortsArray) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");

  }

  @Override
  public void setIntArray(int row, int[] intArray) {
    int newStartIndex = updateHeader(row, intArray.length);
    for (int i = 0; i < intArray.length; i++) {
      dataWriter.setInt(newStartIndex + i, 0, intArray[i]);
    }
  }

  @Override
  public void setLongArray(int row, long[] longArray) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");

  }

  @Override
  public void setFloatArray(int row, float[] floatArray) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");

  }

  @Override
  public void setDoubleArray(int row, double[] doubleArray) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");

  }

  @Override
  public void setStringArray(int row, String[] stringArray) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");

  }

  @Override
  public void setBytesArray(int row, byte[][] bytesArray) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");
  }

}
