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
package com.linkedin.pinot.core.io.writer.impl.v1;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.io.writer.SingleColumnMultiValueWriter;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;


public class FixedByteMultiValueWriter implements
    SingleColumnMultiValueWriter {
  private static int SIZE_OF_INT = 4;
  private static int NUM_COLS_IN_HEADER = 2;

  private PinotDataBuffer indexDataBuffer;
  private PinotDataBuffer headerBuffer;
  private PinotDataBuffer dataBuffer;
  private FixedByteSingleValueMultiColWriter headerWriter;
  private FixedByteSingleValueMultiColWriter dataWriter;
  private FixedByteSingleValueMultiColReader headerReader;

  public FixedByteMultiValueWriter(File file, int numDocs,
      int totalNumValues, int columnSizeInBytes) throws Exception {
    // there will be two sections header and data
    // header will contain N lines, each line corresponding to the
    int headerSize = numDocs * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    int dataSize = totalNumValues * columnSizeInBytes;
    int totalSize = headerSize + dataSize;
    indexDataBuffer = PinotDataBuffer.fromFile(file, 0, totalSize, ReadMode.mmap, FileChannel.MapMode.READ_WRITE,
        file.getAbsolutePath() + "indexWriter");

    headerBuffer = indexDataBuffer.view(0, headerSize);
    dataBuffer = indexDataBuffer.view(headerSize, totalSize);
    headerWriter = new FixedByteSingleValueMultiColWriter(headerBuffer,
        numDocs, 2, new int[] { SIZE_OF_INT, SIZE_OF_INT });
    headerReader = new FixedByteSingleValueMultiColReader(headerBuffer,
        numDocs, new int[] { SIZE_OF_INT, SIZE_OF_INT });

    dataWriter = new FixedByteSingleValueMultiColWriter(dataBuffer,
        totalNumValues, 1, new int[] { columnSizeInBytes });
  }

  @Override
  public void close() throws IOException {
    dataWriter.close();
    headerWriter.close();
    headerReader.close();
    indexDataBuffer.close();

    indexDataBuffer = null;
    dataBuffer = null;
    headerBuffer = null;
    dataWriter = null;
    headerWriter = null;
    headerReader = null;
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
    int newStartIndex = updateHeader(row, charArray.length);
    for (int i = 0; i < charArray.length; i++) {
      dataWriter.setChar(newStartIndex + i, 0, charArray[i]);
    }
  }

  @Override
  public void setShortArray(int row, short[] shortsArray) {
    int newStartIndex = updateHeader(row, shortsArray.length);
    for (int i = 0; i < shortsArray.length; i++) {
      dataWriter.setShort(newStartIndex + i, 0, shortsArray[i]);
    }
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
    int newStartIndex = updateHeader(row, longArray.length);
    for (int i = 0; i < longArray.length; i++) {
      dataWriter.setLong(newStartIndex + i, 0, longArray[i]);
    }
  }

  @Override
  public void setFloatArray(int row, float[] floatArray) {
    int newStartIndex = updateHeader(row, floatArray.length);
    for (int i = 0; i < floatArray.length; i++) {
      dataWriter.setFloat(newStartIndex + i, 0, floatArray[i]);
    }
  }

  @Override
  public void setDoubleArray(int row, double[] doubleArray) {
    int newStartIndex = updateHeader(row, doubleArray.length);
    for (int i = 0; i < doubleArray.length; i++) {
      dataWriter.setDouble(newStartIndex + i, 0, doubleArray[i]);
    }
  }

  @Override
  public void setStringArray(int row, String[] stringArray) {
    int newStartIndex = updateHeader(row, stringArray.length);
    for (int i = 0; i < stringArray.length; i++) {
      dataWriter.setString(newStartIndex + i, 0, stringArray[i]);
    }
  }

  @Override
  public void setBytesArray(int row, byte[][] bytesArray) {
    int newStartIndex = updateHeader(row, bytesArray.length);
    for (int i = 0; i < bytesArray.length; i++) {
      dataWriter.setBytes(newStartIndex + i, 0, bytesArray[i]);
    }
  }

}
