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
import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthRowColDataFileReader;
import com.linkedin.pinot.core.index.writer.SingleColumnMultiValueWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.commons.io.IOUtils;


public class FixedByteWidthSingleColumnMultiValueWriter implements
    SingleColumnMultiValueWriter {
  private static int SIZE_OF_INT = 4;
  private static int NUM_COLS_IN_HEADER = 2;
  private ByteBuffer headerBuffer;
  private ByteBuffer dataBuffer;
  private RandomAccessFile raf;
  private FixedByteWidthRowColDataFileWriter headerWriter;
  private FixedByteWidthRowColDataFileWriter dataWriter;
  private FixedByteWidthRowColDataFileReader headerReader;

  public FixedByteWidthSingleColumnMultiValueWriter(File file, int numDocs,
      int totalNumValues, int columnSizeInBytes) throws Exception {
    // there will be two sections header and data
    // header will contain N lines, each line corresponding to the
    int headerSize = numDocs * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    int dataSize = totalNumValues * columnSizeInBytes;
    int totalSize = headerSize + dataSize;
    raf = new RandomAccessFile(file, "rw");
    headerBuffer = MmapUtils.mmapFile(raf, FileChannel.MapMode.READ_WRITE, 0,
        headerSize, file, this.getClass().getSimpleName() + " headerBuffer");
    dataBuffer = MmapUtils.mmapFile(raf, FileChannel.MapMode.READ_WRITE,
        headerSize, dataSize, file, this.getClass().getSimpleName() + " dataBuffer");
    headerWriter = new FixedByteWidthRowColDataFileWriter(headerBuffer,
        numDocs, 2, new int[] { SIZE_OF_INT, SIZE_OF_INT });
    headerReader = new FixedByteWidthRowColDataFileReader(headerBuffer,
        numDocs, 2, new int[] { SIZE_OF_INT, SIZE_OF_INT });

    dataWriter = new FixedByteWidthRowColDataFileWriter(dataBuffer,
        totalNumValues, 1, new int[] { columnSizeInBytes });

  }

  @Override
  public boolean setMetadata(DataFileMetadata metadata) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(raf);
    raf = null;
    MmapUtils.unloadByteBuffer(dataBuffer);
    dataBuffer = null;
    MmapUtils.unloadByteBuffer(headerBuffer);
    headerBuffer = null;
    dataWriter.close();
    dataWriter = null;
    headerWriter.close();
    headerWriter = null;
    headerReader.close();
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
