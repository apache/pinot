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
package com.linkedin.pinot.core.io.readerwriter.impl;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.io.readerwriter.BaseSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


/**
 * Supports both reads and writes using the same data structure.<br>
 * Writes must be strictly sequential, while reads can be random <br>
 * It is very similar to the SingleColumnMultiValue format representation <br>
 * except that the variable size data buffer size is not known up front in case FixedByteSingleColumnMultiValueReaderWriter
 * This class allocates extra memory in chunks as needed.
 * Data format
 * <code>
 *  HEADER SECTION
 *    bufferId startIndex EndIndex
 *    bufferId startIndex EndIndex
 *    bufferId startIndex EndIndex
 *  Data BUFFER SECTION 0
 *    [set of values of row 0] [set of values of row 1]
 *     .....
 *     [set of values of row m]
 *  Data BUFFER SECTION 1
 *     [set of values of row m +1 ] [set of values of row M +2]
 *     .....
 *     [set of values of row ]
 *  Data BUFFER SECTION N
 *     [set of values of row ... ] [set of values of row ...]
 *     .....
 *     [set of values of row n]
 * </code>
 *
 */

public class FixedByteSingleColumnMultiValueReaderWriter extends BaseSingleColumnMultiValueReaderWriter {

  public static final int DEFAULT_MAX_NUMBER_OF_MULTIVALUES = 1000;
  /**
   * number of columns is 1, column size is variable but less than maxNumberOfMultiValuesPerRow
   * @param rows
   */

  private static final int SIZE_OF_INT = 4;
  private static final int NUM_COLS_IN_HEADER = 3;

  private static final int INCREMENT_PERCENTAGE = 100;//Increments the Initial size by 100% of initial capacity every time we runs out of capacity

  private PinotDataBuffer headerBuffer;
  private List<PinotDataBuffer> dataBuffers = new ArrayList<>();
  private FixedByteSingleValueMultiColWriter headerWriter;
  private FixedByteSingleValueMultiColReader headerReader;
  private List<FixedByteSingleValueMultiColWriter> dataWriters = new ArrayList<FixedByteSingleValueMultiColWriter>();
  private List<FixedByteSingleValueMultiColReader> dataReaders = new ArrayList<FixedByteSingleValueMultiColReader>();
  private FixedByteSingleValueMultiColWriter currentDataWriter;
  private int currentDataWriterIndex = -1;
  private int currentCapacity = 0;
  private int headerSize;
  private int incrementalCapacity;
  private int columnSizeInBytes;
  private int maxNumberOfMultiValuesPerRow;

  public FixedByteSingleColumnMultiValueReaderWriter(int rows, int columnSizeInBytes, int maxNumberOfMultiValuesPerRow,
      int avgMultiValueCount)
      throws IOException {
    int initialCapacity = Math.max(maxNumberOfMultiValuesPerRow, rows * avgMultiValueCount);
    int incrementalCapacity =
        Math.max(maxNumberOfMultiValuesPerRow, (int) (initialCapacity * 1.0f * INCREMENT_PERCENTAGE / 100));
    init(rows, columnSizeInBytes, maxNumberOfMultiValuesPerRow, initialCapacity, incrementalCapacity);
  }

  private void init(int rows, int columnSizeInBytes, int maxNumberOfMultiValuesPerRow, int initialCapacity,
      int incrementalCapacity) throws IOException {
    this.columnSizeInBytes = columnSizeInBytes;
    this.maxNumberOfMultiValuesPerRow = maxNumberOfMultiValuesPerRow;
    headerSize = rows * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    headerBuffer = PinotDataBuffer.allocateDirect(headerSize);
    // We know that these bufffers will not be copied directly into a file (or mapped from a file).
    // So, we can use native byte order here.
    headerBuffer.order(ByteOrder.nativeOrder());
    //dataBufferId, startIndex, length
    headerWriter =
        new FixedByteSingleValueMultiColWriter(headerBuffer, rows, 3,
            new int[] { SIZE_OF_INT, SIZE_OF_INT, SIZE_OF_INT });
    headerReader =
        new FixedByteSingleValueMultiColReader(headerBuffer, rows, new int[] { SIZE_OF_INT, SIZE_OF_INT, SIZE_OF_INT });
    //at least create space for million entries, which for INT translates into 4mb buffer
    this.incrementalCapacity = incrementalCapacity;
    addCapacity(initialCapacity);
  }

  /**
   * This method automatically computes the space needed based on the columnSizeInBytes
   * @param rowCapacity Additional capacity to be added in terms of number of rows
   * @throws RuntimeException
   */
  private void addCapacity(int rowCapacity) throws RuntimeException {
    PinotDataBuffer dataBuffer;
    try {
      dataBuffer = PinotDataBuffer.allocateDirect(rowCapacity * columnSizeInBytes);
      //dataBuffer.order(ByteOrder.nativeOrder());
      dataBuffers.add(dataBuffer);
      currentDataWriter =
          new FixedByteSingleValueMultiColWriter(dataBuffer, rowCapacity, 1, new int[] { columnSizeInBytes });
      dataWriters.add(currentDataWriter);

      FixedByteSingleValueMultiColReader dataFileReader =
          new FixedByteSingleValueMultiColReader(dataBuffer, rowCapacity, new int[] { columnSizeInBytes });
      dataReaders.add(dataFileReader);
      //update the capacity
      currentCapacity = rowCapacity;
      currentDataWriterIndex = currentDataWriterIndex + 1;
    } catch (Exception e) {
      throw new RuntimeException("Error while expanding the capacity by allocating additional buffer with capacity:"
          + rowCapacity, e);
    }
  }

  @Override
  public void close() {
    for (PinotDataBuffer dataBuffer : dataBuffers) {
      dataBuffer.close();
    }
    dataBuffers.clear();
    headerBuffer.close();
    headerBuffer = null;
  }

  private int updateHeader(int row, int length) {
    assert (length < maxNumberOfMultiValuesPerRow);
    int prevRowStartIndex = 0;
    int prevRowLength = 0;
    if (row > 0) {
      prevRowStartIndex = headerReader.getInt(row - 1, 1);
      prevRowLength = headerReader.getInt(row - 1, 2);
    }
    int newStartIndex = prevRowStartIndex + prevRowLength;
    if (newStartIndex + length > currentCapacity) {
      addCapacity(incrementalCapacity);
      prevRowStartIndex = 0;
      prevRowLength = 0;
      newStartIndex = prevRowStartIndex + prevRowLength;
    }
    headerWriter.setInt(row, 0, currentDataWriterIndex);
    headerWriter.setInt(row, 1, newStartIndex);
    headerWriter.setInt(row, 2, length);
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

  @Override
  public int getCharArray(int row, char[] charArray) {
    int bufferIndex = headerReader.getInt(row, 1);
    int startIndex = headerReader.getInt(row, 1);
    int length = headerReader.getInt(row, 2);
    FixedByteSingleValueMultiColReader dataReader = dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      charArray[i] = dataReader.getChar(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getShortArray(int row, short[] shortsArray) {
    int bufferIndex = headerReader.getInt(row, 1);
    int startIndex = headerReader.getInt(row, 1);
    int length = headerReader.getInt(row, 2);
    FixedByteSingleValueMultiColReader dataReader = dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      shortsArray[i] = dataReader.getShort(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    int bufferIndex = headerReader.getInt(row, 0);
    int startIndex = headerReader.getInt(row, 1);
    int length = headerReader.getInt(row, 2);
    FixedByteSingleValueMultiColReader dataReader = dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      intArray[i] = dataReader.getInt(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getLongArray(int row, long[] longArray) {
    int bufferIndex = headerReader.getInt(row, 0);
    int startIndex = headerReader.getInt(row, 1);
    int length = headerReader.getInt(row, 2);
    FixedByteSingleValueMultiColReader dataReader = dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      longArray[i] = dataReader.getLong(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getFloatArray(int row, float[] floatArray) {
    int bufferIndex = headerReader.getInt(row, 1);
    int startIndex = headerReader.getInt(row, 1);
    int length = headerReader.getInt(row, 2);
    FixedByteSingleValueMultiColReader dataReader = dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      floatArray[i] = dataReader.getFloat(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getDoubleArray(int row, double[] doubleArray) {
    int bufferIndex = headerReader.getInt(row, 1);
    int startIndex = headerReader.getInt(row, 1);
    int length = headerReader.getInt(row, 2);
    FixedByteSingleValueMultiColReader dataReader = dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      doubleArray[i] = dataReader.getDouble(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getStringArray(int row, String[] stringArray) {
    int bufferIndex = headerReader.getInt(row, 1);
    int startIndex = headerReader.getInt(row, 1);
    int length = headerReader.getInt(row, 2);
    FixedByteSingleValueMultiColReader dataReader = dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      stringArray[i] = dataReader.getString(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getBytesArray(int row, byte[][] bytesArray) {
    int bufferIndex = headerReader.getInt(row, 1);
    int startIndex = headerReader.getInt(row, 1);
    int length = headerReader.getInt(row, 2);
    FixedByteSingleValueMultiColReader dataReader = dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      bytesArray[i] = dataReader.getBytes(startIndex + i, 0);
    }
    return length;
  }

}
