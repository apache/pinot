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

import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.io.readerwriter.BaseSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides expandable off-heap implementation to store a multi-valued column across a number of rows.
 * The maximum number of values in any row must be known while invoking the constructor. Other than that, this class
 * allocates additional memory as needed to accommodate any number of rows.
 *
 * Writes into the data structure are strictly sequential, but reads can be random.
 *
 * Writes are of type:
 *
 *   setIntArray(int rowNumber, int[] values)
 *
 * It is expected that rowNumber starts with 0 and increments by 1 on each invocation, and that it is decided ahead of
 * time that the class is used to store a certain type of data structure (int arrays, or char arrays, etc.) Mix & match
 * is not allowed.
 *
 * Two kinds of data structures are used in this class.
 *
 * 1. A header (essentially an index into the other data structure) that has one entry per row. The entry has 3 integers
 *   - data buffer ID
 *   - offset in the data buffer where column values start
 *   - length (number of values in the multi-valued column).
 *
 *   New header structures are added as new rows come in. Each header class holds the same number of rows (for easy lookup)
 *
 * 2. A data buffer that has the values for the column that the header points to. Data buffers are added as needed,
 *    whenever we reach a limitation that we cannot fit the values of a column in the current buffer.
 *
 * Note that data buffers and headers grow independently.
 *
 * Data format
 * <code>
 *  HEADER SECTION 0
 *    bufferId startIndex length
 *    bufferId startIndex length
 *    bufferId startIndex length
 *    ...
 *  HEADER SECTION 1
 *    bufferId startIndex length
 *    bufferId startIndex length
 *    bufferId startIndex length
 *    ...
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

  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSingleColumnMultiValueReaderWriter.class);
  /**
   * number of columns is 1, column size is variable but less than _maxNumberOfMultiValuesPerRow
   */

  private static final int SIZE_OF_INT = 4;
  private static final int NUM_COLS_IN_HEADER = 3;

  private static final int INCREMENT_PERCENTAGE = 100;
      //Increments the Initial size by 100% of initial capacity every time we runs out of capacity

  private PinotDataBuffer _headerBuffer;
  private List<PinotDataBuffer> _dataBuffers = new ArrayList<>();
  private List<PinotDataBuffer> _headerBuffers = new ArrayList<>();
  private List<FixedByteSingleValueMultiColReader> _headerReaders = new ArrayList<>();
  private List<FixedByteSingleValueMultiColWriter> _headerWriters = new ArrayList<>();
  private FixedByteSingleValueMultiColWriter _curHeaderWriter;
  private List<FixedByteSingleValueMultiColWriter> _dataWriters = new ArrayList<>();
  private List<FixedByteSingleValueMultiColReader> _dataReaders = new ArrayList<>();
  private FixedByteSingleValueMultiColWriter _currentDataWriter;
  private int _currentDataWriterIndex = -1;
  private int _currentCapacity = 0;
  private int _headerSize;
  private int _incrementalCapacity;
  private int _columnSizeInBytes;
  private int _maxNumberOfMultiValuesPerRow;
  private final int _rowCountPerChunk;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _context;
  private int _prevRowStartIndex = 0;  // Offset in the data-buffer for the last row added.
  private int _prevRowLength = 0;  // Number of values in the column for the last row added.

  public FixedByteSingleColumnMultiValueReaderWriter(int maxNumberOfMultiValuesPerRow, int avgMultiValueCount,
      int rowCountPerChunk, int columnSizeInBytes, PinotDataBufferMemoryManager memoryManager, String context) {
    _memoryManager = memoryManager;
    _context = context;
    int initialCapacity = Math.max(maxNumberOfMultiValuesPerRow, rowCountPerChunk * avgMultiValueCount);
    int incrementalCapacity =
        Math.max(maxNumberOfMultiValuesPerRow, (int) (initialCapacity * 1.0f * INCREMENT_PERCENTAGE / 100));
    _columnSizeInBytes = columnSizeInBytes;
    _maxNumberOfMultiValuesPerRow = maxNumberOfMultiValuesPerRow;
    _headerSize = rowCountPerChunk * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    _rowCountPerChunk = rowCountPerChunk;
    addHeaderBuffers();
    //at least create space for million entries, which for INT translates into 4mb buffer
    _incrementalCapacity = incrementalCapacity;
    addDataBuffers(initialCapacity);
    //init(_rowCountPerChunk, _columnSizeInBytes, _maxNumberOfMultiValuesPerRow, initialCapacity, _incrementalCapacity);
  }

  private void addHeaderBuffers() {
    LOGGER.info("Allocating header buffer of size {} for: {}", _headerSize, _context);
    _headerBuffer = _memoryManager.allocate(_headerSize, _context);
    // We know that these buffers will not be copied directly into a file (or mapped from a file).
    // So, we can use native byte order here.
    _headerBuffer.order(ByteOrder.nativeOrder());
    // dataBufferId, startIndex, length
    _curHeaderWriter = new FixedByteSingleValueMultiColWriter(_headerBuffer, _rowCountPerChunk, 3,
        new int[]{SIZE_OF_INT, SIZE_OF_INT, SIZE_OF_INT});
    FixedByteSingleValueMultiColReader curHeaderReader =
        new FixedByteSingleValueMultiColReader(_headerBuffer, _rowCountPerChunk,
            new int[]{SIZE_OF_INT, SIZE_OF_INT, SIZE_OF_INT});
    _headerBuffers.add(_headerBuffer);
    _headerWriters.add(_curHeaderWriter);
    _headerReaders.add(curHeaderReader);
  }

  /**
   * This method automatically computes the space needed based on the _columnSizeInBytes
   * @param rowCapacity Additional capacity to be added in terms of number of rows
   * @throws RuntimeException
   */
  private void addDataBuffers(int rowCapacity)
      throws RuntimeException {
    PinotDataBuffer dataBuffer;
    try {
      final long size = rowCapacity * _columnSizeInBytes;
      LOGGER.info("Allocating data buffer of size {} for column {}", size, _context);
      dataBuffer = _memoryManager.allocate(size, _context);
      dataBuffer.order(ByteOrder.nativeOrder());
      _dataBuffers.add(dataBuffer);
      _currentDataWriter =
          new FixedByteSingleValueMultiColWriter(dataBuffer, rowCapacity, 1, new int[]{_columnSizeInBytes});
      _dataWriters.add(_currentDataWriter);

      FixedByteSingleValueMultiColReader dataFileReader =
          new FixedByteSingleValueMultiColReader(dataBuffer, rowCapacity, new int[]{_columnSizeInBytes});
      _dataReaders.add(dataFileReader);
      //update the capacity
      _currentCapacity = rowCapacity;
      _currentDataWriterIndex = _currentDataWriterIndex + 1;
    } catch (Exception e) {
      throw new RuntimeException(
          "Error while expanding the capacity by allocating additional buffer with capacity:" + rowCapacity, e);
    }
  }

  @Override
  public void close() {
    for (PinotDataBuffer dataBuffer : _dataBuffers) {
      dataBuffer.close();
    }
    _dataBuffers.clear();
    for (PinotDataBuffer headerBuffer : _headerBuffers) {
      headerBuffer.close();
    }
    _headerBuffers.clear();
    _headerBuffer = null;
    for (FixedByteSingleValueMultiColReader reader : _headerReaders) {
      reader.close();
    }
    for (FixedByteSingleValueMultiColReader reader : _dataReaders) {
      reader.close();
    }
    for (FixedByteSingleValueMultiColWriter writer : _headerWriters) {
      writer.close();
    }
    for (FixedByteSingleValueMultiColWriter writer : _dataWriters) {
      writer.close();
    }
  }

  private void writeIntoHeader(int row, int dataWriterIndex, int startIndex, int length) {
    if (row >= _headerBuffers.size() * _rowCountPerChunk) {
      addHeaderBuffers();
    }
    _curHeaderWriter.setInt(getRowInCurrentHeader(row), 0, dataWriterIndex);
    _curHeaderWriter.setInt(getRowInCurrentHeader(row), 1, startIndex);
    _curHeaderWriter.setInt(getRowInCurrentHeader(row), 2, length);
  }

  // TODO Use powers of two for _rowCountPerChunk to optimize computation for the
  // methods below. Or, assert that the input values to the class are powers of two. TBD.
  private FixedByteSingleValueMultiColReader getCurrentReader(int row) {
    return _headerReaders.get(row / _rowCountPerChunk);
  }

  private int getRowInCurrentHeader(int row) {
    return row % _rowCountPerChunk;
  }

  private int updateHeader(int row, int numValues) {
    assert (numValues <= _maxNumberOfMultiValuesPerRow);
    int newStartIndex = _prevRowStartIndex + _prevRowLength;
    if (newStartIndex + numValues > _currentCapacity) {
      addDataBuffers(_incrementalCapacity);
      _prevRowStartIndex = 0;
      _prevRowLength = 0;
      newStartIndex = 0;
    }
    writeIntoHeader(row, _currentDataWriterIndex, newStartIndex, numValues);
    _prevRowStartIndex = newStartIndex;
    _prevRowLength = numValues;
    return newStartIndex;
  }

  @Override
  public void setCharArray(int row, char[] charArray) {
    int newStartIndex = updateHeader(row, charArray.length);
    for (int i = 0; i < charArray.length; i++) {
      _currentDataWriter.setChar(newStartIndex + i, 0, charArray[i]);
    }
  }

  @Override
  public void setShortArray(int row, short[] shortsArray) {

    int newStartIndex = updateHeader(row, shortsArray.length);
    for (int i = 0; i < shortsArray.length; i++) {
      _currentDataWriter.setShort(newStartIndex + i, 0, shortsArray[i]);
    }
  }

  @Override
  public void setIntArray(int row, int[] intArray) {
    int newStartIndex = updateHeader(row, intArray.length);
    for (int i = 0; i < intArray.length; i++) {
      _currentDataWriter.setInt(newStartIndex + i, 0, intArray[i]);
    }
  }

  @Override
  public void setLongArray(int row, long[] longArray) {
    int newStartIndex = updateHeader(row, longArray.length);
    for (int i = 0; i < longArray.length; i++) {
      _currentDataWriter.setLong(newStartIndex + i, 0, longArray[i]);
    }
  }

  @Override
  public void setFloatArray(int row, float[] floatArray) {
    int newStartIndex = updateHeader(row, floatArray.length);
    for (int i = 0; i < floatArray.length; i++) {
      _currentDataWriter.setFloat(newStartIndex + i, 0, floatArray[i]);
    }
  }

  @Override
  public void setDoubleArray(int row, double[] doubleArray) {
    int newStartIndex = updateHeader(row, doubleArray.length);
    for (int i = 0; i < doubleArray.length; i++) {
      _currentDataWriter.setDouble(newStartIndex + i, 0, doubleArray[i]);
    }
  }

  @Override
  public void setStringArray(int row, String[] stringArray) {
    int newStartIndex = updateHeader(row, stringArray.length);
    for (int i = 0; i < stringArray.length; i++) {
      _currentDataWriter.setString(newStartIndex + i, 0, stringArray[i]);
    }
  }

  @Override
  public void setBytesArray(int row, byte[][] bytesArray) {
    int newStartIndex = updateHeader(row, bytesArray.length);
    for (int i = 0; i < bytesArray.length; i++) {
      _currentDataWriter.setBytes(newStartIndex + i, 0, bytesArray[i]);
    }
  }

  @Override
  public int getCharArray(int row, char[] charArray) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(row);
    int rowInCurrentHeader = getRowInCurrentHeader(row);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      charArray[i] = dataReader.getChar(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getShortArray(int row, short[] shortsArray) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(row);
    int rowInCurrentHeader = getRowInCurrentHeader(row);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      shortsArray[i] = dataReader.getShort(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getIntArray(int row, int[] intArray) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(row);
    int rowInCurrentHeader = getRowInCurrentHeader(row);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      intArray[i] = dataReader.getInt(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getLongArray(int row, long[] longArray) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(row);
    int rowInCurrentHeader = getRowInCurrentHeader(row);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      longArray[i] = dataReader.getLong(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getFloatArray(int row, float[] floatArray) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(row);
    int rowInCurrentHeader = getRowInCurrentHeader(row);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      floatArray[i] = dataReader.getFloat(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getDoubleArray(int row, double[] doubleArray) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(row);
    int rowInCurrentHeader = getRowInCurrentHeader(row);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      doubleArray[i] = dataReader.getDouble(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getStringArray(int row, String[] stringArray) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(row);
    int rowInCurrentHeader = getRowInCurrentHeader(row);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      stringArray[i] = dataReader.getString(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int getBytesArray(int row, byte[][] bytesArray) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(row);
    int rowInCurrentHeader = getRowInCurrentHeader(row);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      bytesArray[i] = dataReader.getBytes(startIndex + i, 0);
    }
    return length;
  }
}
