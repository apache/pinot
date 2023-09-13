/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.realtime.impl.forward;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.pinot.segment.local.io.reader.impl.FixedByteSingleValueMultiColReader;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteSingleValueMultiColWriter;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
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
 *   New header structures are added as new rows come in. Each header class holds the same number of rows (for easy
 *   lookup)
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
// TODO: Optimize it
public class FixedByteMVMutableForwardIndex implements MutableForwardIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteMVMutableForwardIndex.class);

  /**
   * number of columns is 1, column size is variable but less than _maxNumberOfMultiValuesPerRow
   */

  private static final int SIZE_OF_INT = 4;
  private static final int NUM_COLS_IN_HEADER = 3;

  private static final int INCREMENT_PERCENTAGE = 100;
  //Increments the Initial size by 100% of initial capacity every time we runs out of capacity

  // For single writer multiple readers setup, use ArrayList for writer and CopyOnWriteArrayList for reader
  private final List<FixedByteSingleValueMultiColWriter> _headerWriters = new ArrayList<>();
  private final List<FixedByteSingleValueMultiColReader> _headerReaders = new CopyOnWriteArrayList<>();
  private final List<FixedByteSingleValueMultiColWriter> _dataWriters = new ArrayList<>();
  private final List<FixedByteSingleValueMultiColReader> _dataReaders = new CopyOnWriteArrayList<>();
  private final int _headerSize;
  private final int _incrementalCapacity;
  private final int _columnSizeInBytes;
  private final int _maxNumberOfMultiValuesPerRow;
  private final int _rowCountPerChunk;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _context;
  private final boolean _isDictionaryEncoded;
  private final FieldSpec.DataType _storedType;

  private FixedByteSingleValueMultiColWriter _curHeaderWriter;
  private FixedByteSingleValueMultiColWriter _currentDataWriter;
  private int _currentCapacity = 0;
  private int _prevRowStartIndex = 0;  // Offset in the data-buffer for the last row added.
  private int _prevRowLength = 0;  // Number of values in the column for the last row added.

  public FixedByteMVMutableForwardIndex(int maxNumberOfMultiValuesPerRow, int avgMultiValueCount, int rowCountPerChunk,
      int columnSizeInBytes, PinotDataBufferMemoryManager memoryManager, String context, boolean isDictionaryEncoded,
      FieldSpec.DataType storedType) {
    _memoryManager = memoryManager;
    _context = context;
    int initialCapacity = Math.max(maxNumberOfMultiValuesPerRow, rowCountPerChunk * avgMultiValueCount);
    int incrementalCapacity =
        Math.max(maxNumberOfMultiValuesPerRow, (int) (initialCapacity * 1.0f * INCREMENT_PERCENTAGE / 100));
    _columnSizeInBytes = columnSizeInBytes;
    _maxNumberOfMultiValuesPerRow = maxNumberOfMultiValuesPerRow;
    _headerSize = rowCountPerChunk * SIZE_OF_INT * NUM_COLS_IN_HEADER;
    _rowCountPerChunk = rowCountPerChunk;
    addHeaderBuffer();
    //at least create space for million entries, which for INT translates into 4mb buffer
    _incrementalCapacity = incrementalCapacity;
    addDataBuffer(initialCapacity);
    //init(_rowCountPerChunk, _columnSizeInBytes, _maxNumberOfMultiValuesPerRow, initialCapacity, _incrementalCapacity);
    _isDictionaryEncoded = isDictionaryEncoded;
    _storedType = storedType;
  }

  private void addHeaderBuffer() {
    LOGGER.info("Allocating header buffer of size {} for: {}", _headerSize, _context);
    // NOTE: PinotDataBuffer is tracked in the PinotDataBufferMemoryManager. No need to track it inside the class.
    PinotDataBuffer headerBuffer = _memoryManager.allocate(_headerSize, _context);
    // dataBufferId, startIndex, length
    _curHeaderWriter =
        new FixedByteSingleValueMultiColWriter(headerBuffer, 3, new int[]{SIZE_OF_INT, SIZE_OF_INT, SIZE_OF_INT});
    _headerWriters.add(_curHeaderWriter);
    _headerReaders.add(new FixedByteSingleValueMultiColReader(headerBuffer, _rowCountPerChunk,
        new int[]{SIZE_OF_INT, SIZE_OF_INT, SIZE_OF_INT}));
  }

  /**
   * This method automatically computes the space needed based on the _columnSizeInBytes
   * @param rowCapacity Additional capacity to be added in terms of number of rows
   */
  private void addDataBuffer(int rowCapacity) {
    try {
      long size = (long) rowCapacity * (long) _columnSizeInBytes;
      LOGGER.info("Allocating data buffer of size {} for column {}", size, _context);
      // NOTE: PinotDataBuffer is tracked in PinotDataBufferMemoryManager. No need to track and close inside the class.
      PinotDataBuffer dataBuffer = _memoryManager.allocate(size, _context);
      _currentDataWriter = new FixedByteSingleValueMultiColWriter(dataBuffer, 1, new int[]{_columnSizeInBytes});
      _dataWriters.add(_currentDataWriter);
      _dataReaders.add(new FixedByteSingleValueMultiColReader(dataBuffer, rowCapacity, new int[]{_columnSizeInBytes}));
      //update the capacity
      _currentCapacity = rowCapacity;
    } catch (Exception e) {
      throw new RuntimeException(
          "Error while expanding the capacity by allocating additional buffer with capacity:" + rowCapacity, e);
    }
  }

  private void writeIntoHeader(int row, int dataWriterIndex, int startIndex, int length) {
    if (row >= _headerWriters.size() * _rowCountPerChunk) {
      addHeaderBuffer();
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
      addDataBuffer(_incrementalCapacity);
      _prevRowStartIndex = 0;
      _prevRowLength = 0;
      newStartIndex = 0;
    }
    writeIntoHeader(row, _dataWriters.size() - 1, newStartIndex, numValues);
    _prevRowStartIndex = newStartIndex;
    _prevRowLength = numValues;
    return newStartIndex;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return _isDictionaryEncoded;
  }

  @Override
  public boolean isSingleValue() {
    return false;
  }

  @Override
  public DataType getStoredType() {
    return _storedType;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _columnSizeInBytes;
  }

  @Override
  public int getLengthOfLongestElement() {
    return _columnSizeInBytes;
  }

  @Override
  public int getDictIdMV(int docId, int[] dictIdBuffer) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(docId);
    int rowInCurrentHeader = getRowInCurrentHeader(docId);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      dictIdBuffer[i] = dataReader.getInt(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public int[] getDictIdMV(int docId) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(docId);
    int rowInCurrentHeader = getRowInCurrentHeader(docId);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    int[] dictIdBuffer = new int[length];
    for (int i = 0; i < length; i++) {
      dictIdBuffer[i] = dataReader.getInt(startIndex + i, 0);
    }
    return dictIdBuffer;
  }

  @Override
  public int getIntMV(int docId, int[] valueBuffer) {
    return getDictIdMV(docId, valueBuffer);
  }

  @Override
  public int[] getIntMV(int docId) {
    return getDictIdMV(docId);
  }

  @Override
  public int getLongMV(int docId, long[] valueBuffer) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(docId);
    int rowInCurrentHeader = getRowInCurrentHeader(docId);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      valueBuffer[i] = dataReader.getLong(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public long[] getLongMV(int docId) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(docId);
    int rowInCurrentHeader = getRowInCurrentHeader(docId);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    long[] valueBuffer = new long[length];
    for (int i = 0; i < length; i++) {
      valueBuffer[i] = dataReader.getLong(startIndex + i, 0);
    }
    return valueBuffer;
  }

  @Override
  public int getFloatMV(int docId, float[] valueBuffer) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(docId);
    int rowInCurrentHeader = getRowInCurrentHeader(docId);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      valueBuffer[i] = dataReader.getFloat(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public float[] getFloatMV(int docId) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(docId);
    int rowInCurrentHeader = getRowInCurrentHeader(docId);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    float[] valueBuffer = new float[length];
    for (int i = 0; i < length; i++) {
      valueBuffer[i] = dataReader.getFloat(startIndex + i, 0);
    }
    return valueBuffer;
  }

  @Override
  public int getDoubleMV(int docId, double[] valueBuffer) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(docId);
    int rowInCurrentHeader = getRowInCurrentHeader(docId);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    for (int i = 0; i < length; i++) {
      valueBuffer[i] = dataReader.getDouble(startIndex + i, 0);
    }
    return length;
  }

  @Override
  public double[] getDoubleMV(int docId) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(docId);
    int rowInCurrentHeader = getRowInCurrentHeader(docId);
    int bufferIndex = headerReader.getInt(rowInCurrentHeader, 0);
    int startIndex = headerReader.getInt(rowInCurrentHeader, 1);
    int length = headerReader.getInt(rowInCurrentHeader, 2);
    FixedByteSingleValueMultiColReader dataReader = _dataReaders.get(bufferIndex);
    double[] valueBuffer = new double[length];
    for (int i = 0; i < length; i++) {
      valueBuffer[i] = dataReader.getDouble(startIndex + i, 0);
    }
    return valueBuffer;
  }

  @Override
  public int getNumValuesMV(int docId) {
    FixedByteSingleValueMultiColReader headerReader = getCurrentReader(docId);
    int rowInCurrentHeader = getRowInCurrentHeader(docId);
    return headerReader.getInt(rowInCurrentHeader, 2);
  }

  @Override
  public void setDictIdMV(int docId, int[] dictIds) {
    int newStartIndex = updateHeader(docId, dictIds.length);
    for (int i = 0; i < dictIds.length; i++) {
      _currentDataWriter.setInt(newStartIndex + i, 0, dictIds[i]);
    }
  }

  @Override
  public void setIntMV(int docId, int[] values) {
    setDictIdMV(docId, values);
  }

  @Override
  public void setLongMV(int docId, long[] values) {
    int newStartIndex = updateHeader(docId, values.length);
    for (int i = 0; i < values.length; i++) {
      _currentDataWriter.setLong(newStartIndex + i, 0, values[i]);
    }
  }

  @Override
  public void setFloatMV(int docId, float[] values) {
    int newStartIndex = updateHeader(docId, values.length);
    for (int i = 0; i < values.length; i++) {
      _currentDataWriter.setFloat(newStartIndex + i, 0, values[i]);
    }
  }

  @Override
  public void setDoubleMV(int docId, double[] values) {
    int newStartIndex = updateHeader(docId, values.length);
    for (int i = 0; i < values.length; i++) {
      _currentDataWriter.setDouble(newStartIndex + i, 0, values[i]);
    }
  }

  @Override
  public void close()
      throws IOException {
    for (FixedByteSingleValueMultiColWriter writer : _headerWriters) {
      writer.close();
    }
    for (FixedByteSingleValueMultiColReader reader : _headerReaders) {
      reader.close();
    }
    for (FixedByteSingleValueMultiColWriter writer : _dataWriters) {
      writer.close();
    }
    for (FixedByteSingleValueMultiColReader reader : _dataReaders) {
      reader.close();
    }
  }
}
