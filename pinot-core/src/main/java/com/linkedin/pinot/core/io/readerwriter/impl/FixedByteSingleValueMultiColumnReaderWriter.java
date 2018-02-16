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
import com.linkedin.pinot.core.io.readerwriter.BaseSingleValueMultiColumnReaderWriter;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;


/**
 * Extension of {@link BaseSingleValueMultiColumnReaderWriter} that fully implements a
 * SingleValue MultiColumn reader and writer for fixed size columns.
 *
 * This implementation currently does not support reading with context, but can be enhanced to do so.
 */
public class FixedByteSingleValueMultiColumnReaderWriter extends BaseSingleValueMultiColumnReaderWriter {

  private List<FixedByteSingleValueMultiColWriter> _writers;
  private volatile List<FixedByteSingleValueMultiColReader> _readers;
  private final int _numRowsPerChunk;

  private final int[] _columnSizesInBytes;
  private final PinotDataBufferMemoryManager _memoryManager;
  private String _allocationContext;
  private final long _chunkSizeInBytes;
  private int _capacityInRows;
  private List<PinotDataBuffer> _dataBuffers;
  private int _numColumns;

  /**
   * Constructor for the class.
   *
   * @param numRowsPerChunk Number of rows per chunk of data buffer.
   * @param columnSizesInBytes Int array containing size of columns in bytes.
   * @param memoryManager Memory manager to be used for allocating memory.
   * @param allocationContext Allocation context (for debugging).
   */
  public FixedByteSingleValueMultiColumnReaderWriter(int numRowsPerChunk, int[] columnSizesInBytes,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {

    _numRowsPerChunk = numRowsPerChunk;
    _columnSizesInBytes = columnSizesInBytes;
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;

    _writers = new ArrayList<>();
    _readers = new ArrayList<>();
    _dataBuffers = new ArrayList<>();

    _capacityInRows = 0;
    int rowSizeInBytes = 0;
    for (int columnSizesInByte : columnSizesInBytes) {
      rowSizeInBytes += columnSizesInByte;
    }

    _numColumns = _columnSizesInBytes.length;
    _chunkSizeInBytes = rowSizeInBytes * numRowsPerChunk;
  }

  @Override
  public int getInt(int row, int column) {
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;

    return _readers.get(chunkId).getInt(rowInChunk, column);
  }

  @Override
  public long getLong(int row, int column) {
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;

    return _readers.get(chunkId).getLong(rowInChunk, column);
  }

  @Override
  public float getFloat(int row, int column) {
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;

    return _readers.get(chunkId).getFloat(rowInChunk, column);
  }

  @Override
  public double getDouble(int row, int column) {
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;

    return _readers.get(chunkId).getDouble(rowInChunk, column);
  }

  @Override
  public String getString(int row, int column) {
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;

    return _readers.get(chunkId).getString(rowInChunk, column);
  }

  @Override
  public void setInt(int row, int column, int value) {
    ensureCapacity(row);
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;
    _writers.get(chunkId).setInt(rowInChunk, column, value);
  }

  @Override
  public void setLong(int row, int column, long value) {
    ensureCapacity(row);
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;
    _writers.get(chunkId).setLong(rowInChunk, column, value);
  }

  @Override
  public void setFloat(int row, int column, float value) {
    ensureCapacity(row);
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;
    _writers.get(chunkId).setFloat(rowInChunk, column, value);
  }

  @Override
  public void setDouble(int row, int column, double value) {
    ensureCapacity(row);
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;
    _writers.get(chunkId).setDouble(rowInChunk, column, value);
  }

  @Override
  public void setString(int row, int column, String value) {
    ensureCapacity(row);
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;
    _writers.get(chunkId).setString(rowInChunk, column, value);
  }

  @Override
  public void close()
      throws IOException {
    _capacityInRows = 0;
    _writers.clear();
    _readers.clear();

    for (PinotDataBuffer dataBuffer : _dataBuffers) {
      dataBuffer.close();
    }
  }

  /**
   * Helper method to ensure capacity so that a row at specified location is available
   * for write.
   *
   * @param row row id to be written at.
   */
  private void ensureCapacity(int row) {
    if (row >= _capacityInRows) {

      // Adding _chunkSizeInBytes in the numerator for rounding up. +1 because rows are 0-based index.
      long buffersNeeded = (row + 1 - _capacityInRows + _numRowsPerChunk) / _numRowsPerChunk;
      for (int i = 0; i < buffersNeeded; i++) {
        addBuffer();
      }
    }
  }

  /**
   * Helper method to add data buffer during expansion.
   */
  private void addBuffer() {
    PinotDataBuffer buffer = _memoryManager.allocate(_chunkSizeInBytes, _allocationContext);
    _capacityInRows += _numRowsPerChunk;

    buffer.order(ByteOrder.nativeOrder());
    _dataBuffers.add(buffer);

    FixedByteSingleValueMultiColReader reader =
        new FixedByteSingleValueMultiColReader(buffer, _numRowsPerChunk, _columnSizesInBytes);

    FixedByteSingleValueMultiColWriter writer =
        new FixedByteSingleValueMultiColWriter(buffer, _numRowsPerChunk, _numColumns, _columnSizesInBytes);

    _writers.add(writer);

    // ArrayList is non-threadsafe. So add to a new copy and then change teh reference (_readers is volatile).
    List<FixedByteSingleValueMultiColReader> readers = new ArrayList<>(_readers);
    readers.add(reader);
    _readers = readers;
  }
}
