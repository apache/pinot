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
package org.apache.pinot.segment.local.io.readerwriter.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.io.reader.impl.FixedByteSingleValueMultiColReader;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteSingleValueMultiColWriter;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;


/**
 * SingleValue MultiColumn reader and writer for fixed size columns.
 *
 * This implementation currently does not support reading with context, but can be enhanced to do so.
 *
 * TODO: Clean this up
 */
public class FixedByteSingleValueMultiColumnReaderWriter implements Closeable {

  private final List<FixedByteSingleValueMultiColWriter> _writers;
  private volatile List<FixedByteSingleValueMultiColReader> _readers;
  private final int _numRowsPerChunk;

  private final int[] _columnSizesInBytes;
  private final PinotDataBufferMemoryManager _memoryManager;
  private String _allocationContext;
  private final long _chunkSizeInBytes;
  private int _capacityInRows;
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

    _capacityInRows = 0;
    int rowSizeInBytes = 0;
    for (int columnSizesInByte : columnSizesInBytes) {
      rowSizeInBytes += columnSizesInByte;
    }

    _numColumns = _columnSizesInBytes.length;
    _chunkSizeInBytes = rowSizeInBytes * numRowsPerChunk;
  }

  public int getInt(int row, int column) {
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;

    return _readers.get(chunkId).getInt(rowInChunk, column);
  }

  public long getLong(int row, int column) {
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;

    return _readers.get(chunkId).getLong(rowInChunk, column);
  }

  public float getFloat(int row, int column) {
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;

    return _readers.get(chunkId).getFloat(rowInChunk, column);
  }

  public double getDouble(int row, int column) {
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;

    return _readers.get(chunkId).getDouble(rowInChunk, column);
  }

  public String getString(int row, int column) {
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;

    return _readers.get(chunkId).getString(rowInChunk, column);
  }

  public void setInt(int row, int column, int value) {
    ensureCapacity(row);
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;
    _writers.get(chunkId).setInt(rowInChunk, column, value);
  }

  public void setLong(int row, int column, long value) {
    ensureCapacity(row);
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;
    _writers.get(chunkId).setLong(rowInChunk, column, value);
  }

  public void setFloat(int row, int column, float value) {
    ensureCapacity(row);
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;
    _writers.get(chunkId).setFloat(rowInChunk, column, value);
  }

  public void setDouble(int row, int column, double value) {
    ensureCapacity(row);
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;
    _writers.get(chunkId).setDouble(rowInChunk, column, value);
  }

  public void setString(int row, int column, String value) {
    ensureCapacity(row);
    int rowInChunk = row % _numRowsPerChunk;
    int chunkId = row / _numRowsPerChunk;
    _writers.get(chunkId).setString(rowInChunk, column, value);
  }

  @Override
  public void close() throws IOException {
    for (FixedByteSingleValueMultiColWriter writer : _writers) {
      writer.close();
    }
    for (FixedByteSingleValueMultiColReader reader : _readers) {
      reader.close();
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
    // NOTE: PinotDataBuffer is tracked in the PinotDataBufferMemoryManager. No need to track it inside the class.
    PinotDataBuffer buffer = _memoryManager.allocate(_chunkSizeInBytes, _allocationContext);
    _capacityInRows += _numRowsPerChunk;
    _writers.add(new FixedByteSingleValueMultiColWriter(buffer, _numColumns, _columnSizesInBytes));

    FixedByteSingleValueMultiColReader reader =
        new FixedByteSingleValueMultiColReader(buffer, _numRowsPerChunk, _columnSizesInBytes);
    // ArrayList is non-threadsafe. So add to a new copy and then change teh reference (_readers is volatile).
    List<FixedByteSingleValueMultiColReader> readers = new ArrayList<>(_readers);
    readers.add(reader);
    _readers = readers;
  }
}
