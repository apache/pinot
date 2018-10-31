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
import com.linkedin.pinot.core.io.readerwriter.BaseSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements reader as well as writer interfaces for fixed-byte single column and single value data.
 * <ul>
 *   <li> Auto expands memory allocation on-demand. </li>
 *   <li> Supports random reads and writes. </li>
 *   <li> Callers should ensure they are only reading row that were written, as allocated but not written rows
 *   are not guaranteed to have a deterministic value. </li>
 * </ul>
 */
public class FixedByteSingleColumnSingleValueReaderWriter extends BaseSingleColumnSingleValueReaderWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSingleColumnSingleValueReaderWriter.class);

  private List<WriterWithOffset> _writers = new ArrayList<>();
  private List<ReaderWithOffset> _readers = new ArrayList<>();
  private List<PinotDataBuffer> _dataBuffers = new ArrayList<>();

  private final long _chunkSizeInBytes;
  private final int _numRowsPerChunk;
  private final int _columnSizesInBytes;

  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;
  private int _capacityInRows = 0;

  /**
   * @param numRowsPerChunk Number of rows to pack in one chunk before a new chunk is created.
   * @param columnSizesInBytes Size of column value in bytes.
   * @param memoryManager Memory manager to be used for allocating memory.
   * @param allocationContext Allocation allocationContext.
   */
  public FixedByteSingleColumnSingleValueReaderWriter(int numRowsPerChunk, int columnSizesInBytes,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    _chunkSizeInBytes = numRowsPerChunk * columnSizesInBytes;
    _numRowsPerChunk = numRowsPerChunk;
    _columnSizesInBytes = columnSizesInBytes;
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    addBuffer();
  }

  @Override
  public void close()
      throws IOException {
    for (ReaderWithOffset reader : _readers) {
      reader.close();
    }
    for (WriterWithOffset writer : _writers) {
      writer.close();
    }
    for (PinotDataBuffer buffer : _dataBuffers) {
      buffer.close();
    }
  }

  @Override
  public void setInt(int row, int i) {
    addBufferIfNeeded(row);
    getWriterForRow(row).setInt(row, i);
  }

  @Override
  public void setLong(int row, long l) {
    addBufferIfNeeded(row);
    getWriterForRow(row).setLong(row, l);
  }

  @Override
  public void setFloat(int row, float f) {
    addBufferIfNeeded(row);
    getWriterForRow(row).setFloat(row, f);
  }

  @Override
  public void setDouble(int row, double d) {
    addBufferIfNeeded(row);
    getWriterForRow(row).setDouble(row, d);
  }

  private WriterWithOffset getWriterForRow(int row) {
    return _writers.get(getBufferId(row));
  }

  @Override
  public int getInt(int row) {
    int bufferId = getBufferId(row);
    return _readers.get(bufferId).getInt(row);
  }

  @Override
  public long getLong(int row) {
    int bufferId = getBufferId(row);
    return _readers.get(bufferId).getLong(row);
  }

  @Override
  public float getFloat(int row) {
    int bufferId = getBufferId(row);
    return _readers.get(bufferId).getFloat(row);
  }

  @Override
  public double getDouble(int row) {
    int bufferId = getBufferId(row);
    return _readers.get(bufferId).getDouble(row);
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    /*
     * TODO
     * If we assume that the row IDs in 'rows' is sorted, then we can write logic to move values from one reader
     * at a time, identifying the rows in sequence that belong to the same block. This logic is more complex, but may
     * perform better in the sorted case.
     *
     * An alternative is to not have multiple _dataBuffers, but just copy the values from one buffer to next as we
     * increase the number of rows.
     */
    if (_readers.size() == 1) {
      _readers.get(0).getReader().readIntValues(rows, 0, rowStartPos, rowSize, values, valuesStartPos);
    } else {
      for (int rowIter = rowStartPos, valueIter = valuesStartPos; rowIter < rowStartPos + rowSize;
          rowIter++, valueIter++) {
        int row = rows[rowIter];
        int bufferId = getBufferId(row);
        values[valueIter] = _readers.get(bufferId).getInt(row);
      }
    }
  }

  private int getBufferId(int row) {
    return row / _numRowsPerChunk;
  }

  private void addBuffer() {
    LOGGER.info("Allocating {} bytes for: {}", _chunkSizeInBytes, _allocationContext);
    PinotDataBuffer buffer = _memoryManager.allocate(_chunkSizeInBytes, _allocationContext);
    _capacityInRows += _numRowsPerChunk;

    buffer.order(ByteOrder.nativeOrder());
    _dataBuffers.add(buffer);

    FixedByteSingleValueMultiColReader reader =
        new FixedByteSingleValueMultiColReader(buffer, _numRowsPerChunk, new int[]{_columnSizesInBytes});
    FixedByteSingleValueMultiColWriter writer =
        new FixedByteSingleValueMultiColWriter(buffer, _numRowsPerChunk, /*cols=*/1, new int[]{_columnSizesInBytes});

    final int startRowId = _numRowsPerChunk * (_dataBuffers.size() - 1);
    _writers.add(new WriterWithOffset(writer, startRowId));
    _readers.add(new ReaderWithOffset(reader, startRowId));
  }

  /**
   * Helper class that encapsulates writer and global startRowId.
   */
  private void addBufferIfNeeded(int row) {
    if (row >= _capacityInRows) {

      // Adding _chunkSizeInBytes in the numerator for rounding up. +1 because rows are 0-based index.
      long buffersNeeded = (row + 1 - _capacityInRows + _numRowsPerChunk) / _numRowsPerChunk;
      for (int i = 0; i < buffersNeeded; i++) {
        addBuffer();
      }
    }
  }

  private static class WriterWithOffset implements Closeable {
    final FixedByteSingleValueMultiColWriter _writer;
    final int _startRowId;

    private WriterWithOffset(FixedByteSingleValueMultiColWriter writer, int startRowId) {
      _writer = writer;
      _startRowId = startRowId;
    }

    @Override
    public void close() {
      _writer.close();
    }

    public void setInt(int row, int value) {
      _writer.setInt(row - _startRowId, 0, value);
    }

    public void setLong(int row, long value) {
      _writer.setLong(row - _startRowId, 0, value);
    }

    public void setFloat(int row, float value) {
      _writer.setFloat(row - _startRowId, 0, value);
    }

    public void setDouble(int row, double value) {
      _writer.setDouble(row - _startRowId, 0, value);
    }
  }

  /**
   * Helper class that encapsulates reader and global startRowId.
   */
  private static class ReaderWithOffset implements Closeable {
    final FixedByteSingleValueMultiColReader _reader;
    final int _startRowId;

    private ReaderWithOffset(FixedByteSingleValueMultiColReader reader, int startRowId) {
      _reader = reader;
      _startRowId = startRowId;
    }

    @Override
    public void close() {
      _reader.close();
    }

    public int getInt(int row) {
      return _reader.getInt(row - _startRowId, 0);
    }

    public long getLong(int row) {
      return _reader.getLong(row - _startRowId, 0);
    }

    public float getFloat(int row) {
      return _reader.getFloat(row - _startRowId, 0);
    }

    public double getDouble(int row) {
      return _reader.getDouble(row - _startRowId, 0);
    }

    public FixedByteSingleValueMultiColReader getReader() {
      return _reader;
    }
  }
}
