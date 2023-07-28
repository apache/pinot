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

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.io.reader.impl.FixedByteSingleValueMultiColReader;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteSingleValueMultiColWriter;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
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
// TODO: Fix thread-safety issue for ArrayList
// TODO: Optimize it
public class FixedByteSVMutableForwardIndex implements MutableForwardIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSVMutableForwardIndex.class);

  private final List<WriterWithOffset> _writers = new ArrayList<>();
  private final List<ReaderWithOffset> _readers = new ArrayList<>();

  private final boolean _dictionaryEncoded;
  private final DataType _storedType;
  private final int _valueSizeInBytes;
  private final int _numRowsPerChunk;
  private final long _chunkSizeInBytes;

  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;
  private int _capacityInRows = 0;

  /**
   * @param storedType Data type of the values
   * @param fixedLength Fixed length of values if known: only used for BYTES field (HyperLogLog and BigDecimal storage)
   * @param numRowsPerChunk Number of rows to pack in one chunk before a new chunk is created.
   * @param memoryManager Memory manager to be used for allocating memory.
   * @param allocationContext Allocation allocationContext.
   */
  public FixedByteSVMutableForwardIndex(boolean dictionaryEncoded, DataType storedType, int fixedLength,
      int numRowsPerChunk, PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    _dictionaryEncoded = dictionaryEncoded;
    _storedType = storedType;
    if (!storedType.isFixedWidth()) {
      Preconditions.checkState(fixedLength > 0, "Fixed length must be provided for type: %s", storedType);
      _valueSizeInBytes = fixedLength;
    } else {
      _valueSizeInBytes = storedType.size();
    }
    _numRowsPerChunk = numRowsPerChunk;
    _chunkSizeInBytes = numRowsPerChunk * _valueSizeInBytes;
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    addBuffer();
  }

  public FixedByteSVMutableForwardIndex(boolean dictionaryEncoded, DataType valueType, int numRowsPerChunk,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    this(dictionaryEncoded, valueType, -1, numRowsPerChunk, memoryManager, allocationContext);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return _dictionaryEncoded;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getStoredType() {
    return _storedType;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _valueSizeInBytes;
  }

  @Override
  public int getLengthOfLongestElement() {
    return _valueSizeInBytes;
  }

  @Override
  public int getDictId(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getInt(docId);
  }

  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer) {
    /*
     * TODO
     * If we assume that the document ids are sorted, then we can write logic to move values from one reader
     * at a time, identifying the rows in sequence that belong to the same block. This logic is more complex, but may
     * perform better in the sorted case.
     *
     * An alternative is to not have multiple _dataBuffers, but just copy the values from one buffer to next as we
     * increase the number of rows.
     */
    if (_readers.size() == 1) {
      _readers.get(0).getReader().readIntValues(docIds, 0, 0, length, dictIdBuffer, 0);
    } else {
      for (int i = 0; i < length; i++) {
        int docId = docIds[i];
        dictIdBuffer[i] = _readers.get(getBufferId(docId)).getInt(docId);
      }
    }
  }

  @Override
  public int getInt(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getInt(docId);
  }

  @Override
  public long getLong(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getLong(docId);
  }

  @Override
  public float getFloat(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getFloat(docId);
  }

  @Override
  public double getDouble(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getDouble(docId);
  }

  @Override
  public BigDecimal getBigDecimal(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getBigDecimal(docId);
  }

  private int getBufferId(int row) {
    return row / _numRowsPerChunk;
  }

  @Override
  public void setDictId(int docId, int dictId) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setInt(docId, dictId);
  }

  @Override
  public void setInt(int docId, int value) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setInt(docId, value);
  }

  @Override
  public void setLong(int docId, long value) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setLong(docId, value);
  }

  @Override
  public void setFloat(int docId, float value) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setFloat(docId, value);
  }

  @Override
  public void setDouble(int docId, double value) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setDouble(docId, value);
  }

  @Override
  public byte[] getBytes(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getBytes(docId);
  }

  @Override
  public void setBytes(int docId, byte[] value) {
    Preconditions.checkArgument(value.length == _valueSizeInBytes, "Expected value size to be: %s but got: %s ",
        _valueSizeInBytes, value.length);

    addBufferIfNeeded(docId);
    getWriterForRow(docId).setBytes(docId, value);
  }

  private WriterWithOffset getWriterForRow(int row) {
    return _writers.get(getBufferId(row));
  }

  @Override
  public void close()
      throws IOException {
    for (WriterWithOffset writer : _writers) {
      writer.close();
    }
    for (ReaderWithOffset reader : _readers) {
      reader.close();
    }
  }

  private void addBuffer() {
    LOGGER.info("Allocating {} bytes for: {}", _chunkSizeInBytes, _allocationContext);
    // NOTE: PinotDataBuffer is tracked in the PinotDataBufferMemoryManager. No need to track it inside the class.
    PinotDataBuffer buffer = _memoryManager.allocate(_chunkSizeInBytes, _allocationContext);
    _writers.add(
        new WriterWithOffset(new FixedByteSingleValueMultiColWriter(buffer, /*cols=*/1, new int[]{_valueSizeInBytes}),
            _capacityInRows));
    _readers.add(new ReaderWithOffset(
        new FixedByteSingleValueMultiColReader(buffer, _numRowsPerChunk, new int[]{_valueSizeInBytes}),
        _capacityInRows));
    _capacityInRows += _numRowsPerChunk;
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
    public void close()
        throws IOException {
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

    public void setBytes(int row, byte[] value) {
      _writer.setBytes(row - _startRowId, 0, value);
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
    public void close()
        throws IOException {
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

    public BigDecimal getBigDecimal(int row) {
      return BigDecimalUtils.deserialize(_reader.getBytes(row - _startRowId, 0));
    }

    public byte[] getBytes(int row) {
      return _reader.getBytes(row - _startRowId, 0);
    }

    public FixedByteSingleValueMultiColReader getReader() {
      return _reader;
    }
  }
}
