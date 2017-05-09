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
import com.linkedin.pinot.core.io.readerwriter.BaseSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


// TODO Check if MMAP allows us to pack more partitions on a single machine.
public class FixedByteSingleColumnSingleValueReaderWriter extends BaseSingleColumnSingleValueReaderWriter {

  FixedByteSingleValueMultiColWriter currentWriter;
  private List<FixedByteSingleValueMultiColWriter> writers = new ArrayList<>();
  private List<FixedByteSingleValueMultiColReader> readers = new ArrayList<>();
  private List<PinotDataBuffer> buffers = new ArrayList<>();
  private final long chunkSizeInBytes;
  private final int numRowsPerChunk;
  private final int columnSizesInBytes;
  private long numBytesInCurrentWriter = 0;

  /**
   *  @param numRowsPerChunk Number of rows to pack in one chunk before a new chunk is created.
   * @param columnSizesInBytes
   */
  public FixedByteSingleColumnSingleValueReaderWriter(int numRowsPerChunk, int columnSizesInBytes) {
    chunkSizeInBytes = numRowsPerChunk * columnSizesInBytes;
    this.numRowsPerChunk = numRowsPerChunk;
    this.columnSizesInBytes = columnSizesInBytes;
    addBuffer();
  }

  private void addBuffer() {
    PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(chunkSizeInBytes);
    buffer.order(ByteOrder.nativeOrder());
    buffers.add(buffer);
    FixedByteSingleValueMultiColReader reader = new FixedByteSingleValueMultiColReader(buffer, numRowsPerChunk, new int[]{columnSizesInBytes});
    currentWriter = new FixedByteSingleValueMultiColWriter(buffer, numRowsPerChunk, /*cols=*/1, new int[]{columnSizesInBytes});
    readers.add(reader);
    writers.add(currentWriter);
    numBytesInCurrentWriter = 0;
  }

  private void addBufferIfNeeded(int row) {
    if (numBytesInCurrentWriter + columnSizesInBytes > chunkSizeInBytes) {
      addBuffer();
    }
    numBytesInCurrentWriter += columnSizesInBytes;
  }

  @Override
  public void close() throws IOException {
    for (FixedByteSingleValueMultiColReader reader : readers) {
      reader.close();
    }
    for (FixedByteSingleValueMultiColWriter writer : writers) {
      writer.close();
    }
    for (PinotDataBuffer buffer : buffers) {
      buffer.close();
    }
  }

  private int getRowInChunk(int row) {
    return row % numRowsPerChunk;
  }

  private int getBufferId(int row) {
    return row / numRowsPerChunk;
  }

  @Override
  public void setChar(int row, char ch) {
    addBufferIfNeeded(row);
    int rowInChunk = getRowInChunk(row);
    currentWriter.setChar(rowInChunk, 0, ch);
  }

  @Override
  public void setInt(int row, int i) {
    addBufferIfNeeded(row);
    int rowInChunk = getRowInChunk(row);
    currentWriter.setInt(rowInChunk, 0, i);
  }

  @Override
  public void setShort(int row, short s) {
    addBufferIfNeeded(row);
    int rowInChunk = getRowInChunk(row);
    currentWriter.setShort(rowInChunk, 0, s);
  }

  @Override
  public void setLong(int row, long l) {
    addBufferIfNeeded(row);
    int rowInChunk = getRowInChunk(row);
    currentWriter.setLong(rowInChunk, 0, l);
  }

  @Override
  public void setFloat(int row, float f) {
    addBufferIfNeeded(row);
    int rowInChunk = getRowInChunk(row);
    currentWriter.setFloat(rowInChunk, 0, f);
  }

  @Override
  public void setDouble(int row, double d) {
    addBufferIfNeeded(row);
    int rowInChunk = getRowInChunk(row);
    currentWriter.setDouble(rowInChunk, 0, d);
  }

  @Override
  public void setString(int row, String string) {
    addBufferIfNeeded(row);
    int rowInChunk = getRowInChunk(row);
    currentWriter.setString(rowInChunk, 0, string);
  }

  @Override
  public void setBytes(int row, byte[] bytes) {
    addBufferIfNeeded(row);
    int rowInChunk = getRowInChunk(row);
    currentWriter.setBytes(rowInChunk, 0, bytes);
  }

  @Override
  public char getChar(int row) {
    int rowInChunk = getRowInChunk(row);
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getChar(rowInChunk, 0);
  }

  @Override
  public short getShort(int row) {
    int rowInChunk = getRowInChunk(row);
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getShort(rowInChunk, 0);
  }

  @Override
  public int getInt(int row) {
    int rowInChunk = getRowInChunk(row);
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getInt(rowInChunk, 0);
  }

  @Override
  public long getLong(int row) {
    int rowInChunk = getRowInChunk(row);
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getLong(rowInChunk, 0);
  }

  @Override
  public float getFloat(int row) {
    int rowInChunk = getRowInChunk(row);
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getFloat(rowInChunk, 0);
  }

  @Override
  public double getDouble(int row) {
    int rowInChunk = getRowInChunk(row);
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getDouble(rowInChunk, 0);
  }

  @Override
  public String getString(int row) {
    int rowInChunk = getRowInChunk(row);
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getString(rowInChunk, 0);
  }

  @Override
  public byte[] getBytes(int row) {
    int rowInChunk = getRowInChunk(row);
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getBytes(rowInChunk, 0);
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    /*
     * TODO
     * If we assume that the row IDs in 'rows' is sorted, then we can write logic to move values from one reader
     * at a time, identifying the rows in sequence that belong to the same block. This logic is more complex, but may
     * perform better in the sorted case.
     *
     * An alternative is to not have multiple buffers, but just copy the values from one buffer to next as we
     * increase the number of rows.
     */
    if (readers.size() == 1) {
      readers.get(0).readIntValues(rows, 0, rowStartPos, rowSize, values, valuesStartPos);
    }
    int numRows = 0;
    for (int rowIter = rowStartPos, valueIter = valuesStartPos; numRows < rowSize; numRows++, rowIter++, valueIter++) {
      int row = rows[rowIter];
      values[valueIter] = getInt(row);
    }
  }
}
