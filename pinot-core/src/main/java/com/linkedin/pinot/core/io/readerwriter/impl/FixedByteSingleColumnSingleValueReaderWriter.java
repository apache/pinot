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
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO Check if MMAP allows us to pack more partitions on a single machine.
public class FixedByteSingleColumnSingleValueReaderWriter extends BaseSingleColumnSingleValueReaderWriter {

  public static Logger LOGGER = LoggerFactory.getLogger(FixedByteSingleColumnSingleValueReaderWriter.class);
  WriterWithOffset currentWriter;
  private List<WriterWithOffset> writers = new ArrayList<>();
  private List<ReaderWithOffset> readers = new ArrayList<>();
  private List<PinotDataBuffer> buffers = new ArrayList<>();
  private final long chunkSizeInBytes;
  private final int numRowsPerChunk;
  private final int columnSizesInBytes;
  private final RealtimeIndexOffHeapMemoryManager memoryManager;
  private final String columnName;
  private long numBytesInCurrentWriter = 0;

  private static class WriterWithOffset implements Closeable {
    final FixedByteSingleValueMultiColWriter writer;
    final int startRowId;

    private WriterWithOffset(FixedByteSingleValueMultiColWriter writer, int startRowId) {
      this.writer = writer;
      this.startRowId = startRowId;
    }

    @Override
    public void close() {
      writer.close();
    }

    public void setInt(int row, int value) {
      writer.setInt(row - startRowId, 0, value);
    }

    public void setLong(int row, long value) {
      writer.setLong(row - startRowId, 0, value);
    }

    public void setFloat(int row, float value) {
      writer.setFloat(row - startRowId, 0, value);
    }

    public void setDouble(int row, double value) {
      writer.setDouble(row - startRowId, 0, value);
    }
  }

  private static class ReaderWithOffset implements Closeable {
    final FixedByteSingleValueMultiColReader reader;
    final int startRowId;

    private ReaderWithOffset(FixedByteSingleValueMultiColReader reader, int startRowId) {
      this.reader = reader;
      this.startRowId = startRowId;
    }

    @Override
    public void close() {
      reader.close();
    }

    public int getInt(int row) {
      return reader.getInt(row - startRowId, 0);
    }

    public long getLong(int row) {
      return reader.getLong(row - startRowId, 0);
    }

    public float getFloat(int row) {
      return reader.getFloat(row - startRowId, 0);
    }

    public double getDouble(int row) {
      return reader.getDouble(row - startRowId, 0);
    }

    public FixedByteSingleValueMultiColReader getReader() {
      return reader;
    }
  }

  /**
   * @param numRowsPerChunk Number of rows to pack in one chunk before a new chunk is created.
   * @param columnSizesInBytes
   * @param memoryManager
   * @param columnName
   */
  public FixedByteSingleColumnSingleValueReaderWriter(int numRowsPerChunk, int columnSizesInBytes,
      RealtimeIndexOffHeapMemoryManager memoryManager, String columnName) {
    chunkSizeInBytes = numRowsPerChunk * columnSizesInBytes;
    this.numRowsPerChunk = numRowsPerChunk;
    this.columnSizesInBytes = columnSizesInBytes;
    this.memoryManager = memoryManager;
    this.columnName = columnName;
    addBuffer();
  }

  private void addBuffer() {
    LOGGER.info("Allocating {} bytes for column {} for segment {}", chunkSizeInBytes, columnName, memoryManager.getSegmentName());
    PinotDataBuffer buffer = memoryManager.allocate(chunkSizeInBytes, columnName);
    buffer.order(ByteOrder.nativeOrder());
    buffers.add(buffer);
    FixedByteSingleValueMultiColReader reader = new FixedByteSingleValueMultiColReader(buffer, numRowsPerChunk, new int[]{columnSizesInBytes});
    FixedByteSingleValueMultiColWriter writer  = new FixedByteSingleValueMultiColWriter(buffer, numRowsPerChunk, /*cols=*/1, new int[]{columnSizesInBytes});
    final int startRowId = numRowsPerChunk * (buffers.size()-1);
    currentWriter = new WriterWithOffset(writer, startRowId);
    readers.add(new ReaderWithOffset(reader, startRowId));
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
    for (ReaderWithOffset reader : readers) {
      reader.close();
    }
    for (WriterWithOffset writer : writers) {
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
  public void setInt(int row, int i) {
    addBufferIfNeeded(row);
    currentWriter.setInt(row, i);
  }

  @Override
  public void setLong(int row, long l) {
    addBufferIfNeeded(row);
    currentWriter.setLong(row, l);
  }

  @Override
  public void setFloat(int row, float f) {
    addBufferIfNeeded(row);
    currentWriter.setFloat(row, f);
  }

  @Override
  public void setDouble(int row, double d) {
    addBufferIfNeeded(row);
    currentWriter.setDouble(row, d);
  }

  @Override
  public int getInt(int row) {
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getInt(row);
  }

  @Override
  public long getLong(int row) {
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getLong(row);
  }

  @Override
  public float getFloat(int row) {
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getFloat(row);
  }

  @Override
  public double getDouble(int row) {
    int bufferId = getBufferId(row);
    return readers.get(bufferId).getDouble(row);
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
      readers.get(0).getReader().readIntValues(rows, 0, rowStartPos, rowSize, values, valuesStartPos);
    } else {
      for (int rowIter = rowStartPos, valueIter = valuesStartPos;
          rowIter < rowStartPos + rowSize;
          rowIter++, valueIter++) {
        int row = rows[rowIter];
        int bufferId = getBufferId(row);
        values[valueIter] = readers.get(bufferId).getInt(row);
      }
    }
  }
}
