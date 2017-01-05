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
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.IOException;


public class FixedByteSingleColumnSingleValueReaderWriter extends BaseSingleColumnSingleValueReaderWriter {

  FixedByteSingleValueMultiColReader reader;
  FixedByteSingleValueMultiColWriter writer;
  private int cols;
  private int[] colOffSets;
  private int rowSize;
  private PinotDataBuffer _buffer;

  public FixedByteSingleColumnSingleValueReaderWriter(int rows, int columnSizesInBytes) throws IOException {
    this(rows, new int[]{columnSizesInBytes});
  }
  /**
   *
   * @param rows
   * @param columnSizesInBytes
   */
  public FixedByteSingleColumnSingleValueReaderWriter(int rows, int[] columnSizesInBytes) throws IOException {
    this.cols = 1;
    colOffSets = new int[columnSizesInBytes.length];
    rowSize = 0;
    for (int i = 0; i < columnSizesInBytes.length; i++) {
      colOffSets[i] = rowSize;
      rowSize += columnSizesInBytes[i];
    }
    final int totalSize = rowSize * rows;
    _buffer = PinotDataBuffer.allocateDirect(totalSize);
    //_buffer.order(ByteOrder.nativeOrder());
    reader = new FixedByteSingleValueMultiColReader(_buffer, rows, cols, columnSizesInBytes);
    writer = new FixedByteSingleValueMultiColWriter(_buffer, rows, cols, columnSizesInBytes);
  }

  @Override
  public void close() throws IOException {
    reader.close();
    reader = null;
    writer.close();
    writer = null;
    _buffer.close();
    _buffer = null;
  }

  @Override
  public void setChar(int row, char ch) {
    writer.setChar(row, 0, ch);
  }

  @Override
  public void setInt(int row, int i) {
    writer.setInt(row, 0, i);

  }

  @Override
  public void setShort(int row, short s) {
    writer.setShort(row, 0, s);

  }

  @Override
  public void setLong(int row, long l) {
    writer.setLong(row, 0, l);

  }

  @Override
  public void setFloat(int row, float f) {
    writer.setFloat(row, 0, f);

  }

  @Override
  public void setDouble(int row, double d) {
    writer.setDouble(row, 0, d);

  }

  @Override
  public void setString(int row, String string) {
    writer.setString(row, 0, string);
  }

  @Override
  public void setBytes(int row, byte[] bytes) {
    writer.setBytes(row, 0, bytes);
  }

  @Override
  public char getChar(int row) {
    return reader.getChar(row, 0);
  }

  @Override
  public short getShort(int row) {
    return reader.getShort(row, 0);
  }

  @Override
  public int getInt(int row) {
    return reader.getInt(row, 0);
  }

  @Override
  public long getLong(int row) {
    return reader.getLong(row, 0);
  }

  @Override
  public float getFloat(int row) {
    return reader.getFloat(row, 0);
  }

  @Override
  public double getDouble(int row) {
    return reader.getDouble(row, 0);
  }

  @Override
  public String getString(int row) {
    return reader.getString(row, 0);
  }

  @Override
  public byte[] getBytes(int row) {
    return reader.getBytes(row, 0);
  }

  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    reader.readIntValues(rows, 0, rowStartPos, rowSize, values, valuesStartPos);
  }
}
