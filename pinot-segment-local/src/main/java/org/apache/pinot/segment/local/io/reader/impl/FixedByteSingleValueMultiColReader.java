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
package org.apache.pinot.segment.local.io.reader.impl;

import java.io.Closeable;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 *
 * Generic utility class to read data from file. The data file consists of rows
 * and columns. The number of columns are fixed. Each column can have either
 * single value or multiple values. There are two basic types of methods to read
 * the data <br>
 * 1. &lt;TYPE&gt; getType(int row, int col) this is used for single value column <br>
 * 2. int getTYPEArray(int row, int col, TYPE[] array). The caller has to create
 * and initialize the array. The implementation will fill up the array. The
 * caller is responsible to ensure that the array is big enough to fit all the
 * values. The return value tells the number of values.<br>
 *
 *
 */
public class FixedByteSingleValueMultiColReader implements Closeable {
  private final PinotDataBuffer _dataBuffer;
  private final int _numRows;
  private final int[] _columnSizes;
  private final int[] _columnOffSets;
  private final int _rowSize;

  public FixedByteSingleValueMultiColReader(PinotDataBuffer dataBuffer, int numRows, int[] columnSizes) {
    _dataBuffer = dataBuffer;
    _numRows = numRows;
    _columnSizes = columnSizes;
    int numColumns = _columnSizes.length;
    _columnOffSets = new int[numColumns];
    int offset = 0;
    for (int i = 0; i < numColumns; i++) {
      _columnOffSets[i] = offset;
      offset += columnSizes[i];
    }
    _rowSize = offset;
  }

  /**
   * Computes the offset where the actual column data can be read
   *
   * @param row
   * @param col
   * @return
   */
  private int computeOffset(int row, int col) {
    final int offset = row * _rowSize + _columnOffSets[col];
    return offset;
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public char getChar(int row, int col) {
    final int offset = computeOffset(row, col);
    return _dataBuffer.getChar(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public short getShort(int row, int col) {
    final int offset = computeOffset(row, col);
    return _dataBuffer.getShort(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public int getInt(int row, int col) {
    assert getColumnSizes()[col] == 4;
    final int offset = computeOffset(row, col);
    return _dataBuffer.getInt(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public long getLong(int row, int col) {
    assert getColumnSizes()[col] == 8;
    final int offset = computeOffset(row, col);
    return _dataBuffer.getLong(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public float getFloat(int row, int col) {
    assert getColumnSizes()[col] == 4;
    final int offset = computeOffset(row, col);
    return _dataBuffer.getFloat(offset);
  }

  /**
   * Reads the double at row,col
   *
   * @param row
   * @param col
   * @return
   */
  public double getDouble(int row, int col) {
    assert getColumnSizes()[col] == 8;
    final int offset = computeOffset(row, col);
    return _dataBuffer.getDouble(offset);
  }

  /**
   * Returns the string value, NOTE: It expects all String values in the file
   * to be of same length
   *
   * @param row
   * @param col
   * @return
   */
  public String getString(int row, int col) {
    return new String(getBytes(row, col), UTF_8);
  }

  /**
   * Generic method to read the raw bytes
   *
   * @param row
   * @param col
   * @return
   */
  public byte[] getBytes(int row, int col) {
    final int length = getColumnSizes()[col];
    final byte[] dst = new byte[length];
    final int offset = computeOffset(row, col);
    _dataBuffer.copyTo(offset, dst, 0, length);
    return dst;
  }

  public int getNumberOfRows() {
    return _numRows;
  }

  public int[] getColumnSizes() {
    return _columnSizes;
  }

  public boolean open() {
    return false;
  }

  public void readIntValues(int[] rows, int col, int startPos, int limit, int[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getInt(rows[iter], col);
    }
  }

  public void readLongValues(int[] rows, int col, int startPos, int limit, long[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getLong(rows[iter], col);
    }
  }

  public void readFloatValues(int[] rows, int col, int startPos, int limit, float[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getFloat(rows[iter], col);
    }
  }

  public void readDoubleValues(int[] rows, int col, int startPos, int limit, double[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getDouble(rows[iter], col);
    }
  }

  public void readStringValues(int[] rows, int col, int startPos, int limit, String[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getString(rows[iter], col);
    }
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
