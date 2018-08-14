/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.io.reader.impl;

import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.Closeable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSingleValueMultiColReader.class);

  private final int rows;
  private final int[] colOffSets;
  private int rowSize;
  private PinotDataBuffer indexDataBuffer;
  private final int[] columnSizes;

  /**
   *
   * @param pinotDataBuffer
   * @param rows
   * @param columnSizes
   *            in bytes
   * @throws IOException
   */
  public FixedByteSingleValueMultiColReader(PinotDataBuffer pinotDataBuffer, int rows, int[] columnSizes) {
    this.rows = rows;
    this.columnSizes = columnSizes;
    colOffSets = new int[columnSizes.length];
    rowSize = 0;
    for (int i = 0; i < columnSizes.length; i++) {
      colOffSets[i] = rowSize;
      rowSize += columnSizes[i];
    }
    this.indexDataBuffer = pinotDataBuffer;
  }

  /**
   * Computes the offset where the actual column data can be read
   *
   * @param row
   * @param col
   * @return
   */
  private int computeOffset(int row, int col) {
    final int offset = row * rowSize + colOffSets[col];
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
    return indexDataBuffer.getChar(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public short getShort(int row, int col) {
    final int offset = computeOffset(row, col);
    return indexDataBuffer.getShort(offset);
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
    return indexDataBuffer.getInt(offset);
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
    return indexDataBuffer.getLong(offset);
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
    return indexDataBuffer.getFloat(offset);
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
    return indexDataBuffer.getDouble(offset);
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
    return StringUtil.decodeUtf8(getBytes(row, col));
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
    indexDataBuffer.copyTo(offset, dst, 0, length);
    return dst;
  }

  public int getNumberOfRows() {
    return rows;
  }

  public int[] getColumnSizes() {
    return columnSizes;
  }

  @Override
  public void close() throws IOException {
    indexDataBuffer.close();
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

}
