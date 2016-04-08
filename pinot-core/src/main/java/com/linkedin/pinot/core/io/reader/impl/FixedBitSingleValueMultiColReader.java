/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.util.PinotDataCustomBitSet;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic utility class to read data from file. The data file consists of rows
 * and columns. The number of columns are fixed. Each column can have either
 * single value or multiple values. There are two basic types of methods to read
 * the data <br>
 * 1. &lt;TYPE&gt; getType(int row, int col) this is used for single value column <br>
 * 2. int getTYPEArray(int row, int col, TYPE[] array). The caller has to create
 * and initialize the array. The implementation will fill up the array. The
 * caller is responsible to ensure that the array is big enough to fit all the
 * values. The return value tells the number of values.<br>
 */
public class FixedBitSingleValueMultiColReader implements Closeable {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(FixedBitSingleValueMultiColReader.class);

  private int rows;
  private int cols;
  private int[] colBitOffSets;
  private int rowSizeInBits;
  private PinotDataBuffer indexBuffer;
  private int[] colSizesInBits;

  /**
   * used to get the actual value val - offset. offset is non zero if the values
   * contain negative numbers
   */
  private int[] offsets;
  private PinotDataCustomBitSet customBitSet;

  private int totalSizeInBytes;

  public FixedBitSingleValueMultiColReader(PinotDataBuffer indexBuffer, int rows, int cols,
      int[] columnSizesInBits) throws IOException {
    boolean[] signed = new boolean[cols];
    Arrays.fill(signed, false);
    instantiate(indexBuffer, rows, cols, columnSizesInBits, signed);
  }

  /**
   * @param indexBuffer
   * @param rows
   * @param cols
   * @param columnSizesInBits
   *          in bytes
   * @param signed
   *          , true if the data consists of negative numbers
   * @throws IOException
   */
  public FixedBitSingleValueMultiColReader(PinotDataBuffer indexBuffer, int rows, int cols, int[] columnSizesInBits,
      boolean[] signed) {
    instantiate(indexBuffer, rows, cols, columnSizesInBits, signed);
  }

  private void instantiate(PinotDataBuffer indexBuffer, int rows, int cols,
      int[] columnSizesInBits, boolean[] signed) {
    init(rows, cols, columnSizesInBits, signed);
    this.indexBuffer = indexBuffer.view(0, totalSizeInBytes);
    customBitSet = PinotDataCustomBitSet.withDataBuffer(totalSizeInBytes, indexBuffer);
  }

  private void init(int rows, int cols, int[] columnSizesInBits, boolean[] signed) {
    this.rows = rows;
    this.cols = cols;
    this.colSizesInBits = new int[cols];
    rowSizeInBits = 0;
    colBitOffSets = new int[cols];
    offsets = new int[cols];
    for (int i = 0; i < columnSizesInBits.length; i++) {
      colBitOffSets[i] = rowSizeInBits;
      int colSize = columnSizesInBits[i];
      offsets[i] = 0;
      if (signed[i]) {
        offsets[i] = (int) Math.pow(2, colSize) - 1;
        colSize += 1;
      }
      colSizesInBits[i] = colSize;
      rowSizeInBits += colSize;
    }
    totalSizeInBytes = (int) (((((long) rowSizeInBits) * rows) + 7) / 8);
  }

  /**
   * Computes the bit offset where the actual column data can be read
   * @param row
   * @param col
   * @return
   */
  private long computeBitOffset(int row, int col) {
    final long offset = ( (long)row * rowSizeInBits ) + colBitOffSets[col];
    return offset;
  }

  /**
   * @param row
   * @param col
   * @return
   */
  public int getInt(int row, int col) {
    final long startBitOffset = computeBitOffset(row, col);
    final long endBitOffset = startBitOffset + colSizesInBits[col];
    int ret = customBitSet.readInt(startBitOffset, endBitOffset);
    ret = ret - offsets[col];
    return ret;
  }

  /**
   * @param startRow
   * @param col
   * @return
   */
  public void getInt(int startRow, int length, int col, int[] output) {
    long startBitOffset = computeBitOffset(startRow, col);
    long endBitOffset;
    for (int i = 0; i < length; i++) {
      endBitOffset = startBitOffset + colSizesInBits[col];
      output[i] = customBitSet.readInt(startBitOffset, endBitOffset) - offsets[col];
      startBitOffset = endBitOffset;
    }
  }

  /**
   * @param row
   * @param col
   * @param context to store the state of previous read
   * @return
   */
  public int getInt(int row, int col, ReaderContext context) {
    final long startBitOffset = computeBitOffset(row, col);
    final long endBitOffset = startBitOffset + colSizesInBits[col];
    int ret = customBitSet.readInt(startBitOffset, endBitOffset);
    ret = ret - offsets[col];
    return ret;
  }

  public int getNumberOfRows() {
    return rows;
  }

  public int getNumberOfCols() {
    return rows;
  }

  public int[] getColumnSizes() {
    return colSizesInBits;
  }

  @Override
  public void close() throws IOException {
    customBitSet.close();
    indexBuffer.close();
  }

  public boolean open() {
    return true;
  }

  public void readValues(int[] rows, int col, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    int endPos = rowStartPos + rowSize;
    for (int ri = rowStartPos; ri < endPos; ++ri) {
      values[valuesStartPos++] = getInt(rows[ri], col);
    }
  }
}
