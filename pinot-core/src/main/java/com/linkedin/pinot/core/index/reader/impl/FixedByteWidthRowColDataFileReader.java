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
package com.linkedin.pinot.core.index.reader.impl;

import com.linkedin.pinot.common.utils.MmapUtils;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.indexsegment.utils.GenericRowColumnDataFileReader;

/**
 *
 * Generic utility class to read data from file. The data file consists of rows
 * and columns. The number of columns are fixed. Each column can have either
 * single value or multiple values. There are two basic types of methods to read
 * the data <br/>
 * 1. <TYPE> getType(int row, int col) this is used for single value column <br/>
 * 2. int getTYPEArray(int row, int col, TYPE[] array). The caller has to create
 * and initialize the array. The implementation will fill up the array. The
 * caller is responsible to ensure that the array is big enough to fit all the
 * values. The return value tells the number of values.<br/>
 *
 * @author kgopalak
 *
 */
public class FixedByteWidthRowColDataFileReader implements Closeable, AutoCloseable {
  private static Logger logger = LoggerFactory
      .getLogger(GenericRowColumnDataFileReader.class);

  RandomAccessFile file;
  private final int rows;
  private final int cols;
  private final int[] colOffSets;
  private int rowSize;
  private ByteBuffer byteBuffer;
  private final int[] columnSizes;

  /**
   *
   * @param file
   * @param rows
   * @param cols
   * @param columnSizes
   * @return
   * @throws IOException
   */
  public static FixedByteWidthRowColDataFileReader forHeap(File file,
      int rows, int cols, int[] columnSizes) throws IOException {
    return new FixedByteWidthRowColDataFileReader(file, rows, cols,
        columnSizes, false);
  }

  /**
   *
   * @param file
   * @param rows
   * @param cols
   * @param columnSizes
   * @return
   * @throws IOException
   */
  public static FixedByteWidthRowColDataFileReader forMmap(File file,
      int rows, int cols, int[] columnSizes) throws IOException {
    return new FixedByteWidthRowColDataFileReader(file, rows, cols,
        columnSizes, true);
  }

  /**
   *
   * @param fileName
   * @param rows
   * @param cols
   * @param columnSizes
   *            in bytes
   * @throws IOException
   */
  public FixedByteWidthRowColDataFileReader(File dataFile, int rows,
      int cols, int[] columnSizes, boolean isMmap) throws IOException {
    this.rows = rows;
    this.cols = cols;
    this.columnSizes = columnSizes;
    colOffSets = new int[columnSizes.length];
    rowSize = 0;
    for (int i = 0; i < columnSizes.length; i++) {
      colOffSets[i] = rowSize;
      rowSize += columnSizes[i];
    }
    file = new RandomAccessFile(dataFile, "rw");
    final long totalSize = rowSize * rows;
    if (isMmap) {
      byteBuffer = file.getChannel()
          .map(FileChannel.MapMode.READ_ONLY, 0, totalSize)
          .order(ByteOrder.BIG_ENDIAN);
    } else {
      byteBuffer = ByteBuffer.allocate((int) totalSize);
      file.getChannel().read(byteBuffer);
    }
  }

  public FixedByteWidthRowColDataFileReader(ByteBuffer buffer, int rows,
      int cols, int[] columnSizes) throws IOException {
    this.rows = rows;
    this.cols = cols;
    this.columnSizes = columnSizes;
    colOffSets = new int[columnSizes.length];
    rowSize = 0;
    for (int i = 0; i < columnSizes.length; i++) {
      colOffSets[i] = rowSize;
      rowSize += columnSizes[i];
    }
    byteBuffer = buffer;
  }

  public FixedByteWidthRowColDataFileReader(String fileName, int rows,
      int cols, int[] columnSizes) throws IOException {
    this(new File(fileName), rows, cols, columnSizes, true);
  }

  /**
   * Computes the offset where the actual column data can be read
   *
   * @param row
   * @param col
   * @return
   */
  private int computeOffset(int row, int col) {
    if (row >= rows || col >= cols) {
      final String message = String.format(
          "Input (%d,%d) is not with in expected range (%d,%d)", col,
          row, col, rows);
      throw new IndexOutOfBoundsException(message);
    }
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
    return byteBuffer.getChar(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public short getShort(int row, int col) {
    final int offset = computeOffset(row, col);
    return byteBuffer.getShort(offset);
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
    return byteBuffer.getInt(offset);
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
    return byteBuffer.getLong(offset);
  }

  /**
   *
   * @param row
   * @param col
   * @return
   */
  public float getFloat(int row, int col) {
    assert getColumnSizes()[col] == 8;
    final int offset = computeOffset(row, col);
    return byteBuffer.getFloat(offset);
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
    return byteBuffer.getDouble(offset);
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
    return new String(getBytes(row, col));
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
    byteBuffer.position(offset);
    byteBuffer.get(dst, 0, length);
    return dst;
  }

  public int getNumberOfRows() {
    return rows;
  }

  public int getNumberOfCols() {
    return rows;
  }

  public int[] getColumnSizes() {
    return columnSizes;
  }

  public void close() {
    IOUtils.closeQuietly(file);
    file = null;
    MmapUtils.unloadByteBuffer(byteBuffer);
  }

  public boolean open() {
    return false;
  }
}
