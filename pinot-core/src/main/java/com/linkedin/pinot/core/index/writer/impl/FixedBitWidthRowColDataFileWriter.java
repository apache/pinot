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
package com.linkedin.pinot.core.index.writer.impl;

import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.core.util.CustomBitSet;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;


/**
 * Represents a datatable where each col contains values that can be represented
 * using a fix set of bits.
 *
 *
 */
public class FixedBitWidthRowColDataFileWriter implements Closeable {
  private File file;
  private int[] columnOffsetsInBits;

  private int[] offsets;
  private MappedByteBuffer byteBuffer;
  private RandomAccessFile raf;
  private int rowSizeInBits;
  private int[] colSizesInBits;
  private int[] maxValues;
  private int[] minValues;

  private CustomBitSet bitSet;
  private int bytesRequired;

  public FixedBitWidthRowColDataFileWriter(File file, int rows, int cols,
                                           int[] columnSizesInBits) throws Exception {
    init(file, rows, cols, columnSizesInBits);
    createBuffer(file);
    bitSet = CustomBitSet.withByteBuffer(bytesRequired, byteBuffer);
  }

  /**
   * forwardIndexFile, numDocs, 1, new int[] { maxNumberOfBits }, new boolean[] { hasNulls })
   * @param cols 1
   * @param columnSizesInBits 是一个空数组，长度是列的势
   * */
  public FixedBitWidthRowColDataFileWriter(File file, int rows, int cols,
                                           int[] columnSizesInBits, boolean[] hasNegativeValues) throws Exception {
    init(file, rows, cols, columnSizesInBits, hasNegativeValues);
    createBuffer(file);
    bitSet = CustomBitSet.withByteBuffer(bytesRequired, byteBuffer);
  }

  public FixedBitWidthRowColDataFileWriter(ByteBuffer byteBuffer, int rows,
                                           int cols, int[] columnSizesInBits) throws Exception {
    init(file, rows, cols, columnSizesInBits);
    bitSet = CustomBitSet.withByteBuffer(bytesRequired, byteBuffer);
  }

  public FixedBitWidthRowColDataFileWriter(ByteBuffer byteBuffer, int rows,
                                           int cols, int[] columnSizesInBits, boolean[] hasNegativeValues)
          throws Exception {
    init(file, rows, cols, columnSizesInBits, hasNegativeValues);
    bitSet = CustomBitSet.withByteBuffer(bytesRequired, byteBuffer);
  }

  private void init(File file, int rows, int cols, int[] columnSizesInBits) {
    boolean[] hasNegativeValues = new boolean[cols];
    Arrays.fill(hasNegativeValues, false);
    init(file, rows, cols, columnSizesInBits, hasNegativeValues);
  }

  private void init(File file, int rows, int cols, int[] columnSizesInBits,
                    boolean[] signed) {
    this.file = file;
    this.colSizesInBits = new int[cols];
    this.columnOffsetsInBits = new int[cols];
    this.offsets = new int[cols];
    this.maxValues = new int[cols];
    this.minValues = new int[cols];
    this.rowSizeInBits = 0;
    for (int i = 0; i < cols; i++) {
      this.columnOffsetsInBits[i] = rowSizeInBits;
      int colSize = columnSizesInBits[i];
      int max = (int) Math.pow(2, colSize);
      this.maxValues[i] = max - 1;
      this.minValues[i] = 0;
      this.offsets[i] = 0;
      // additional bit for sign
      if (signed[i]) {
        this.offsets[i] = this.maxValues[i];
        this.minValues[i] = -this.maxValues[i];
        colSize += 1;
      }
      this.rowSizeInBits += colSize;
      this.colSizesInBits[i] = colSize;
    }
    int totalSizeInBits = rowSizeInBits * rows;
    this.bytesRequired = (totalSizeInBits + 7) / 8;
  }

  private void createBuffer(File file) throws FileNotFoundException,
          IOException {
    raf = new RandomAccessFile(file, "rw");
    byteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
            bytesRequired);
    byteBuffer.position(0);
    for (int i = 0; i < bytesRequired; i++) {
      byteBuffer.put((byte) 0);
    }
  }

  public boolean open() {
    return true;
  }

  /**
   *
   * @param row
   * @param col 通常情况等于0
   * @param val
   */
  public void setInt(int row, int col, int val) {
    assert val >= minValues[col]  && val <= maxValues[col];
    // rowSizeInBits = 1
    // columnOffsetsInBits[0]=0
    // bitOffset = rowSizeInBits * row + columnOffsetsInBits[col];
    //=> bitOffset =  row
    int bitOffset = rowSizeInBits * row + columnOffsetsInBits[col];
    val = val + offsets[col];
    for (int bitPos = colSizesInBits[col] - 1; bitPos >= 0; bitPos--) {
      if ((val & (1 << bitPos)) != 0) {
        bitSet.setBit(bitOffset + (colSizesInBits[col] - bitPos - 1));
      }
    }
  }

  public void close() {
    IOUtils.closeQuietly(raf);
    raf = null;
    MmapUtils.unloadByteBuffer(byteBuffer);
  }
}
