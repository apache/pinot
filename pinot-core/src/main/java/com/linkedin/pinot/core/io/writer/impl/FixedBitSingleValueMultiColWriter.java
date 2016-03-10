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
package com.linkedin.pinot.core.io.writer.impl;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.util.PinotDataCustomBitSet;
import java.io.Closeable;
import java.io.File;
import java.nio.channels.FileChannel;
import java.util.Arrays;


/**
 * Represents a datatable where each col contains values that can be represented
 * using a fix set of bits.
 *
 *
 */
public class FixedBitSingleValueMultiColWriter implements Closeable {

  private int[] columnOffsetsInBits;

  private int[] offsets;
  private PinotDataBuffer indexDataBuffer;
  private int rowSizeInBits;
  private int[] colSizesInBits;
  private int[] maxValues;
  private int[] minValues;

  private PinotDataCustomBitSet bitSet;
  private int bytesRequired;

  public FixedBitSingleValueMultiColWriter(File file, int rows,
      int cols, int[] columnSizesInBits) throws Exception {
    init(rows, cols, columnSizesInBits);
    this.indexDataBuffer = PinotDataBuffer.fromFile(file, 0, bytesRequired, ReadMode.mmap, FileChannel.MapMode.READ_WRITE,
        file.getAbsolutePath() + this.getClass().getCanonicalName());
    bitSet = PinotDataCustomBitSet.withDataBuffer(bytesRequired, indexDataBuffer);
  }


  public FixedBitSingleValueMultiColWriter(File file, int rows,
      int cols, int[] columnSizesInBits, boolean[] hasNegativeValues)
      throws Exception {
    init(rows, cols, columnSizesInBits, hasNegativeValues);
    this.indexDataBuffer = PinotDataBuffer.fromFile(file, 0, bytesRequired, ReadMode.mmap,
        FileChannel.MapMode.READ_WRITE, file.getAbsolutePath() + this.getClass().getCanonicalName());
    bitSet = PinotDataCustomBitSet.withDataBuffer(bytesRequired, indexDataBuffer);
  }

  public FixedBitSingleValueMultiColWriter(PinotDataBuffer dataBuffer, int rows,
      int cols, int[] columnSizesInBits) throws Exception {
    init(rows, cols, columnSizesInBits);
    this.indexDataBuffer = dataBuffer;
    bitSet = PinotDataCustomBitSet.withDataBuffer(bytesRequired, indexDataBuffer);
  }

  public FixedBitSingleValueMultiColWriter(PinotDataBuffer dataBuffer, int rows,
      int cols, int[] columnSizesInBits, boolean[] hasNegativeValues)
      throws Exception {
    init(rows, cols, columnSizesInBits, hasNegativeValues);
    this.indexDataBuffer = dataBuffer;
    bitSet = PinotDataCustomBitSet.withDataBuffer(bytesRequired, indexDataBuffer);
  }

  private void init(int rows, int cols, int[] columnSizesInBits) {
    boolean[] hasNegativeValues = new boolean[cols];
    Arrays.fill(hasNegativeValues, false);
    init(rows, cols, columnSizesInBits, hasNegativeValues);
  }

  private void init(int rows, int cols, int[] columnSizesInBits,
      boolean[] signed) {
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
    long totalSizeInBits = ((long) rowSizeInBits) * rows;
    // jfim: We keep the number of bytes required as an int, as Java Buffers cannot be larger than 2GB
    this.bytesRequired = (int)((totalSizeInBits + 7) / 8);
  }

  public boolean open() {
    return true;
  }

  /**
   *
   * @param row
   * @param col
   * @param val
   */
  public void setInt(int row, int col, int val) {
    assert val >= minValues[col]  && val <= maxValues[col];
    long bitOffset = ((long) rowSizeInBits) * row + columnOffsetsInBits[col];
    val = val + offsets[col];
    bitSet.writeInt(bitOffset, colSizesInBits[col], val);
  }

  @Override
  public void close() {
    bitSet.close();
    indexDataBuffer.close();
    bitSet = null;
    indexDataBuffer = null;
  }
}
