package com.linkedin.pinot.core.index.writer.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import com.linkedin.pinot.core.util.CustomBitSet;

/**
 * Represents a datatable where each col contains values that can be represented
 * using a fix set of bits.
 * 
 * @author kgopalak
 *
 */
public class FixedBitWidthRowColDataFileWriter {
  private File file;
  private int[] columnOffsetsInBits;

  private int[] offsets;
  private ByteBuffer byteBuffer;
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
   * @param col
   * @param val
   */
  public void setInt(int row, int col, int val) {
    assert val > minValues[col]  && val < maxValues[col];
    int bitOffset = rowSizeInBits * row + columnOffsetsInBits[col];
    val = val + offsets[col];
    for (int bitPos = colSizesInBits[col] - 1; bitPos >= 0; bitPos--) {
      if ((val & (1 << bitPos)) != 0) {
        bitSet.setBit(bitOffset + (colSizesInBits[col] - bitPos - 1));
      }
    }
  }

  public boolean saveAndClose() {
    if (raf != null) {
      try {
        raf.close();
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }
}
