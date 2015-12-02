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
package com.linkedin.pinot.core.index.writer.impl.v2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.MmapUtils;
import com.linkedin.pinot.core.index.writer.SingleColumnSingleValueWriter;
import com.linkedin.pinot.core.util.SizeUtil;

import me.lemire.integercompression.BitPacking;

/**
 * Represents a datatable where each col contains values that can be represented
 * using a fix set of bits.
 */
public class FixedBitSingleValueWriter implements SingleColumnSingleValueWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedBitSingleValueWriter.class);
  private ByteBuffer byteBuffer;
  private RandomAccessFile raf;
  private int maxValue;
  private int minValue;
  private int currentRow = -1;
  int[] input;
  private int numBits;
  private int compressedSize;
  private int uncompressedSize;
  private int[] uncompressedData;
  private int[] compressedData;
  private int numRows;
  boolean ownsByteBuffer;
  boolean isMmap;

  private void init(File file, int rows, int numBits) throws Exception {
    init(rows, numBits, false);
    createBuffer(file);
  }

  public FixedBitSingleValueWriter(File file, int rows, int numBits) throws Exception {
    init(file, rows, numBits);
    createBuffer(file);
  }

  public FixedBitSingleValueWriter(File file, int rows, int numBits, boolean hasNegativeValues)
      throws Exception {
    init(rows, numBits, hasNegativeValues);
    createBuffer(file);
  }

  public FixedBitSingleValueWriter(ByteBuffer byteBuffer, int rows, int numBits) throws Exception {
    this.byteBuffer = byteBuffer;
    init(rows, numBits, false);
  }

  public FixedBitSingleValueWriter(ByteBuffer byteBuffer, int rows, int numBits,
      boolean hasNegativeValues) throws Exception {
    this.byteBuffer = byteBuffer;
    init(rows, numBits, hasNegativeValues);
  }

  private void init(int rows, int numBits, boolean signed) throws Exception {
    this.numRows = rows;
    int max = (int) Math.pow(2, numBits);
    this.maxValue = max - 1;
    // additional bit for sign
    if (signed) {
      this.minValue = -1 * maxValue;
      this.numBits = numBits + 1;
    } else {
      this.minValue = 0;
      this.numBits = numBits;
    }
    uncompressedSize = SizeUtil.BIT_UNPACK_BATCH_SIZE;
    compressedSize = numBits;
    uncompressedData = new int[uncompressedSize];
    compressedData = new int[compressedSize];
  }

  private void createBuffer(File file) throws FileNotFoundException, IOException {
    raf = new RandomAccessFile(file, "rw");
    int bytesRequired = SizeUtil.computeBytesRequired(numRows, numBits, uncompressedSize);
    LOGGER.info("Creating byteBuffer of size:{} to store {} values of bits:{}", bytesRequired,
        numRows, numBits);
    byteBuffer = MmapUtils.mmapFile(raf, FileChannel.MapMode.READ_WRITE, 0, bytesRequired, file,
        this.getClass().getSimpleName() + " byteBuffer");
    isMmap = true;
    ownsByteBuffer = true;
    byteBuffer.position(0);
  }

  public boolean open() {
    return true;
  }

  /**
   * @param row
   * @param col
   * @param val
   */
  public void setInt(int row, int val) {
    try {
      assert val >= minValue && val <= maxValue && row == currentRow + 1;
      int index = row % uncompressedSize;
      uncompressedData[index] = val;
      if (index == uncompressedSize - 1 || row == numRows - 1) {
        BitPacking.fastpack(uncompressedData, 0, compressedData, 0, numBits);
        for (int i = 0; i < compressedSize; i++) {
          byteBuffer.putInt(compressedData[i]);
        }
        int[] out = new int[uncompressedSize];
        BitPacking.fastunpack(compressedData, 0, out, 0, numBits);
        Arrays.fill(uncompressedData, 0);
      }
      currentRow = row;
    } catch (Exception e) {
      LOGGER.error("Failed to set row:{} val:{} ", row, val, e);
      throw e;
    }
  }

  @Override
  public void close() {
    if (ownsByteBuffer) {
      MmapUtils.unloadByteBuffer(byteBuffer);
      byteBuffer = null;

      if (isMmap) {
        IOUtils.closeQuietly(raf);
        raf = null;
      }
    }
  }

  @Override
  public void setChar(int row, char ch) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setShort(int row, short s) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setLong(int row, long l) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setFloat(int row, float f) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setDouble(int row, double d) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setString(int row, String string) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBytes(int row, byte[] bytes) {
    // TODO Auto-generated method stub

  }
}
