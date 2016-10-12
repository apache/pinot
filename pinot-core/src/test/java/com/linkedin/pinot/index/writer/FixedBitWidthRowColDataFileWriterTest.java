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
package com.linkedin.pinot.index.writer;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.io.writer.impl.FixedBitSingleValueMultiColWriter;
import com.linkedin.pinot.core.util.CustomBitSet;

public class FixedBitWidthRowColDataFileWriterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FixedBitWidthRowColDataFileWriterTest.class);
  @Test
  public void testSingleColUnsigned() throws Exception {
    int maxBits = 1;
    while (maxBits < 32) {
      LOGGER.debug("START test maxBits:" + maxBits);
      final String fileName = getClass().getName() + "_single_col_fixed_bit_"
          + maxBits + ".dat";
      final File file = new File(fileName);
      file.delete();
      final int rows = 100;
      final int cols = 1;
      final int[] columnSizesInBits = new int[] { maxBits };
      final FixedBitSingleValueMultiColWriter writer = new FixedBitSingleValueMultiColWriter(
          file, rows, cols, columnSizesInBits);
      final int[] data = new int[rows];
      final Random r = new Random();
      writer.open();
      final int maxValue = (int) Math.pow(2, maxBits);
      final CustomBitSet set = CustomBitSet
          .withBitLength(rows * cols * maxBits);
      for (int i = 0; i < rows; i++) {
        data[i] = r.nextInt(maxValue);
        writer.setInt(i, 0, data[i]);
        final int value = data[i];
        for (int bitPos = maxBits - 1; bitPos >= 0; bitPos--) {
          if ((value & (1 << bitPos)) != 0) {
            set.setBit(i * maxBits + (maxBits - bitPos - 1));
          }
        }
      }
      LOGGER.debug("bits expected:" + rows * cols * maxBits
          + " bytes expected:" + (rows * cols * maxBits + 7) / 8);

      writer.close();
      final RandomAccessFile raf = new RandomAccessFile(file, "r");
//      System.out.println("file size:" + raf.length());
      final byte[] b = new byte[(int) raf.length()];
      raf.read(b);
      final byte[] byteArray = set.toByteArray();
      if (byteArray.length != raf.length()) {
        final byte[] temp = set.toByteArray();
        System.err.println("byteArray length:" + temp.length + "   from file:"
            + b.length);
      }
      LOGGER.trace("byteArray length:" + byteArray.length
          + "   from file:" + b.length);
      Assert.assertEquals(byteArray.length, b.length);
      Assert.assertEquals(byteArray, b);
      raf.close();
      LOGGER.trace("END test maxBits:" + maxBits);
      maxBits = maxBits + 1;
      file.delete();
      set.close();
    }
  }

  @Test
  public void testSingleColSigned() throws Exception {
    int maxBits = 1;
    while (maxBits < 32) {
      LOGGER.trace("START test maxBits:" + maxBits);
      final String fileName = getClass().getName() + "_single_col_fixed_bit_"
          + maxBits + ".dat";
      final File file = new File(fileName);
      file.delete();
      final int rows = 100;
      final int cols = 1;
      final int[] columnSizesInBits = new int[] { maxBits };
      final FixedBitSingleValueMultiColWriter writer = new FixedBitSingleValueMultiColWriter(
          file, rows, cols, columnSizesInBits, new boolean[] { true });
      final int[] data = new int[rows];
      final Random r = new Random();
      writer.open();
      final int maxValue = (int) Math.pow(2, maxBits);
      int offset = maxValue - 1;
      int requiredBits = maxBits + 1;
      final CustomBitSet set = CustomBitSet.withBitLength(rows * cols
          * requiredBits);
      for (int i = 0; i < rows; i++) {
        int value = r.nextInt(maxValue);
        if (Math.random() > .5) {
          value = -1 * value;
        }
        data[i] = value;
        writer.setInt(i, 0, data[i]);
        int offsetValue = value + offset;
        for (int bitPos = requiredBits - 1; bitPos >= 0; bitPos--) {
          if ((offsetValue & (1 << bitPos)) != 0) {
            set.setBit(i * requiredBits + (requiredBits - bitPos - 1));
          }
        }
      }
      LOGGER.trace("bits expected:" + rows * cols * maxBits
          + " bytes expected:" + (rows * cols * maxBits + 7) / 8);

      writer.close();
      final RandomAccessFile raf = new RandomAccessFile(file, "r");
      LOGGER.trace("file size:" + raf.length());
      final byte[] b = new byte[(int) raf.length()];
      raf.read(b);
      final byte[] byteArray = set.toByteArray();
      if (byteArray.length != raf.length()) {
        final byte[] temp = set.toByteArray();
        System.err.println("byteArray length:" + temp.length + "   from file:"
            + b.length);
      }
      LOGGER.trace("byteArray length:" + byteArray.length
          + "   from file:" + b.length);
      Assert.assertEquals(byteArray.length, b.length);
      Assert.assertEquals(byteArray, b);
      raf.close();
      LOGGER.trace("END test maxBits:" + maxBits);
      maxBits = maxBits + 1;
      file.delete();
      set.close();
    }
  }
}
