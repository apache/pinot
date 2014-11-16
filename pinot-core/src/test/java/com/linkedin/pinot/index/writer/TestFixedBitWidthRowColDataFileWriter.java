package com.linkedin.pinot.index.writer;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.writer.impl.FixedBitWidthRowColDataFileWriter;
import com.linkedin.pinot.core.util.CustomBitSet;

public class TestFixedBitWidthRowColDataFileWriter {

  @Test
  public void testSingleCol() throws Exception {
    int maxBits = 1;
    while (maxBits < 32) {
      System.out.println("START test maxBits:" + maxBits);
      final String fileName = getClass().getName()+ "_single_col_fixed_bit_" + maxBits + ".dat";
      final File file = new File(fileName);
      file.delete();
      final int rows = 100;
      final int cols = 1;
      final int[] columnSizesInBits = new int[] { maxBits };
      final FixedBitWidthRowColDataFileWriter writer = new FixedBitWidthRowColDataFileWriter(
          file, rows, cols, columnSizesInBits);
      final int[] data = new int[rows];
      final Random r = new Random();
      writer.open();
      final int maxValue = (int) Math.pow(2, maxBits);
      final CustomBitSet set = CustomBitSet.withBitLength(rows * cols * maxBits);
      for (int i = 0; i < rows; i++) {
        data[i] = r.nextInt(maxValue);
        writer.setInt(i, 0, data[i]);
        final int value = data[i];
        for (int bitPos = maxBits - 1; bitPos >= 0; bitPos--) {
          if (( value & (1 << bitPos)) != 0) {
            set.setBit(i * maxBits + (maxBits - bitPos - 1));
          }
        }
      }
      System.out.println("bits expected:" + rows * cols * maxBits
          + " bytes expected:" + (rows * cols * maxBits + 7) / 8);

      writer.saveAndClose();
      final RandomAccessFile raf = new RandomAccessFile(file, "r");
      System.out.println("file size:" + raf.length());
      final byte[] b = new byte[(int) raf.length()];
      raf.read(b);
      final byte[] byteArray = set.toByteArray();
      if (byteArray.length != raf.length()) {
        final byte[] temp = set.toByteArray();
        System.err.println("byteArray length:" + temp.length
            + "   from file:" + b.length);
      }
      System.out.println("byteArray length:" + byteArray.length
          + "   from file:" + b.length);
      Assert.assertEquals(byteArray.length, b.length);
      Assert.assertEquals(byteArray, b);
      raf.close();
      System.out.println("END test maxBits:" + maxBits);
      maxBits = maxBits + 1;
      file.delete();
    }
  }
}
