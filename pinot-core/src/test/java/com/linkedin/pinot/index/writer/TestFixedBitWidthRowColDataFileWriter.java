package com.linkedin.pinot.index.writer;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.BitSet;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.reader.impl.FixedBitWidthRowColDataFileReader;
import com.linkedin.pinot.core.index.writer.impl.FixedBitWidthRowColDataFileWriter;

public class TestFixedBitWidthRowColDataFileWriter {

  @Test
  public void testSingleCol() throws Exception {
    final int[] maxBitsArray = new int[] { 5, 10, 15, 25, 31 };
    for (final int maxBits : maxBitsArray) {

      final File file = new File("single_col_fixed_bit_" + maxBits + ".dat");
      final int rows = 1000000;
      final int cols = 1;
      final int[] columnSizesInBits = new int[] { maxBits };
      final FixedBitWidthRowColDataFileWriter writer = new FixedBitWidthRowColDataFileWriter(
          file, rows, cols, columnSizesInBits);
      final int[] data = new int[rows];
      final Random r = new Random();
      writer.open();
      final int maxValue = (int) Math.pow(2, maxBits);
      final BitSet set = new BitSet(rows * cols * maxBits);
      for (int i = 0; i < rows; i++) {
        data[i] = r.nextInt(maxValue);
        writer.setInt(i, 0, data[i]);
        for (int j = 0; j < maxBits; j++) {
          if (((data[i] >> j) & 1) == 1) {
            set.set(i * maxBits + j);
          }
        }
      }
      writer.saveAndClose();
      final RandomAccessFile raf = new RandomAccessFile(file, "r");
      final byte[] b = new byte[(int) raf.length()];
      raf.read(b);
      final byte[] byteArray = set.toByteArray();
      Assert.assertEquals(byteArray.length, b.length);
      Assert.assertEquals(byteArray, b);
      raf.close();

      final FixedBitWidthRowColDataFileReader reader = FixedBitWidthRowColDataFileReader.forMmap(file, rows, 1, columnSizesInBits);

      for (int i = 0; i < rows; i++) {
        System.out.println(reader.getInt(i, 0));
      }
    }
  }
}
