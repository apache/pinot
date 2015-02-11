package com.linkedin.pinot.index.reader;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.util.CustomBitSet;

public class TestFixedBitWidthSingleColumnMultiValueReader {

  @Test
  public void testSingleColMultiValue() throws Exception {
    int maxBits = 1;
    while (maxBits < 32) {
      final String fileName = getClass().getName()
          + "_test_single_col_mv_fixed_bit.dat";
      final File f = new File(fileName);
      f.delete();
      final DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));
      final int[][] data = new int[100][];
      final Random r = new Random();
      final int maxValue = (int) Math.pow(2, maxBits);
      // generate the
      for (int i = 0; i < data.length; i++) {
        final int numValues = r.nextInt(100) + 1;
        data[i] = new int[numValues];
        for (int j = 0; j < numValues; j++) {
          data[i][j] = r.nextInt(maxValue);
        }
      }
      int cumValues = 0;
      // write the header section
      for (final int[] element : data) {
        dos.writeInt(cumValues);
        dos.writeInt(element.length);
        cumValues += element.length;
      }
      // write the data section
      final CustomBitSet bitSet = CustomBitSet.withBitLength(cumValues
          * maxBits);
      int index = 0;
      for (final int[] element : data) {
        final int numValues = element.length;
        for (int j = 0; j < numValues; j++) {
          final int value = element[j];
          for (int bitPos = maxBits - 1; bitPos >= 0; bitPos--) {
            if ((value & (1 << bitPos)) != 0) {
              bitSet.setBit(index * maxBits + (maxBits - bitPos - 1));
            }
          }
          index = index + 1;
        }
      }
      dos.write(bitSet.toByteArray());
      dos.flush();
      dos.close();
      final RandomAccessFile raf = new RandomAccessFile(f, "rw");
      System.out.println("file size: " + raf.getChannel().size());
      FixedBitCompressedMVForwardIndexReader reader;
      reader = new FixedBitCompressedMVForwardIndexReader(f, data.length,
          maxBits, true);
      reader.open();
      final int[] readValues = new int[100];
      for (int i = 0; i < data.length; i++) {
        final int numValues = reader.getIntArray(i, readValues);
        Assert.assertEquals(numValues, data[i].length);
        for (int j = 0; j < numValues; j++) {
          Assert.assertEquals(readValues[j], data[i][j]);
        }
      }
      reader.close();
      raf.close();
      f.delete();
      maxBits = maxBits + 1;
    }
  }
}
