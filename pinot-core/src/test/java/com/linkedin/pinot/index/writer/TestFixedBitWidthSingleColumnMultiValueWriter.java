package com.linkedin.pinot.index.writer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.core.index.writer.impl.FixedBitWidthSingleColumnMultiValueWriter;
import com.linkedin.pinot.core.util.CustomBitSet;

public class TestFixedBitWidthSingleColumnMultiValueWriter {
  @Test
  public void testSingleColMultiValue() throws Exception {
    int maxBits = 2;
    while (maxBits < 32) {
      System.out.println("START test maxBit:" + maxBits);
      File file = new File("test_single_col_multi_value_writer.dat");
      file.delete();
      int rows = 100;
      int[][] data = new int[rows][];
      int maxValue = (int) Math.pow(2, maxBits);
      Random r = new Random();
      int totalNumValues = 0;
      for (int i = 0; i < rows; i++) {
        int numValues = r.nextInt(100) + 1;
        data[i] = new int[numValues];
        for (int j = 0; j < numValues; j++) {
          data[i][j] = r.nextInt(maxValue);
        }
        totalNumValues += numValues;
      }
      FixedBitWidthSingleColumnMultiValueWriter writer = new FixedBitWidthSingleColumnMultiValueWriter(
          file, rows, totalNumValues, maxBits);
      CustomBitSet bitSet = CustomBitSet
          .withBitLength(totalNumValues * maxBits);
      int index = 0;
      for (int i = 0; i < rows; i++) {
        writer.setIntArray(i, data[i]);
        for (int j = 0; j < data[i].length; j++) {
          int value = data[i][j];
          for (int bitPos = maxBits - 1; bitPos >= 0; bitPos--) {
            if ((value & (1 << bitPos)) != 0) {
              bitSet.setBit(index * maxBits + (maxBits - bitPos - 1));
            }
          }
          index = index + 1;
        }
      }
      writer.close();
      // verify header
      DataInputStream dis = new DataInputStream(new FileInputStream(file));
      int totalLength = 0;
      for (int i = 0; i < rows; i++) {
        Assert.assertEquals(dis.readInt(), totalLength);
        Assert.assertEquals(dis.readInt(), data[i].length);
        totalLength += data[i].length;
      }
      dis.close();

      // verify data
      byte[] byteArray = bitSet.toByteArray();
      RandomAccessFile raf = new RandomAccessFile(file, "r");
      // Header contains 1 row for each doc and each row contains 2 ints
      int headerSize = rows * 2 * 4;
      int dataLength = (int) raf.length() - headerSize;
      byte[] b = new byte[dataLength];
      // read the data segment that starts after the header.
      raf.seek(headerSize);
      raf.read(b, 0, dataLength);
      Assert.assertEquals(byteArray.length, b.length);
      Assert.assertEquals(byteArray, b);
      raf.close();
      file.delete();
      System.out.println("END test maxBit:" + maxBits);
      maxBits = maxBits + 1;
    }

  }
}
