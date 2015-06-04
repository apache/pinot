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
package com.linkedin.pinot.index.reader;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.util.CustomBitSet;


public class FixedBitWidthSingleColumnMultiValueReaderTest {
  @Test
  public void testSingleColMultiValue() throws Exception {
    int maxBits = 1;
    while (maxBits < 32) {
      System.out.println("maxBits = " + maxBits);
      final String fileName = getClass().getName() + "_test_single_col_mv_fixed_bit.dat";
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

      Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);
      final int[] readValues = new int[100];

      FixedBitCompressedMVForwardIndexReader heapReader = new FixedBitCompressedMVForwardIndexReader(f, data.length, maxBits, false);
      for (int i = 0; i < data.length; i++) {
        final int numValues = heapReader.getIntArray(i, readValues);
        Assert.assertEquals(numValues, data[i].length);
        for (int j = 0; j < numValues; j++) {
          Assert.assertEquals(readValues[j], data[i][j]);
        }
      }
      Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);
      heapReader.close();
      Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);

      FixedBitCompressedMVForwardIndexReader mmapReader = new FixedBitCompressedMVForwardIndexReader(f, data.length, maxBits, true);
      for (int i = 0; i < data.length; i++) {
        final int numValues = mmapReader.getIntArray(i, readValues);
        Assert.assertEquals(numValues, data[i].length);
        for (int j = 0; j < numValues; j++) {
          Assert.assertEquals(readValues[j], data[i][j]);
        }
      }
      Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 2);
      mmapReader.close();
      Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);

      f.delete();
      maxBits = maxBits + 1;
    }
  }
}
