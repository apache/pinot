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

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.reader.impl.FixedBitSkipListSCMVReader;
import com.linkedin.pinot.core.index.writer.impl.FixedBitSkipListSCMVWriter;

public class FixedBitSkipListSCMVReaderTest {

  @Test
  public void testSingleColMultiValue() throws Exception {
    int maxBits = 1;
    while (maxBits < 32) {
      final String fileName = getClass().getName() + "_test_single_col_mv_fixed_bit.dat";
      final File f = new File(fileName);
      f.delete();
      int numDocs = 10;
      int maxNumValues = 100;
      final int[][] data = new int[numDocs][];
      final Random r = new Random();
      final int maxValue = (int) Math.pow(2, maxBits);
      int totalNumValues = 0;
      int[] startOffsets = new int[numDocs];
      int[] lengths = new int[numDocs];
      for (int i = 0; i < data.length; i++) {
        final int numValues = r.nextInt(maxNumValues) + 1;
        data[i] = new int[numValues];
        for (int j = 0; j < numValues; j++) {
          data[i][j] = r.nextInt(maxValue);
        }
        startOffsets[i] = totalNumValues;
        lengths[i] = numValues;
        totalNumValues = totalNumValues + numValues;
      }
      System.out.println(Arrays.toString(startOffsets));
      System.out.println(Arrays.toString(lengths));
      FixedBitSkipListSCMVWriter writer =
          new FixedBitSkipListSCMVWriter(f, numDocs, totalNumValues, maxBits);

      for (int i = 0; i < data.length; i++) {
        writer.setIntArray(i, data[i]);
      }
      writer.close();

      final RandomAccessFile raf = new RandomAccessFile(f, "rw");
      System.out.println("file size: " + raf.getChannel().size());
      FixedBitSkipListSCMVReader reader;
      reader = new FixedBitSkipListSCMVReader(f, numDocs, totalNumValues, maxBits, false, false);
      final int[] readValues = new int[maxNumValues];
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
