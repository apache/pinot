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
import java.util.Arrays;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.reader.impl.FixedByteSkipListSCMVReader;
import com.linkedin.pinot.core.index.writer.impl.FixedByteSkipListSCMVWriter;


public class FixedByteSkipListSCMVReaderTest {

  @Test
  public void testSingleColMultiValue() throws Exception {
    String fileName = "test_single_col.dat";
    File f = new File(fileName);
    f.delete();
    int numDocs = 100;
    int maxMultiValues = 100;
    int[][] data = new int[numDocs][];
    Random r = new Random();
    int totalNumValues = 0;
    int[] startOffsets = new int[numDocs];
    int[] lengths = new int[numDocs];

    for (int i = 0; i < data.length; i++) {
      int numValues = r.nextInt(maxMultiValues) + 1;
      data[i] = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        data[i][j] = r.nextInt();
      }
      startOffsets[i] = totalNumValues;
      lengths[i] = numValues;
      totalNumValues = totalNumValues + numValues;
    }

    for (int i = 0; i < data.length; i++) {
      System.out.print("(" + i + "," + startOffsets[i] + "," + lengths[i] + ")");
      System.out.println(",");
    }
    System.out.println(Arrays.toString(startOffsets));
    System.out.println(Arrays.toString(lengths));
    FixedByteSkipListSCMVWriter writer = new FixedByteSkipListSCMVWriter(f, numDocs, totalNumValues, Integer.SIZE / 8);

    for (int i = 0; i < data.length; i++) {
      writer.setIntArray(i, data[i]);
    }
    writer.close();

    int[] readValues = new int[maxMultiValues];
    FixedByteSkipListSCMVReader heapReader = new FixedByteSkipListSCMVReader(f, numDocs, totalNumValues, Integer.SIZE / 8, false);
    for (int i = 0; i < data.length; i++) {
      int numValues = heapReader.getIntArray(i, readValues);
      Assert.assertEquals(numValues, data[i].length);
      for (int j = 0; j < numValues; j++) {
        Assert.assertEquals(readValues[j], data[i][j], "");
      }
    }
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);
    heapReader.close();
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);

    FixedByteSkipListSCMVReader mmapReader = new FixedByteSkipListSCMVReader(f, numDocs, totalNumValues, Integer.SIZE / 8, true);
    for (int i = 0; i < data.length; i++) {
      int numValues = mmapReader.getIntArray(i, readValues);
      Assert.assertEquals(numValues, data[i].length);
      for (int j = 0; j < numValues; j++) {
        Assert.assertEquals(readValues[j], data[i][j], "");
      }
    }
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 2);
    mmapReader.close();
    // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);

    f.delete();
  }
}
