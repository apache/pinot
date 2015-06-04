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

import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthSingleColumnMultiValueReader;


public class FixedByteWidthSingleColumnMultiValueReaderTest {

  @Test
  public void testSingleColMultiValue() throws Exception {
    String fileName = "test_single_col.dat";
    File f = new File(fileName);
    f.delete();
    DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));
    int[][] data = new int[100][];
    Random r = new Random();
    for (int i = 0; i < data.length; i++) {
      int numValues = r.nextInt(100) + 1;
      data[i] = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        data[i][j] = r.nextInt();
      }
    }
    int cumValues = 0;
    for (int i = 0; i < data.length; i++) {
      dos.writeInt(cumValues);
      dos.writeInt(data[i].length);
      cumValues += data[i].length;
    }
    for (int i = 0; i < data.length; i++) {
      int numValues = data[i].length;
      for (int j = 0; j < numValues; j++) {
        dos.writeInt(data[i][j]);
      }
    }
    dos.flush();
    dos.close();

    int[] readValues = new int[100];

    FixedByteWidthSingleColumnMultiValueReader heapReader = new FixedByteWidthSingleColumnMultiValueReader(f, data.length, 4, false);
    for (int i = 0; i < data.length; i++) {
      int numValues = heapReader.getIntArray(i, readValues);
      Assert.assertEquals(numValues, data[i].length);
      for (int j = 0; j < numValues; j++) {
        Assert.assertEquals(readValues[j], data[i][j]);
      }
    }
    Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);
    heapReader.close();
    Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);

    FixedByteWidthSingleColumnMultiValueReader mmapReader = new FixedByteWidthSingleColumnMultiValueReader(f, data.length, 4, true);
    for (int i = 0; i < data.length; i++) {
      int numValues = mmapReader.getIntArray(i, readValues);
      Assert.assertEquals(numValues, data[i].length);
      for (int j = 0; j < numValues; j++) {
        Assert.assertEquals(readValues[j], data[i][j]);
      }
    }
    Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 2);
    mmapReader.close();
    Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(f), 0);

    f.delete();
  }
}
