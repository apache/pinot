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

import com.linkedin.pinot.core.io.writer.impl.v1.FixedByteMultiValueWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FixedByteWidthSingleColumnMultiValueWriterTest {
  @Test
  public void testSingleColMultiValue() throws Exception {

    File file = new File("test_single_col_multi_value_writer.dat");
    file.delete();
    int rows = 100;
    int[][] data = new int[rows][];
    Random r = new Random();
    int totalNumValues = 0;
    for (int i = 0; i < rows; i++) {
      int numValues = r.nextInt(100) + 1;
      data[i] = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        data[i][j] = r.nextInt();
      }
      totalNumValues += numValues;
    }

    FixedByteMultiValueWriter writer = new FixedByteMultiValueWriter(
        file, rows, totalNumValues, 4);
    for (int i = 0; i < rows; i++) {
      writer.setIntArray(i, data[i]);
    }
    writer.close();
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    int cumLength = 0;
    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(dis.readInt(), cumLength);
      Assert.assertEquals(dis.readInt(), data[i].length);
      cumLength += data[i].length;
    }
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < data[i].length; j++) {
        Assert.assertEquals(dis.readInt(), data[i][j]);
      }
    }
    dis.close();
    file.delete();
  }


}
