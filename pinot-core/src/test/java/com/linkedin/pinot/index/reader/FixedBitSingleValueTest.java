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

import com.linkedin.pinot.core.io.reader.impl.v2.FixedBitSingleValueReader;
import com.linkedin.pinot.core.io.writer.impl.v2.FixedBitSingleValueWriter;

public class FixedBitSingleValueTest {

  @Test
  public void testV2() throws Exception {
    int ROWS = 1000;
    for (int numBits = 1; numBits < 32; numBits++) {
      File file = new File(this.getClass().getName() + "_" + numBits + ".test");
      FixedBitSingleValueWriter writer = new FixedBitSingleValueWriter(file, ROWS, numBits);
      int data[] = new int[ROWS];
      Random random = new Random();
      int max = (int) Math.pow(2, numBits);
      for (int i = 0; i < ROWS; i++) {
        data[i] = random.nextInt(max);
        writer.setInt(i, data[i]);
      }
      writer.close();
      FixedBitSingleValueReader reader = FixedBitSingleValueReader.forHeap(file, ROWS, numBits);
      int[] read = new int[ROWS];
      for (int i = 0; i < ROWS; i++) {
        read[i] = reader.getInt(i);
        Assert.assertEquals(reader.getInt(i), data[i],
            "Failed for bit:" + numBits + " Expected " + data[i] + " but found " + reader.getInt(i) + "  at " + i);
      }
      System.out.println(Arrays.toString(data));
      System.out.println(Arrays.toString(read));
      reader.close();
      file.delete();
    }
  }
}
