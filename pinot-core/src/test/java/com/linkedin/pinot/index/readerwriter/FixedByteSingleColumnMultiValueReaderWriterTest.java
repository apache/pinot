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
package com.linkedin.pinot.index.readerwriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;


public class FixedByteSingleColumnMultiValueReaderWriterTest {

  @Test
  public void testIntArray() {
    Random r = new Random();
    final long seed = r.nextLong();
    try {
      testIntArray(seed);
    } catch (Exception e) {
      Assert.fail("Failed with seed " + seed);
    }
  }

  public void testIntArray(final long seed)
      throws IOException {
    FixedByteSingleColumnMultiValueReaderWriter readerWriter;
    int rows = 1000;
    int columnSizeInBytes = Integer.SIZE / 8;
    int maxNumberOfMultiValuesPerRow = 2000;
    readerWriter =
        new FixedByteSingleColumnMultiValueReaderWriter(rows, columnSizeInBytes, maxNumberOfMultiValuesPerRow, 2);

    Random r = new Random(seed);
    int[][] data = new int[rows][];
    for (int i = 0; i < rows; i++) {
      data[i] = new int[r.nextInt(maxNumberOfMultiValuesPerRow)];
      for (int j = 0; j < data[i].length; j++) {
        data[i][j] = r.nextInt();
      }
      readerWriter.setIntArray(i, data[i]);
    }
    int[] ret = new int[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getIntArray(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed="+seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed="+seed);
    }
    readerWriter.close();
  }
}
