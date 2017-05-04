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
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;


public class FixedByteSingleColumnSingleValueReaderWriterTest {
  @Test
  public void testInt() throws IOException {
    FixedByteSingleColumnSingleValueReaderWriter readerWriter;
    int rows = 10;
    final int columnSizesInBytes = Integer.SIZE / 8;
    readerWriter = new FixedByteSingleColumnSingleValueReaderWriter(rows, columnSizesInBytes);
    Random r = new Random();
    int[] data = new int[rows];
    for (int i = 0; i < rows; i++) {
      data[i] = r.nextInt();
      readerWriter.setInt(i, data[i]);
    }
    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(data[i], readerWriter.getInt(i));
    }
    readerWriter.close();
  }

  @Test
  public void testLong() throws IOException {
    FixedByteSingleColumnSingleValueReaderWriter readerWriter;
    int rows = 10;
    final int columnSizesInBytes = Long.SIZE / 8;
    readerWriter = new FixedByteSingleColumnSingleValueReaderWriter(rows, columnSizesInBytes);
    Random r = new Random();
    long[] data = new long[rows];
    for (int i = 0; i < rows; i++) {
      data[i] = r.nextLong();
      readerWriter.setLong(i, data[i]);
    }
    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(data[i], readerWriter.getLong(i));
    }
    readerWriter.close();
  }
}
