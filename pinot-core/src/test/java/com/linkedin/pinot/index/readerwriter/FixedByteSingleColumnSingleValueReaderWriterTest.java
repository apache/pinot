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

import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FixedByteSingleColumnSingleValueReaderWriterTest {
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(FixedByteSingleColumnSingleValueReaderWriterTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testInt()
      throws IOException {
    Random r = new Random();
    final long seed = r.nextLong();
    r = new Random(seed);
    int rows = 10;
    for (int div = 1; div <= rows / 2; div++) {
      try {
        testInt(r, rows, div);
      } catch (Throwable t) {
        t.printStackTrace();
        Assert.fail("Failed with seed " + seed);
      }
    }
  }

  private void testInt(final Random random, final int rows, final int div)
      throws IOException {
    FixedByteSingleColumnSingleValueReaderWriter readerWriter;
    final int columnSizesInBytes = Integer.SIZE / 8;
    readerWriter =
        new FixedByteSingleColumnSingleValueReaderWriter(rows / div, columnSizesInBytes, _memoryManager, "Int");
    int[] data = new int[rows];
    for (int i = 0; i < rows; i++) {
      data[i] = random.nextInt();
      readerWriter.setInt(i, data[i]);
    }

    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(readerWriter.getInt(i), data[i]);
    }

    // Test mutability by over-writing all rows.
    for (int i = 0; i < rows; i++) {
      data[i] = random.nextInt();
      readerWriter.setInt(i, data[i]);
    }

    int[] rowIds = new int[rows];
    for (int i = 0; i < rows; i++) {
      rowIds[i] = i;
    }
    int[] values = new int[rows];
    Arrays.fill(values, 0);

    int vStart = 2;
    int dStart = 3;
    int numValues = 4;
    readerWriter.readValues(rowIds, dStart, numValues, values, vStart);
    for (int i = 0; i < numValues; i++) {
      Assert.assertEquals(values[i + vStart], data[i + dStart]);
    }

    readerWriter.readValues(rowIds, 0, 0, values, 0);
    Assert.assertEquals(values[0], 0);

    // Write to a large enough row index to ensure multiple chunks are correctly allocated.
    int start = rows * 4;
    for (int i = 0; i < rows; i++) {
      data[i] = random.nextInt();
      readerWriter.setInt(start + i, data[i]);
    }

    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(readerWriter.getInt(start + i), data[i]);
    }

    // Ensure that rows not written default to zero.
    start = rows * 2;
    for (int i = 0; i < 2 * rows; i++) {
      Assert.assertEquals(readerWriter.getInt(start+i), 0);
    }
    readerWriter.close();
  }

  @Test
  public void testLong()
      throws IOException {
    int rows = 10;
    Random r = new Random();
    final long seed = r.nextLong();
    r = new Random(seed);
    for (int div = 1; div <= rows / 2; div++) {
      testLong(r, rows, div);
    }
  }

  private void testLong(final Random random, final int rows, final int div)
      throws IOException {
    FixedByteSingleColumnSingleValueReaderWriter readerWriter;
    final int columnSizesInBytes = Long.SIZE / 8;
    readerWriter =
        new FixedByteSingleColumnSingleValueReaderWriter(rows / div, columnSizesInBytes, _memoryManager, "Long");
    long[] data = new long[rows];

    for (int i = 0; i < rows; i++) {
      data[i] = random.nextLong();
      readerWriter.setLong(i, data[i]);
    }
    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(readerWriter.getLong(i), data[i]);
    }

    // Test mutability by overwriting randomly selected rows.
    for (int i = 0; i < rows; i++) {
      if (random.nextFloat() >= 0.5) {
        data[i] = random.nextLong();
        readerWriter.setLong(i, data[i]);
      }
    }
    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(readerWriter.getLong(i), data[i]);
    }

    // Write to a large enough row index to ensure multiple chunks are correctly allocated.
    int start = rows * 4;
    for (int i = 0; i < rows; i++) {
      data[i] = random.nextLong();
      readerWriter.setLong(start + i, data[i]);
    }

    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(readerWriter.getLong(start + i), data[i]);
    }

    // Ensure that rows not written default to zero.
    start = rows * 2;
    for (int i = 0; i < 2 * rows; i++) {
      Assert.assertEquals(readerWriter.getLong(start+i), 0);
    }
    readerWriter.close();
  }
}
