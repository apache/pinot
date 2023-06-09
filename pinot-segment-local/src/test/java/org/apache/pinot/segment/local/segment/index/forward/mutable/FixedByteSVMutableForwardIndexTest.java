/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.forward.mutable;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FixedByteSVMutableForwardIndexTest {
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(FixedByteSVMutableForwardIndexTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testDictId()
      throws IOException {
    Random r = new Random();
    final long seed = r.nextLong();
    r = new Random(seed);
    int rows = 10;
    for (int div = 1; div <= rows / 2; div++) {
      try {
        testDictId(r, rows, div);
      } catch (Throwable t) {
        t.printStackTrace();
        Assert.fail("Failed with seed " + seed);
      }
    }
  }

  private void testDictId(final Random random, final int rows, final int div)
      throws IOException {
    FixedByteSVMutableForwardIndex readerWriter;
    readerWriter = new FixedByteSVMutableForwardIndex(true, DataType.INT, rows / div, _memoryManager, "Int");
    int[] data = new int[rows];
    for (int i = 0; i < rows; i++) {
      data[i] = random.nextInt();
      readerWriter.setDictId(i, data[i]);
    }

    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(readerWriter.getDictId(i), data[i]);
    }

    // Test mutability by over-writing all rows.
    for (int i = 0; i < rows; i++) {
      data[i] = random.nextInt();
      readerWriter.setDictId(i, data[i]);
    }

    int[] rowIds = new int[rows];
    for (int i = 0; i < rows; i++) {
      rowIds[i] = i;
    }
    int[] values = new int[rows];
    Arrays.fill(values, 0);

    readerWriter.readDictIds(rowIds, rows, values);
    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(values[i], data[i]);
    }

    // Write to a large enough row index to ensure multiple chunks are correctly allocated.
    int start = rows * 4;
    for (int i = 0; i < rows; i++) {
      data[i] = random.nextInt();
      readerWriter.setDictId(start + i, data[i]);
    }

    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(readerWriter.getDictId(start + i), data[i]);
    }

    // Ensure that rows not written default to zero.
    start = rows * 2;
    for (int i = 0; i < 2 * rows; i++) {
      Assert.assertEquals(readerWriter.getDictId(start + i), 0);
    }
    readerWriter.close();
  }

  @Test
  public void testBytes() throws IOException {
    int rows = 10;
    Random r = new Random();
    final long seed = r.nextLong();
    r = new Random(seed);
    for (int div = 1; div <= rows / 2; div++) {
      testBytes(r, rows, div);
    }
  }

  private void testBytes(final Random random, final int rows, final int div) throws IOException {
    int hllLog2M12Size = 2740;
    int log2m = 12;

    FixedByteSVMutableForwardIndex readerWriter;
    readerWriter =
        new FixedByteSVMutableForwardIndex(false, DataType.BYTES, hllLog2M12Size, rows / div, _memoryManager, "Long");
    byte[][] data = new byte[rows][];

    for (int i = 0; i < rows; i++) {
      HyperLogLog hll = new HyperLogLog(log2m);
      hll.offer(random.nextLong());
      data[i] = hll.getBytes();
      Assert.assertEquals(data[i].length, hllLog2M12Size);
      readerWriter.setBytes(i, data[i]);
      Assert.assertEquals(readerWriter.getBytes(i).length, data[i].length);
      Assert.assertEquals(readerWriter.getBytes(i), data[i]);
    }
    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(readerWriter.getBytes(i), data[i]);
    }

    // Test mutability by overwriting randomly selected rows.
    for (int i = 0; i < rows; i++) {
      if (random.nextFloat() >= 0.5) {
        HyperLogLog hll = new HyperLogLog(log2m);
        hll.offer(random.nextLong());
        data[i] = hll.getBytes();
        readerWriter.setBytes(i, data[i]);
      }
    }
    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(readerWriter.getBytes(i), data[i]);
    }

    // Write to a large enough row index to ensure multiple chunks are correctly allocated.
    int start = rows * 4;
    for (int i = 0; i < rows; i++) {
      HyperLogLog hll = new HyperLogLog(log2m);
      hll.offer(random.nextLong());
      data[i] = hll.getBytes();
      readerWriter.setBytes(start + i, data[i]);
    }

    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(readerWriter.getBytes(start + i), data[i]);
    }

    // Ensure that rows not written default to an empty byte array.
    byte[] emptyBytes = new byte[hllLog2M12Size];
    start = rows * 2;
    for (int i = 0; i < 2 * rows; i++) {
      byte[] bytes = readerWriter.getBytes(start + i);
      Assert.assertEquals(bytes, emptyBytes);
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
    FixedByteSVMutableForwardIndex readerWriter;
    readerWriter = new FixedByteSVMutableForwardIndex(false, DataType.LONG, rows / div, _memoryManager, "Long");
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
      Assert.assertEquals(readerWriter.getLong(start + i), 0);
    }
    readerWriter.close();
  }
}
