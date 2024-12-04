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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FixedByteMVMutableForwardIndexTest implements PinotBuffersAfterClassCheckRule {
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(FixedByteMVMutableForwardIndexTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testIntArray() {
    Random r = new Random();
    final long seed = r.nextLong();
    try {
      testIntArray(seed, true);
      testIntArray(seed, false);
      testWithZeroSize(seed, true);
      testWithZeroSize(seed, false);
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail("Failed with seed " + seed);
    }
    for (int mvs = 10; mvs < 1000; mvs += 10) {
      try {
        testIntArrayFixedSize(mvs, seed, true);
        testIntArrayFixedSize(mvs, seed, false);
      } catch (Throwable e) {
        e.printStackTrace();
        Assert.fail("Failed with seed " + seed + ", mvs " + mvs);
      }
    }
  }

  public void testIntArray(final long seed, boolean isDictionaryEncoded)
      throws IOException {
    FixedByteMVMutableForwardIndex readerWriter;
    int rows = 1000;
    int columnSizeInBytes = Integer.BYTES;
    int maxNumberOfMultiValuesPerRow = 2000;
    readerWriter =
        new FixedByteMVMutableForwardIndex(maxNumberOfMultiValuesPerRow, 2, rows / 2, columnSizeInBytes, _memoryManager,
            "IntArray", isDictionaryEncoded, FieldSpec.DataType.INT);

    Random r = new Random(seed);
    int[][] data = new int[rows][];
    for (int i = 0; i < rows; i++) {
      data[i] = new int[r.nextInt(maxNumberOfMultiValuesPerRow)];
      for (int j = 0; j < data[i].length; j++) {
        data[i][j] = r.nextInt();
      }
      readerWriter.setIntMV(i, data[i]);
    }
    int[] ret = new int[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getIntMV(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed=" + seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();
  }

  public void testIntArrayFixedSize(int multiValuesPerRow, long seed, boolean isDictionaryEncoded)
      throws IOException {
    FixedByteMVMutableForwardIndex readerWriter;
    int rows = 1000;
    int columnSizeInBytes = Integer.BYTES;
    // Keep the rowsPerChunk as a multiple of multiValuesPerRow to check the cases when both data and header buffers
    // transition to new ones
    readerWriter = new FixedByteMVMutableForwardIndex(multiValuesPerRow, multiValuesPerRow, multiValuesPerRow * 2,
        columnSizeInBytes, _memoryManager, "IntArrayFixedSize", isDictionaryEncoded, FieldSpec.DataType.INT);

    Random r = new Random(seed);
    int[][] data = new int[rows][];
    for (int i = 0; i < rows; i++) {
      data[i] = new int[multiValuesPerRow];
      for (int j = 0; j < data[i].length; j++) {
        data[i][j] = r.nextInt();
      }
      readerWriter.setIntMV(i, data[i]);
    }
    int[] ret = new int[multiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getIntMV(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed=" + seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();
  }

  public void testWithZeroSize(long seed, boolean isDictionaryEncoded)
      throws IOException {
    FixedByteMVMutableForwardIndex readerWriter;
    final int maxNumberOfMultiValuesPerRow = 5;
    int rows = 1000;
    int columnSizeInBytes = Integer.BYTES;
    Random r = new Random(seed);
    readerWriter =
        new FixedByteMVMutableForwardIndex(maxNumberOfMultiValuesPerRow, 3, r.nextInt(rows) + 1, columnSizeInBytes,
            _memoryManager, "ZeroSize", isDictionaryEncoded, FieldSpec.DataType.INT);

    int[][] data = new int[rows][];
    for (int i = 0; i < rows; i++) {
      if (r.nextInt() > 0) {
        data[i] = new int[r.nextInt(maxNumberOfMultiValuesPerRow)];
        for (int j = 0; j < data[i].length; j++) {
          data[i][j] = r.nextInt();
        }
        readerWriter.setIntMV(i, data[i]);
      } else {
        data[i] = new int[0];
        readerWriter.setIntMV(i, data[i]);
      }
    }
    int[] ret = new int[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getIntMV(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed=" + seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();
  }

  private FixedByteMVMutableForwardIndex createReaderWriter(FieldSpec.DataType dataType, Random r, int rows,
      int maxNumberOfMultiValuesPerRow, boolean isDictionaryEncoded) {
    final int avgMultiValueCount = r.nextInt(maxNumberOfMultiValuesPerRow) + 1;
    final int rowCountPerChunk = r.nextInt(rows) + 1;

    return new FixedByteMVMutableForwardIndex(maxNumberOfMultiValuesPerRow, avgMultiValueCount, rowCountPerChunk,
        dataType.size(), _memoryManager, "ReaderWriter", isDictionaryEncoded, dataType);
  }

  private long generateSeed() {
    Random r = new Random();
    return r.nextLong();
  }

  @Test
  public void testLongArray()
      throws IOException {
    testLongArray(true);
    testLongArray(false);
  }

  private void testLongArray(boolean isDictionaryEncoded)
      throws IOException {
    final long seed = generateSeed();
    Random r = new Random(seed);
    int rows = 1000;
    final int maxNumberOfMultiValuesPerRow = r.nextInt(100) + 1;
    FixedByteMVMutableForwardIndex readerWriter =
        createReaderWriter(FieldSpec.DataType.LONG, r, rows, maxNumberOfMultiValuesPerRow, isDictionaryEncoded);

    long[][] data = new long[rows][];
    for (int i = 0; i < rows; i++) {
      if (r.nextInt() > 0) {
        data[i] = new long[r.nextInt(maxNumberOfMultiValuesPerRow)];
        for (int j = 0; j < data[i].length; j++) {
          data[i][j] = r.nextLong();
        }
        readerWriter.setLongMV(i, data[i]);
      } else {
        data[i] = new long[0];
        readerWriter.setLongMV(i, data[i]);
      }
    }
    long[] ret = new long[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getLongMV(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed=" + seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();
  }

  @Test
  public void testFloatArray()
      throws IOException {
    testFloatArray(true);
    testFloatArray(false);
  }

  private void testFloatArray(boolean isDictoinaryEncoded)
      throws IOException {
    final long seed = generateSeed();
    Random r = new Random(seed);
    int rows = 1000;
    final int maxNumberOfMultiValuesPerRow = r.nextInt(100) + 1;
    FixedByteMVMutableForwardIndex readerWriter =
        createReaderWriter(FieldSpec.DataType.FLOAT, r, rows, maxNumberOfMultiValuesPerRow, isDictoinaryEncoded);

    float[][] data = new float[rows][];
    for (int i = 0; i < rows; i++) {
      if (r.nextInt() > 0) {
        data[i] = new float[r.nextInt(maxNumberOfMultiValuesPerRow)];
        for (int j = 0; j < data[i].length; j++) {
          data[i][j] = r.nextFloat();
        }
        readerWriter.setFloatMV(i, data[i]);
      } else {
        data[i] = new float[0];
        readerWriter.setFloatMV(i, data[i]);
      }
    }
    float[] ret = new float[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getFloatMV(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed=" + seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();
  }

  @Test
  public void testDoubleArray()
      throws IOException {
    testDoubleArray(true);
    testDoubleArray(false);
  }

  private void testDoubleArray(boolean isDictonaryEncoded)
      throws IOException {
    final long seed = generateSeed();
    Random r = new Random(seed);
    int rows = 1000;
    final int maxNumberOfMultiValuesPerRow = r.nextInt(100) + 1;
    FixedByteMVMutableForwardIndex readerWriter =
        createReaderWriter(FieldSpec.DataType.DOUBLE, r, rows, maxNumberOfMultiValuesPerRow, isDictonaryEncoded);

    double[][] data = new double[rows][];
    for (int i = 0; i < rows; i++) {
      if (r.nextInt() > 0) {
        data[i] = new double[r.nextInt(maxNumberOfMultiValuesPerRow)];
        for (int j = 0; j < data[i].length; j++) {
          data[i][j] = r.nextDouble();
        }
        readerWriter.setDoubleMV(i, data[i]);
      } else {
        data[i] = new double[0];
        readerWriter.setDoubleMV(i, data[i]);
      }
    }
    double[] ret = new double[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getDoubleMV(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed=" + seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();
  }
}
