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
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;


public class FixedByteSingleColumnMultiValueReaderWriterTest {
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(FixedByteSingleColumnMultiValueReaderWriterTest.class.getName());
  }

  @AfterClass
  public void tearDown() throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testIntArray() {
    Random r = new Random();
    final long seed = r.nextLong();
    try {
      testIntArray(seed);
      testWithZeroSize(seed);
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail("Failed with seed " + seed);
    }
    for (int mvs = 10; mvs < 1000; mvs += 10) {
      try {
        testIntArrayFixedSize(mvs, seed);
      } catch (Throwable e) {
        e.printStackTrace();
        Assert.fail("Failed with seed " + seed + ", mvs " + mvs);
      }
    }
  }

  public void testIntArray(final long seed)
      throws IOException {
    FixedByteSingleColumnMultiValueReaderWriter readerWriter;
    int rows = 1000;
    int columnSizeInBytes = Integer.SIZE / 8;
    int maxNumberOfMultiValuesPerRow = 2000;
    readerWriter =
        new FixedByteSingleColumnMultiValueReaderWriter(maxNumberOfMultiValuesPerRow, 2, rows/2, columnSizeInBytes, _memoryManager, "IntArray");

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

  public void testIntArrayFixedSize(int multiValuesPerRow, long seed)
      throws IOException {
    FixedByteSingleColumnMultiValueReaderWriter readerWriter;
    int rows = 1000;
    int columnSizeInBytes = Integer.SIZE / 8;
    // Keep the rowsPerChunk as a multiple of multiValuesPerRow to check the cases when both data and header buffers
    // transition to new ones
    readerWriter =
        new FixedByteSingleColumnMultiValueReaderWriter(multiValuesPerRow, multiValuesPerRow, multiValuesPerRow * 2, columnSizeInBytes,
            _memoryManager, "IntArrayFixedSize");

    Random r = new Random(seed);
    int[][] data = new int[rows][];
    for (int i = 0; i < rows; i++) {
      data[i] = new int[multiValuesPerRow];
      for (int j = 0; j < data[i].length; j++) {
        data[i][j] = r.nextInt();
      }
      readerWriter.setIntArray(i, data[i]);
    }
    int[] ret = new int[multiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getIntArray(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed="+seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed="+seed);
    }
    readerWriter.close();
  }

  public void testWithZeroSize(long seed) {
    FixedByteSingleColumnMultiValueReaderWriter readerWriter;
    final int maxNumberOfMultiValuesPerRow = 5;
    int rows = 1000;
    int columnSizeInBytes = Integer.SIZE / 8;
    Random r = new Random(seed);
    readerWriter =
        new FixedByteSingleColumnMultiValueReaderWriter(maxNumberOfMultiValuesPerRow, 3, r.nextInt(rows) + 1, columnSizeInBytes,
            _memoryManager, "ZeroSize");

    int[][] data = new int[rows][];
    for (int i = 0; i < rows; i++) {
      if (r.nextInt() > 0) {
        data[i] = new int[r.nextInt(maxNumberOfMultiValuesPerRow)];
        for (int j = 0; j < data[i].length; j++) {
          data[i][j] = r.nextInt();
        }
        readerWriter.setIntArray(i, data[i]);
      } else {
        data[i] = new int[0];
        readerWriter.setIntArray(i, data[i]);
      }
    }
    int[] ret = new int[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getIntArray(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed="+seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();

  }

  private FixedByteSingleColumnMultiValueReaderWriter createReaderWriter(FieldSpec.DataType type, Random r, int rows, int maxNumberOfMultiValuesPerRow) {
    final int columnSize = sizeForType(type);
    final int avgMultiValueCount = r.nextInt(maxNumberOfMultiValuesPerRow) + 1;
    final int rowCountPerChunk = r.nextInt(rows) + 1;

    return new FixedByteSingleColumnMultiValueReaderWriter(maxNumberOfMultiValuesPerRow, avgMultiValueCount,
        rowCountPerChunk, columnSize, _memoryManager, "ReaderWriter");
  }

  private int sizeForType(FieldSpec.DataType type) {
    int size;
    switch (type) {
      case SHORT_ARRAY:
        size = Short.SIZE/8;
        break;
      case LONG_ARRAY:
        size = Long.SIZE/8;
        break;
      case FLOAT_ARRAY:
        size = Float.SIZE/8;
        break;
      case DOUBLE_ARRAY:
        size = Double.SIZE/8;
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return size;
  }

  private long generateSeed() {
    Random r = new Random();
    return r.nextLong();
  }

  @Test
  public void testLongArray() throws Exception {
    final long seed = generateSeed();
    Random r = new Random(seed);
    int rows = 1000;
    final int maxNumberOfMultiValuesPerRow = r.nextInt(100) + 1;
    FixedByteSingleColumnMultiValueReaderWriter readerWriter = createReaderWriter(FieldSpec.DataType.LONG_ARRAY, r, rows, maxNumberOfMultiValuesPerRow);

    long[][] data = new long[rows][];
    for (int i = 0; i < rows; i++) {
      if (r.nextInt() > 0) {
        data[i] = new long[r.nextInt(maxNumberOfMultiValuesPerRow)];
        for (int j = 0; j < data[i].length; j++) {
          data[i][j] = r.nextLong();
        }
        readerWriter.setLongArray(i, data[i]);
      } else {
        data[i] = new long[0];
        readerWriter.setLongArray(i, data[i]);
      }
    }
    long[] ret = new long[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getLongArray(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed="+seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();
  }
  @Test
  public void testFloatArray() throws Exception {
    final long seed = generateSeed();
    Random r = new Random(seed);
    int rows = 1000;
    final int maxNumberOfMultiValuesPerRow = r.nextInt(100) + 1;
    FixedByteSingleColumnMultiValueReaderWriter readerWriter = createReaderWriter(FieldSpec.DataType.FLOAT_ARRAY, r, rows, maxNumberOfMultiValuesPerRow);

    float[][] data = new float[rows][];
    for (int i = 0; i < rows; i++) {
      if (r.nextInt() > 0) {
        data[i] = new float[r.nextInt(maxNumberOfMultiValuesPerRow)];
        for (int j = 0; j < data[i].length; j++) {
          data[i][j] = r.nextFloat();
        }
        readerWriter.setFloatArray(i, data[i]);
      } else {
        data[i] = new float[0];
        readerWriter.setFloatArray(i, data[i]);
      }
    }
    float[] ret = new float[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getFloatArray(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed="+seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();
  }

  @Test
  public void testDoubleArray() throws Exception {
    final long seed = generateSeed();
    Random r = new Random(seed);
    int rows = 1000;
    final int maxNumberOfMultiValuesPerRow = r.nextInt(100) + 1;
    FixedByteSingleColumnMultiValueReaderWriter readerWriter = createReaderWriter(FieldSpec.DataType.DOUBLE_ARRAY, r, rows, maxNumberOfMultiValuesPerRow);

    double[][] data = new double[rows][];
    for (int i = 0; i < rows; i++) {
      if (r.nextInt() > 0) {
        data[i] = new double[r.nextInt(maxNumberOfMultiValuesPerRow)];
        for (int j = 0; j < data[i].length; j++) {
          data[i][j] = r.nextDouble();
        }
        readerWriter.setDoubleArray(i, data[i]);
      } else {
        data[i] = new double[0];
        readerWriter.setDoubleArray(i, data[i]);
      }
    }
    double[] ret = new double[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getDoubleArray(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed="+seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();
  }

  @Test
  public void testShortArray() throws Exception {
    final long seed = generateSeed();
    Random r = new Random(seed);
    int rows = 1000;
    final int maxNumberOfMultiValuesPerRow = r.nextInt(100) + 1;
    FixedByteSingleColumnMultiValueReaderWriter readerWriter = createReaderWriter(FieldSpec.DataType.SHORT_ARRAY, r, rows, maxNumberOfMultiValuesPerRow);

    short[][] data = new short[rows][];
    for (int i = 0; i < rows; i++) {
      if (r.nextInt() > 0) {
        data[i] = new short[r.nextInt(maxNumberOfMultiValuesPerRow)];
        for (int j = 0; j < data[i].length; j++) {
          data[i][j] = (short)r.nextInt(Short.MAX_VALUE);
        }
        readerWriter.setShortArray(i, data[i]);
      } else {
        data[i] = new short[0];
        readerWriter.setShortArray(i, data[i]);
      }
    }
    short[] ret = new short[maxNumberOfMultiValuesPerRow];
    for (int i = 0; i < rows; i++) {
      int length = readerWriter.getShortArray(i, ret);
      Assert.assertEquals(data[i].length, length, "Failed with seed="+seed);
      Assert.assertTrue(Arrays.equals(data[i], Arrays.copyOf(ret, length)), "Failed with seed=" + seed);
    }
    readerWriter.close();
  }
}
