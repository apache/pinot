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
package org.apache.pinot.segment.local.realtime.impl.dictionary;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;


public class MultiValueDictionaryTest implements PinotBuffersAfterClassCheckRule {
  private static final int NROWS = 1000;
  private static final int MAX_N_VALUES = 1000;
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(MultiValueDictionaryTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testMultiValueIndexingWithDictionary() {
    long seed = System.nanoTime();

    try (LongOnHeapMutableDictionary dict = new LongOnHeapMutableDictionary();
        DirectMemoryManager memManager = new DirectMemoryManager("test");
        FixedByteMVMutableForwardIndex indexer = new FixedByteMVMutableForwardIndex(MAX_N_VALUES, MAX_N_VALUES / 2,
            NROWS / 3, Integer.BYTES, memManager, "indexer", true, FieldSpec.DataType.INT)) {
      // Insert rows into the indexer and dictionary
      Random random = new Random(seed);
      for (int row = 0; row < NROWS; row++) {
        int numValues = Math.abs(random.nextInt()) % MAX_N_VALUES;
        Long[] values = new Long[numValues];
        for (int i = 0; i < numValues; i++) {
          values[i] = random.nextLong();
        }
        int[] dictIds = dict.index(values);
        assertEquals(dictIds.length, numValues);
        indexer.setDictIdMV(row, dictIds);
      }

      // Read back rows and make sure that the values are good.
      random = new Random(seed);
      int[] dictIds = new int[MAX_N_VALUES];
      for (int row = 0; row < NROWS; row++) {
        int numValues = indexer.getDictIdMV(row, dictIds);
        assertEquals(numValues, Math.abs(random.nextInt()) % MAX_N_VALUES);

        for (int i = 0; i < numValues; i++) {
          long value = dict.getLongValue(dictIds[i]);
          assertEquals(value, random.nextLong());
        }
      }
    } catch (Throwable t) {
      fail("Failed with random seed: " + seed, t);
    }
  }

  @Test
  public void testMultiValueIndexingWithRawInt() {
    long seed = System.nanoTime();

    try (DirectMemoryManager memManager = new DirectMemoryManager("test");
        FixedByteMVMutableForwardIndex indexer = new FixedByteMVMutableForwardIndex(MAX_N_VALUES, MAX_N_VALUES / 2,
            NROWS / 3, Integer.BYTES, memManager, "indexer", false, FieldSpec.DataType.INT)) {
      // Insert rows into the indexer
      Random random = new Random(seed);
      for (int row = 0; row < NROWS; row++) {
        int numValues = Math.abs(random.nextInt()) % MAX_N_VALUES;
        int[] values = new int[numValues];
        for (int i = 0; i < numValues; i++) {
          values[i] = random.nextInt();
        }
        indexer.setIntMV(row, values);
      }

      // Read back rows and make sure that the values are good.
      random = new Random(seed);
      int[] intValues = new int[MAX_N_VALUES];
      for (int row = 0; row < NROWS; row++) {
        int numValues = indexer.getIntMV(row, intValues);
        assertEquals(numValues, Math.abs(random.nextInt()) % MAX_N_VALUES);

        for (int i = 0; i < numValues; i++) {
          assertEquals(intValues[i], random.nextInt());
        }
      }
    } catch (Throwable t) {
      fail("Failed with random seed: " + seed, t);
    }
  }

  @Test
  public void testMultiValueIndexingWithRawLong() {
    long seed = System.nanoTime();

    try (DirectMemoryManager memManager = new DirectMemoryManager("test");
        FixedByteMVMutableForwardIndex indexer = new FixedByteMVMutableForwardIndex(MAX_N_VALUES, MAX_N_VALUES / 2,
            NROWS / 3, Long.BYTES, memManager, "indexer", false, FieldSpec.DataType.LONG)) {
      // Insert rows into the indexer
      Random random = new Random(seed);
      for (int row = 0; row < NROWS; row++) {
        int numValues = Math.abs(random.nextInt()) % MAX_N_VALUES;
        long[] values = new long[numValues];
        for (int i = 0; i < numValues; i++) {
          values[i] = random.nextLong();
        }
        indexer.setLongMV(row, values);
      }

      // Read back rows and make sure that the values are good.
      random = new Random(seed);
      long[] longValues = new long[MAX_N_VALUES];
      for (int row = 0; row < NROWS; row++) {
        int numValues = indexer.getLongMV(row, longValues);
        assertEquals(numValues, Math.abs(random.nextInt()) % MAX_N_VALUES);

        for (int i = 0; i < numValues; i++) {
          assertEquals(longValues[i], random.nextLong());
        }
      }
    } catch (Throwable t) {
      fail("Failed with random seed: " + seed, t);
    }
  }

  @Test
  public void testMultiValueIndexingWithRawFloat() {
    long seed = System.nanoTime();

    try (DirectMemoryManager memManager = new DirectMemoryManager("test");
        FixedByteMVMutableForwardIndex indexer = new FixedByteMVMutableForwardIndex(MAX_N_VALUES, MAX_N_VALUES / 2,
            NROWS / 3, Float.BYTES, memManager, "indexer", false, FieldSpec.DataType.FLOAT)) {
      // Insert rows into the indexer
      Random random = new Random(seed);
      for (int row = 0; row < NROWS; row++) {
        int numValues = Math.abs(random.nextInt()) % MAX_N_VALUES;
        float[] values = new float[numValues];
        for (int i = 0; i < numValues; i++) {
          values[i] = random.nextFloat();
        }
        indexer.setFloatMV(row, values);
      }

      // Read back rows and make sure that the values are good.
      random = new Random(seed);
      float[] floatValues = new float[MAX_N_VALUES];
      for (int row = 0; row < NROWS; row++) {
        int numValues = indexer.getFloatMV(row, floatValues);
        assertEquals(numValues, Math.abs(random.nextInt()) % MAX_N_VALUES);

        for (int i = 0; i < numValues; i++) {
          assertEquals(floatValues[i], random.nextFloat());
        }
      }
    } catch (Throwable t) {
      fail("Failed with random seed: " + seed, t);
    }
  }

  @Test
  public void testMultiValueIndexingWithRawDouble() {
    long seed = System.nanoTime();

    try (DirectMemoryManager memManager = new DirectMemoryManager("test");
        FixedByteMVMutableForwardIndex indexer = new FixedByteMVMutableForwardIndex(MAX_N_VALUES, MAX_N_VALUES / 2,
            NROWS / 3, Double.BYTES, memManager, "indexer", false, FieldSpec.DataType.DOUBLE)) {
      // Insert rows into the indexer
      Random random = new Random(seed);
      for (int row = 0; row < NROWS; row++) {
        int numValues = Math.abs(random.nextInt()) % MAX_N_VALUES;
        double[] values = new double[numValues];
        for (int i = 0; i < numValues; i++) {
          values[i] = random.nextDouble();
        }
        indexer.setDoubleMV(row, values);
      }

      // Read back rows and make sure that the values are good.
      random = new Random(seed);
      double[] doubleValues = new double[MAX_N_VALUES];
      for (int row = 0; row < NROWS; row++) {
        int numValues = indexer.getDoubleMV(row, doubleValues);
        assertEquals(numValues, Math.abs(random.nextInt()) % MAX_N_VALUES);

        for (int i = 0; i < numValues; i++) {
          assertEquals(doubleValues[i], random.nextDouble());
        }
      }
    } catch (Throwable t) {
      fail("Failed with random seed: " + seed, t);
    }
  }

  @Test
  public void testMultiValueIndexingWithRawString() {
    long seed = System.nanoTime();

    try (DirectMemoryManager memManager = new DirectMemoryManager("test");
        FixedByteMVMutableForwardIndex indexer = new FixedByteMVMutableForwardIndex(MAX_N_VALUES, MAX_N_VALUES / 2,
            NROWS / 3, 24, memManager, "indexer", false, FieldSpec.DataType.STRING)) {
      // Insert rows into the indexer
      Random random = new Random(seed);
      for (int row = 0; row < NROWS; row++) {
        int numValues = Math.abs(random.nextInt()) % MAX_N_VALUES;
        String[] values = new String[numValues];
        for (int i = 0; i < numValues; i++) {
          values[i] = "random1";
        }
        final int curRow = row;
        assertThrows(UnsupportedOperationException.class, () -> indexer.setStringMV(curRow, values));
      }

      // Read back rows and make sure that the values are good.
      random = new Random(seed);
      String[] stringValues = new String[MAX_N_VALUES];
      for (int row = 0; row < NROWS; row++) {
        final int curRow = row;
        assertThrows(UnsupportedOperationException.class, () -> indexer.getStringMV(curRow, stringValues));
      }
    } catch (Throwable t) {
      fail("Failed with random seed: " + seed, t);
    }
  }

  @Test
  public void testMultiValueIndexingWithRawByte() {
    long seed = System.nanoTime();

    try (DirectMemoryManager memManager = new DirectMemoryManager("test");
        FixedByteMVMutableForwardIndex indexer = new FixedByteMVMutableForwardIndex(MAX_N_VALUES, MAX_N_VALUES / 2,
            NROWS / 3, 24, memManager, "indexer",
        false, FieldSpec.DataType.BYTES)) {
      // Insert rows into the indexer
      Random random = new Random(seed);
      for (int row = 0; row < NROWS; row++) {
        int numValues = Math.abs(random.nextInt()) % MAX_N_VALUES;
        byte[][] values = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          values[i] = "random1".getBytes(StandardCharsets.UTF_8);
        }
        final int curRow = row;
        assertThrows(UnsupportedOperationException.class, () -> indexer.setBytesMV(curRow, values));
      }

      // Read back rows and make sure that the values are good.
      random = new Random(seed);
      byte[][] byteValues = new byte[MAX_N_VALUES][];
      for (int row = 0; row < NROWS; row++) {
        final int curRow = row;
        assertThrows(UnsupportedOperationException.class, () -> indexer.getBytesMV(curRow, byteValues));
      }
    } catch (Throwable t) {
      fail("Failed with random seed: " + seed, t);
    }
  }
}
