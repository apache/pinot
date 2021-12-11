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

import java.util.Random;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class MultiValueDictionaryTest {
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
  public void testMultiValueIndexing() {
    long seed = System.nanoTime();
    try (LongOnHeapMutableDictionary dict = new LongOnHeapMutableDictionary();
        FixedByteMVMutableForwardIndex indexer = new FixedByteMVMutableForwardIndex(MAX_N_VALUES, MAX_N_VALUES / 2,
            NROWS / 3, Integer.BYTES, new DirectMemoryManager("test"), "indexer")) {
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
}
