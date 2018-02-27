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
package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;


public class MultiValueDictionaryTest {
  private static final int NROWS = 1000;
  private static final int MAX_N_VALUES = 1000;
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(MultiValueDictionaryTest.class.getName());
  }

  @AfterClass
  public void tearDown() throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testMultiValueIndexing() {
    final long seed = System.nanoTime();
    try {
      testMultiValueIndexing(seed);
    } catch (Exception e) {
      Assert.fail("Failed with seed=" + seed);
    }
  }

  private void testMultiValueIndexing(final long seed)
      throws Exception {
    final LongOnHeapMutableDictionary dict = new LongOnHeapMutableDictionary();
    final FixedByteSingleColumnMultiValueReaderWriter indexer =
        new FixedByteSingleColumnMultiValueReaderWriter(MAX_N_VALUES, MAX_N_VALUES/2, NROWS/3, Integer.SIZE/8, new DirectMemoryManager("test"), "indexer");

    // Insert rows into the indexer and dictionary
    Random random = new Random(seed);
    for (int row = 0; row < NROWS; row++) {
      int nValues = Math.abs(random.nextInt()) % MAX_N_VALUES;
      Long[] val = new Long[nValues];
      for (int i = 0; i < nValues; i++) {
        val[i] = random.nextLong();
      }
      dict.index(val);
      int dictIds[] = new int[nValues];
      for (int i = 0; i < nValues; i++) {
        dictIds[i] = dict.indexOf(val[i]);
      }
      indexer.setIntArray(row, dictIds);
    }

    // Read back rows and make sure that the values are good.
    random = new Random(seed);
    final int[] dictIds = new int[MAX_N_VALUES];
    for (int row = 0; row < NROWS; row++) {
      int nValues = indexer.getIntArray(row, dictIds);
      Assert.assertEquals(nValues, Math.abs(random.nextInt()) % MAX_N_VALUES,
          "Mismatching number of values, random seed is: " + seed);

      for (int i = 0; i < nValues; i++) {
        Long val = dict.getLongValue(dictIds[i]);
        Assert.assertEquals(val.longValue(), random.nextLong(),
            "Value mismatch at row " + row + ", random seed is: " + seed);
      }
    }
  }
}
