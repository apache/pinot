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

import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;


public class MultiValueDictionaryTest {
  private static final String COL_NAME = "greek";
  private static final int NROWS = 1000;
  private static final int MAX_N_VALUES = FixedByteSingleColumnMultiValueReaderWriter.DEFAULT_MAX_NUMBER_OF_MULTIVALUES;
  private static final long RANDOM_SEED = System.nanoTime();

  @Test
  public void testMultiValueIndexing()
      throws Exception {
    final FieldSpec mvIntFs = new DimensionFieldSpec(COL_NAME, FieldSpec.DataType.LONG, false);
    final LongMutableDictionary dict = new LongMutableDictionary(COL_NAME);
    final FixedByteSingleColumnMultiValueReaderWriter indexer =
        new FixedByteSingleColumnMultiValueReaderWriter(NROWS, Integer.SIZE / 8, MAX_N_VALUES);

    // Insert rows into the indexer and dictionary
    Random random = new Random(RANDOM_SEED);
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
    random = new Random(RANDOM_SEED);
    final int[] dictIds = new int[MAX_N_VALUES];
    for (int row = 0; row < NROWS; row++) {
      int nValues = indexer.getIntArray(row, dictIds);
      Assert.assertEquals(nValues, Math.abs(random.nextInt()) % MAX_N_VALUES,
          "Mismatching number of values, random seed is: " + RANDOM_SEED);

      for (int i = 0; i < nValues; i++) {
        Long val = dict.getLongValue(dictIds[i]);
        Assert.assertEquals(val.longValue(), random.nextLong(),
            "Value mismatch at row " + row + ", random seed is: " + RANDOM_SEED);
      }
    }
  }
}
