/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;


public class MultiValueDictionaryTest {
  private static final String COL_NAME = "greek";
  private static int NROWS = 10000;
  private static int MAX_N_VALUES = 999;

  @Test
  public void testMultiValueIndexing() throws Exception {
    final FieldSpec mvIntFs = new DimensionFieldSpec(COL_NAME, FieldSpec.DataType.LONG, false);
    final LongMutableDictionary dict = new LongMutableDictionary(mvIntFs);
    final FixedByteSingleColumnMultiValueReaderWriter indexer = new FixedByteSingleColumnMultiValueReaderWriter(NROWS,
        Integer.SIZE / 8, FixedByteSingleColumnMultiValueReaderWriter.DEFAULT_MAX_NUMBER_OF_MULTIVALUES);

    // Insert rows into the indexer and dictionary
    for (int row = 0; row < NROWS; row++) {
      int nValues = (row % MAX_N_VALUES) + 1;
      Long[] val = new Long[nValues];
      for (int i = 0; i < nValues; i++) {
        val[i] = (long)row * i;
      }
      dict.index(val);
      int dictIds[] = new int[nValues];
      for (int i = 0; i < nValues; i++) {
        dictIds[i] = dict.indexOf(val[i]);
      }
      indexer.setIntArray(row, dictIds);
    }

    // Read back rows and make sure that the values are good.
    final int[] dictIds = new int[MAX_N_VALUES];
    for (int row = 0; row < NROWS; row++) {
      int nValues = indexer.getIntArray(row, dictIds);
      Assert.assertEquals(nValues, (row % MAX_N_VALUES)+1, "Mismatching number of values");

      for (int i = 0; i < nValues; i++) {
        Long val = dict.getLongValue(dictIds[i]);
        Assert.assertEquals(val.longValue(), (long)row * i, "Value mismatch at row " + row);
      }
    }
  }
}
