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

import java.util.Arrays;
import org.testng.annotations.Test;


// Test to get memory statistics for off-heap dictionary
public class OffHeapDictionaryMemTest {

  private Long[] colValues;
  private long[] uniqueColValues;

  private void setupValues(final int cardinality, final int nRows) {
    // Create a list of values to insert into the hash map
    uniqueColValues = new long[cardinality];
    for (int i = 0; i < uniqueColValues.length; i++) {
      uniqueColValues[i] = (long)(Math.random() * Long.MAX_VALUE);
    }
    colValues = new Long[nRows];
    for (int i = 0; i < colValues.length; i++) {
      colValues[i] = uniqueColValues[(int)(Math.random() * cardinality)];
    }
  }

  private BaseOffHeapMutableDictionary testMem(final int actualCardinality, final int initialCardinality, final int maxOverflowSize) throws Exception {
    LongOffHeapMutableDictionary dictionary = new LongOffHeapMutableDictionary(initialCardinality, maxOverflowSize);

    for (int i = 0; i < colValues.length; i++) {
      dictionary.index(colValues[i]);
    }
    return dictionary;
  }

  private void printStats(BaseOffHeapMutableDictionary dictionary, int actualCardinality, int initialCardinality, int maxOverflowSize) {
    System.out.println("Cardinality:" + actualCardinality + ",initialCardinality:" + initialCardinality +
        ",OffHeapMem:" + dictionary.getTotalOffHeapMemUsed()/1024/1024 + "MB" +
            ",NumBuffers=" + dictionary.getNumberOfHeapBuffersUsed() +
            ",maxOverflowSize=" + maxOverflowSize +
            ",actualOverflowSize=" + dictionary.getNumberOfOveflowValues() +
            ",rowFills=" + Arrays.toString(dictionary.getRowFillCount())
    );
  }

  @Test
  public void testMem() throws Exception {
    int cardinality = 1_000_000;
    int nRows = 2_500_000;
    setupValues(cardinality, nRows);
    /*
    {
      int initialCardinality = cardinality;
      int maxOverflowSize = 0;
      BaseOffHeapMutableDictionary dictionary = testMem(cardinality, initialCardinality, maxOverflowSize);
      printStats(dictionary, cardinality, initialCardinality, maxOverflowSize);
      dictionary.close();
    }
    {
      int initialCardinality = cardinality;
      int maxOverflowSize = 1000;
      BaseOffHeapMutableDictionary dictionary = testMem(cardinality, initialCardinality, maxOverflowSize);
      printStats(dictionary, cardinality, initialCardinality, maxOverflowSize);
      dictionary.close();
    }
    {
      int initialCardinality = cardinality/3;
      int maxOverflowSize = 0;
      BaseOffHeapMutableDictionary dictionary = testMem(cardinality, initialCardinality, maxOverflowSize);
      printStats(dictionary, cardinality, initialCardinality, maxOverflowSize);
      dictionary.close();
    }
    {
      int initialCardinality = cardinality/3;
      int maxOverflowSize = 1000;
      BaseOffHeapMutableDictionary dictionary = testMem(cardinality, initialCardinality, maxOverflowSize);
      printStats(dictionary, cardinality, initialCardinality, maxOverflowSize);
      dictionary.close();
    }
    */
    {
      int initialCardinality = cardinality/4;
      int maxOverflowSize = 0;
      BaseOffHeapMutableDictionary dictionary = testMem(cardinality, initialCardinality, maxOverflowSize);
      printStats(dictionary, cardinality, initialCardinality, maxOverflowSize);
      dictionary.close();
    }
    {
      int initialCardinality = cardinality/4;
      int maxOverflowSize = 1000;
      BaseOffHeapMutableDictionary dictionary = testMem(cardinality, initialCardinality, maxOverflowSize);
      printStats(dictionary, cardinality, initialCardinality, maxOverflowSize);
      dictionary.close();
    }
  }

}
