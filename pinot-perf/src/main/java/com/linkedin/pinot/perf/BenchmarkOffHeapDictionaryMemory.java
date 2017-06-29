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

package com.linkedin.pinot.perf;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.LongOffHeapMutableDictionary;


// Test to get memory statistics for off-heap dictionary
public class BenchmarkOffHeapDictionaryMemory {
  private Long[] colValues;
  private long[] uniqueColValues;
  private final int nRuns = 10;
  private final int nDivs = 10;
  final int cardinality = 1_000_000;
  final int nRows = 2_500_000;
  private final long[] totalMem = new long[nDivs+1];
  private final int[] nBufs = new int[nDivs+1];
  private final int [] overflowSize = new int[nDivs+1];
  private RealtimeIndexOffHeapMemoryManager _memoryManager;

  @Setup
  public void setUp() {
    _memoryManager = new DirectMemoryManager(BenchmarkOffHeapDictionaryMemory.class.getName());
  }

  @TearDown
  public void tearDown() throws Exception {
    _memoryManager.close();
  }

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
    LongOffHeapMutableDictionary dictionary = new LongOffHeapMutableDictionary(initialCardinality, maxOverflowSize,
        _memoryManager, "longColumn");

    for (int i = 0; i < colValues.length; i++) {
      dictionary.index(colValues[i]);
    }
    return dictionary;
  }

  private void addStats(BaseOffHeapMutableDictionary dictionary, int actualCardinality, int initialCardinality,
      int maxOverflowSize, int div) {
    totalMem[div] += dictionary.getTotalOffHeapMemUsed();
    overflowSize[div] += dictionary.getNumberOfOveflowValues();
    nBufs[div] += dictionary.getNumberOfHeapBuffersUsed();

    /*
    System.out.println("Cardinality:" + actualCardinality + ",initialCardinality:" + initialCardinality +
        ",OffHeapMem:" + dictionary.getTotalOffHeapMemUsed()/1024/1024 + "MB" +
            ",NumBuffers=" + dictionary.getNumberOfHeapBuffersUsed() +
            ",maxOverflowSize=" + maxOverflowSize +
            ",actualOverflowSize=" + dictionary.getNumberOfOveflowValues() +
            ",rowFills=" + Arrays.toString(dictionary.getRowFillCount())
    );
    */
  }

  private void printStats() {
    for (int div = 1; div < nDivs; div++) {
      totalMem[div] /= nRuns;
      nBufs[div] /= nRuns;
      overflowSize[div] /= nRuns;
      System.out.println("Div=" + div + ",TotalMem:" + totalMem[div]/1024/1024 + "MB,nBufs=" + nBufs[div] + ",numOverflows=" + overflowSize[div]);
    }
  }

  private void clearStats() {
    for (int div = 1; div < nDivs; div++) {
      totalMem[div] = 0;
      nBufs[div] = 0;
      overflowSize[div] = 0;
    }
  }

  private void testMem(final int maxOverflowSize) throws Exception {
    clearStats();

    for (int div = 1; div <= nDivs; div++) {
      setupValues(cardinality, nRows);
      for (int i = 0; i < nRuns; i++) {
        int initialCardinality = cardinality/div;
        BaseOffHeapMutableDictionary dictionary = testMem(cardinality, initialCardinality, maxOverflowSize);
        addStats(dictionary, cardinality, initialCardinality, maxOverflowSize, div);
        dictionary.close();
      }
    }

    printStats();
  }

  public static void main(String[] args) throws Exception {
    BenchmarkOffHeapDictionaryMemory benchmark = new BenchmarkOffHeapDictionaryMemory();
    System.out.println("Results with overflow:");
    benchmark.testMem(1000);
    System.out.println("Results without overflow:");
    benchmark.testMem(0);
  }
}
