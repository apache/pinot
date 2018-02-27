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

import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.DirectMemoryManager;
import com.linkedin.pinot.core.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.LongOffHeapMutableDictionary;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;


// Test to get memory statistics for off-heap dictionary
public class BenchmarkOffHeapDictionaryMemory {
  private Long[] _colValues;
  private final int _nRuns = 10;
  private final int _nDivs = 10;
  final int _cardinality = 1_000_000;
  final int _nRows = 2_500_000;
  private final long[] _totalMem = new long[_nDivs + 1];
  private final int[] _nBufs = new int[_nDivs + 1];
  private final int[] _overflowSize = new int[_nDivs + 1];
  private PinotDataBufferMemoryManager _memoryManager;

  @Setup
  public void setUp() {
    _memoryManager = new DirectMemoryManager(BenchmarkOffHeapDictionaryMemory.class.getName());
  }

  @TearDown
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  private void setupValues(final int cardinality, final int nRows) {
    // Create a list of values to insert into the hash map
    long[] uniqueColValues = new long[cardinality];
    for (int i = 0; i < uniqueColValues.length; i++) {
      uniqueColValues[i] = (long) (Math.random() * Long.MAX_VALUE);
    }
    _colValues = new Long[nRows];
    for (int i = 0; i < _colValues.length; i++) {
      _colValues[i] = uniqueColValues[(int) (Math.random() * cardinality)];
    }
  }

  private BaseOffHeapMutableDictionary testMem(final int initialCardinality, final int maxOverflowSize)
      throws Exception {
    LongOffHeapMutableDictionary dictionary =
        new LongOffHeapMutableDictionary(initialCardinality, maxOverflowSize, _memoryManager, "longColumn");

    for (Long colValue : _colValues) {
      dictionary.index(colValue);
    }
    return dictionary;
  }

  private void addStats(BaseOffHeapMutableDictionary dictionary, int div) {
    _totalMem[div] += dictionary.getTotalOffHeapMemUsed();
    _overflowSize[div] += dictionary.getNumberOfOveflowValues();
    _nBufs[div] += dictionary.getNumberOfHeapBuffersUsed();

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
    for (int div = 1; div < _nDivs; div++) {
      _totalMem[div] /= _nRuns;
      _nBufs[div] /= _nRuns;
      _overflowSize[div] /= _nRuns;
      System.out.println(
          "Div=" + div + ",TotalMem:" + _totalMem[div] / 1024 / 1024 + "MB,_nBufs=" + _nBufs[div] + ",numOverflows="
              + _overflowSize[div]);
    }
  }

  private void clearStats() {
    for (int div = 1; div < _nDivs; div++) {
      _totalMem[div] = 0;
      _nBufs[div] = 0;
      _overflowSize[div] = 0;
    }
  }

  private void testMem(final int maxOverflowSize)
      throws Exception {
    clearStats();

    for (int div = 1; div <= _nDivs; div++) {
      setupValues(_cardinality, _nRows);
      for (int i = 0; i < _nRuns; i++) {
        int initialCardinality = _cardinality / div;
        BaseOffHeapMutableDictionary dictionary = testMem(initialCardinality, maxOverflowSize);
        addStats(dictionary, div);
        dictionary.close();
      }
    }

    printStats();
  }

  public static void main(String[] args)
      throws Exception {
    BenchmarkOffHeapDictionaryMemory benchmark = new BenchmarkOffHeapDictionaryMemory();
    System.out.println("Results with overflow:");
    benchmark.testMem(1000);
    System.out.println("Results without overflow:");
    benchmark.testMem(0);
  }
}
