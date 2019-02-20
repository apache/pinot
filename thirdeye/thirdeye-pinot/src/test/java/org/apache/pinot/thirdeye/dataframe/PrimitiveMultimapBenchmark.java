/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package org.apache.pinot.thirdeye.dataframe;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PrimitiveMultimapBenchmark {
  // TODO: validate benchmarking method - Dead Code Elimination, etc. may be playing tricks on us.

  private static final Logger LOG = LoggerFactory.getLogger(PrimitiveMultimapBenchmark.class);

  private static final int N_ROUNDS = 15;
  private static final int N_ELEMENTS = 10_000_000;

  private static final String[] SERIES_NAMES = new String[] { "task", "min", "mid", "max", "outer", "checksum", "samples", "collisions", "rereads" };

  private static final long SEED = System.nanoTime();

  private long tStart;
  private long tStartOuter;
  private List<Long> times = new ArrayList<>();
  private long timeOuter;
  private DataFrame.Builder results = DataFrame.builder(SERIES_NAMES);

  private void benchmarkPrimitiveMap() {
    startTimerOuter();
    long checksum = 0;
    long collisions = 0;
    long rereads = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      int[] keys = generateIntData(N_ELEMENTS);
      int[] values = generateIntData(N_ELEMENTS);
      int[] output = new int[N_ELEMENTS];
      PrimitiveMultimap m = new PrimitiveMultimap(N_ELEMENTS);

      startTimer();
      for(int i=0; i<N_ELEMENTS; i++) {
        m.put(keys[i], values[i]);
      }
      for(int i=0; i<N_ELEMENTS; i++) {
        output[i] = m.get(keys[i]);
      }
      stopTimer();

      assertEquals(output, values);
      checksum ^= checksum(output);
      collisions += m.getCollisions();
      rereads += m.getRereads();
    }

    logResults("benchmarkPrimitiveMap", checksum, collisions, rereads);
  }

  private void benchmarkHashMap() {
    startTimerOuter();
    long checksum = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      int[] keys = generateIntData(N_ELEMENTS);
      int[] values = generateIntData(N_ELEMENTS);
      int[] output = new int[N_ELEMENTS];
      Map<Integer, Integer> m = new HashMap<>(N_ELEMENTS);

      startTimer();
      for(int i=0; i<N_ELEMENTS; i++) {
        m.put(keys[i], values[i]);
      }
      for(int i=0; i<N_ELEMENTS; i++) {
        output[i] = m.get(keys[i]);
      }
      stopTimer();

      assertEquals(output, values);
      checksum ^= checksum(output);
    }

    logResults("benchmarkHashMap", checksum, 0, 0);
  }

  private void benchmarkArrayListMap() {
    startTimerOuter();
    long checksum = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      int[] keys = generateIntData(N_ELEMENTS);
      int[] values = generateIntData(N_ELEMENTS);
      int[] output = new int[N_ELEMENTS];
      Multimap<Integer, Integer> m = ArrayListMultimap.create(N_ELEMENTS, 1);

      startTimer();
      for(int i=0; i<N_ELEMENTS; i++) {
        m.put(keys[i], values[i]);
      }
      for(int i=0; i<N_ELEMENTS; i++) {
        for(int v : m.get(keys[i]))
          output[i] = v;
      }
      stopTimer();

      assertEquals(output, values);
      checksum ^= checksum(output);
    }

    logResults("benchmarkArrayListMap", checksum, 0, 0);
  }

  private void benchmarkPrimitiveMultimap() {
    startTimerOuter();
    long checksum = 0;
    long collisions = 0;
    long rereads = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      int[] values = generateIntData(N_ELEMENTS);
      int[] output = new int[N_ELEMENTS];
      PrimitiveMultimap m = new PrimitiveMultimap(N_ELEMENTS);

      startTimer();
      for(int i=N_ELEMENTS-1; i>=0; i--) {
        m.put(i & (0x400 - 1), values[i]);
      }

      int cntr = 0;
      for(int i=0; i<0x400; i++) {
        int val = m.get(i);
        while(val != -1) {
          output[cntr++] = val;
          val = m.getNext();
        }
      }
      stopTimer();

      if(cntr != N_ELEMENTS)
        throw new IllegalStateException(String.format("Expected %d elements, but got %d", N_ELEMENTS, cntr));
      assertEqualsSorted(output, values);
      checksum ^= checksum(output);
      collisions += m.getCollisions();
      rereads += m.getRereads();
    }

    logResults("benchmarkPrimitiveMultimap", checksum, collisions, rereads);
  }

  private void benchmarkArrayListMultimap() {
    startTimerOuter();
    long checksum = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      int[] values = generateIntData(N_ELEMENTS);
      int[] output = new int[N_ELEMENTS];
      Multimap<Integer, Integer> m = ArrayListMultimap.create(0x400, (N_ELEMENTS >>> 10) + 1);

      startTimer();
      for(int i=N_ELEMENTS-1; i>=0; i--) {
        m.put(i & (0x400 - 1), values[i]);
      }

      int cntr = 0;
      for(int i=0; i<0x400; i++) {
        for(int val : m.get(i))
          output[cntr++] = val;
      }
      stopTimer();

      if(cntr != N_ELEMENTS)
        throw new IllegalStateException(String.format("Expected %d elements, but got %d", N_ELEMENTS, cntr));
      assertEqualsSorted(output, values);
      checksum ^= checksum(output);
    }

    logResults("benchmarkArrayListMultimap", checksum, 0, 0);
  }

  private void benchmarkPrimitiveMultimapSkewed() {
    startTimerOuter();
    long checksum = 0;
    long collisions = 0;
    long rereads = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      int[] values = generateIntData(N_ELEMENTS);
      int[] output = new int[N_ELEMENTS];
      PrimitiveMultimap m = new PrimitiveMultimap(N_ELEMENTS);

      startTimer();
      for(int i=N_ELEMENTS-1; i>=0; i--) {
        m.put(i & (0x10 - 1), values[i]);
      }

      int cntr = 0;
      for(int i=0; i<0x10; i++) {
        int val = m.get(i);
        while(val != -1) {
          output[cntr++] = val;
          val = m.getNext();
        }
      }
      stopTimer();

      if(cntr != N_ELEMENTS)
        throw new IllegalStateException(String.format("Expected %d elements, but got %d", N_ELEMENTS, cntr));
      assertEqualsSorted(output, values);
      checksum ^= checksum(output);
      collisions += m.getCollisions();
      rereads += m.getRereads();
    }

    logResults("benchmarkPrimitiveMultimapSkewed", checksum, collisions, rereads);
  }

  private void benchmarkArrayListMultimapSkewed() {
    startTimerOuter();
    long checksum = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      int[] values = generateIntData(N_ELEMENTS);
      int[] output = new int[N_ELEMENTS];
      Multimap<Integer, Integer> m = ArrayListMultimap.create(0x10, (N_ELEMENTS >>> 4) + 1);

      startTimer();
      for(int i=N_ELEMENTS-1; i>=0; i--) {
        m.put(i & (0x10 - 1), values[i]);
      }

      int cntr = 0;
      for(int i=0; i<0x10; i++) {
        for(int val : m.get(i)) {
          output[cntr++] = val;
        }
      }
      stopTimer();

      if(cntr != N_ELEMENTS)
        throw new IllegalStateException(String.format("Expected %d elements, but got %d", N_ELEMENTS, cntr));
      assertEqualsSorted(output, values);
      checksum ^= checksum(output);
    }

    logResults("benchmarkArrayListMultimapSkewed", checksum, 0, 0);
  }

  private void benchmarkPrimitiveMultimapSequential() {
    startTimerOuter();
    long checksum = 0;
    long collisions = 0;
    long rereads = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      int[] values = generateIntData(N_ELEMENTS);
      int[] output = new int[N_ELEMENTS];
      PrimitiveMultimap m = new PrimitiveMultimap(N_ELEMENTS);

      startTimer();
      for(int i=N_ELEMENTS-1; i>=0; i--) {
        m.put(i >>> 4, values[i]);
      }

      int cntr = 0;
      for(int i=0; i<N_ELEMENTS >>> 4; i++) {
        int val = m.get(i);
        while(val != -1) {
          output[cntr++] = val;
          val = m.getNext();
        }
      }
      stopTimer();

      if(cntr != N_ELEMENTS)
        throw new IllegalStateException(String.format("Expected %d elements, but got %d", N_ELEMENTS, cntr));
      assertEqualsSorted(output, values);
      checksum ^= checksum(output);
      collisions += m.getCollisions();
      rereads += m.getRereads();
    }

    logResults("benchmarkPrimitiveMultimapSequential", checksum, collisions, rereads);
  }

  private void benchmarkArrayListMultimapSequential() {
    startTimerOuter();
    long checksum = 0;

    for (int r = 0; r < N_ROUNDS; r++) {
      int[] values = generateIntData(N_ELEMENTS);
      int[] output = new int[N_ELEMENTS];
      Multimap<Integer, Integer> m = ArrayListMultimap.create(N_ELEMENTS >>> 4, 0x10 + 1);

      startTimer();
      for(int i=N_ELEMENTS-1; i>=0; i--) {
        m.put(i >>> 4, values[i]);
      }

      int cntr = 0;
      for(int i=0; i<N_ELEMENTS >>> 4; i++) {
        for(int val : m.get(i)) {
          output[cntr++] = val;
        }
      }
      stopTimer();

      if(cntr != N_ELEMENTS)
        throw new IllegalStateException(String.format("Expected %d elements, but got %d", N_ELEMENTS, cntr));
      assertEqualsSorted(output, values);
      checksum ^= checksum(output);
    }

    logResults("benchmarkArrayListMultimapSequential", checksum, 0, 0);
  }

  private void benchmarkAll() {
    benchmarkPrimitiveMap();
    benchmarkHashMap();
    benchmarkArrayListMap();
    benchmarkPrimitiveMultimap();
    benchmarkArrayListMultimap();
    benchmarkPrimitiveMultimapSkewed();
    benchmarkArrayListMultimapSkewed();
    benchmarkPrimitiveMultimapSequential();
    benchmarkArrayListMultimapSequential();
  }

  private void startTimer() {
    this.tStart = System.nanoTime();
  }

  private void stopTimer() {
    long tDelta = System.nanoTime() - this.tStart;
    this.times.add(tDelta);
  }

  private void startTimerOuter() {
    this.tStartOuter = System.nanoTime();
  }

  private void stopTimerOuter() {
    this.timeOuter = System.nanoTime() - this.tStartOuter;
  }

  private void logResults(String name, long checksum, long collissions, long rereads) {
    stopTimerOuter();
    Collections.sort(this.times);
    long tMid = this.times.get(this.times.size() / 2);
    long tMin = Collections.min(this.times);
    long tMax = Collections.max(this.times);
    LOG.info("{}: min/mid/max = {}ms {}ms {}ms [all={}ms, chk={}, cnt={}, colls={}, rread={}]", name, tMin / 1000000, tMid / 1000000, tMax / 1000000, timeOuter / 1000000, checksum % 1000, this.times.size(), collissions, rereads);
    this.results.append(name, tMin, tMid, tMax, this.timeOuter, checksum, this.times.size(), collissions, rereads);

    // reset timer stats
    this.times = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    LOG.info("Press Enter key to start.");
    System.in.read();

    LOG.info("Running PrimitiveMultimap benchmark ...");
    PrimitiveMultimapBenchmark b = new PrimitiveMultimapBenchmark();
    b.benchmarkAll();

    Series.LongFunction toMillis = new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] / 1000000;
      }
    };

    DataFrame df = b.results.build();
    df.mapInPlace(toMillis, "min");
    df.mapInPlace(toMillis, "mid");
    df.mapInPlace(toMillis, "max");
    df.mapInPlace(toMillis, "outer");
    df.mapInPlace(new Series.LongFunction() {
      @Override
      public long apply(long... values) {
        return values[0] % 1000;
      }
    }, "checksum");

    LOG.info("Summary:\n{}", df.toString(40, SERIES_NAMES));
    LOG.info("done.");
  }

  private static int[] generateIntData(int n) {
    Random r = new Random();
    r.setSeed(SEED);
    int[] values = new int[n];
    for(int i=0; i<n; i++) {
      values[i] = r.nextInt() & 0x7FFFFFFF;
    }
    return values;
  }

  private static long checksum(double... values) {
    long bits = 0;
    for(double v : values) {
      bits ^= Double.doubleToLongBits(v);
    }
    return bits;
  }

  private static long checksum(long... values) {
    long bits = 0;
    for(long v : values) {
      bits ^= v;
    }
    return bits;
  }

  private static long checksum(int... values) {
    long bits = 0;
    for(int v : values) {
      bits ^= v;
    }
    return bits;
  }

  private static int[] assertEquals(int[] actual, int[] expected) {
    if(actual.length != expected.length)
      throw new IllegalArgumentException(String.format("expected length %d, but got %d", expected.length, actual.length));
    for(int i=0; i<expected.length; i++) {
      if(expected[i] != actual[i])
        throw new IllegalArgumentException(String.format("Expected index=%d value %d, but got %d", i, expected[i], actual[i]));
    }
    return actual;
  }

  private static int[] assertEqualsSorted(int[] actual, int[] expected) {
    Arrays.sort(actual);
    Arrays.sort(expected);
    return assertEquals(actual, expected);
  }

  // from: https://stackoverflow.com/questions/1519736/random-shuffling-of-an-array
  private static long[] shuffle(long[] arr) {
    arr = Arrays.copyOf(arr, arr.length);
    Random rnd = ThreadLocalRandom.current();
    for (int i=arr.length-1; i>0; i--) {
      int index = rnd.nextInt(i + 1);
      long a = arr[index];
      arr[index] = arr[i];
      arr[i] = a;
    }
    return arr;
  }
}
