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
package org.apache.pinot.perf;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.core.query.aggregation.groupby.DictionaryBasedGroupKeyGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class BenchmarkIntOpenHashMap {
  private static final int NUM_VALUES = 10_000_000;
  private static final int NUM_THREADS = 20;
  private static final Random RANDOM = new Random();

  private final int[] _values = new int[NUM_VALUES];
  private ExecutorService _executorService;

  @Param({"10000", "20000", "50000", "100000", "150000"})
  public int _cardinality;

  @Setup
  public void setUp() throws Exception {
    for (int i = 0; i < NUM_VALUES; i++) {
      _values[i] = RANDOM.nextInt(_cardinality);
    }
    _executorService = Executors.newFixedThreadPool(NUM_THREADS);
  }

  @TearDown
  public void tearDown() {
    _executorService.shutdown();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int intOpenHashMap() throws Exception {
    AtomicInteger globalSum = new AtomicInteger();
    CountDownLatch operatorLatch = new CountDownLatch(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      _executorService.submit(() -> {
        int sum = 0;
        Int2IntOpenHashMap map = new Int2IntOpenHashMap((int) ((1 << 10) * 0.75f));
        map.defaultReturnValue(-1);
        for (int value : _values) {
          sum += getGroupId(map, value);
        }
        ObjectIterator<Int2IntMap.Entry> iterator = map.int2IntEntrySet().fastIterator();
        while (iterator.hasNext()) {
          Int2IntMap.Entry entry = iterator.next();
          sum += entry.getIntKey();
          sum += entry.getIntValue();
        }
        globalSum.addAndGet(sum);
        operatorLatch.countDown();
      });
    }
    operatorLatch.await();
    return globalSum.get();
  }

  private int getGroupId(Int2IntOpenHashMap map, int value) {
    int numGroups = map.size();
    if (numGroups < 100_000) {
      return map.computeIfAbsent(value, k -> numGroups);
    } else {
      return map.get(value);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int intGroupIdMap() throws Exception {
    AtomicInteger globalSum = new AtomicInteger();
    CountDownLatch operatorLatch = new CountDownLatch(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      _executorService.submit(() -> {
        int sum = 0;
        DictionaryBasedGroupKeyGenerator.IntGroupIdMap map = new DictionaryBasedGroupKeyGenerator.IntGroupIdMap();
        for (int value : _values) {
          map.getGroupId(value, 100_000);
        }
        Iterator<DictionaryBasedGroupKeyGenerator.IntGroupIdMap.Entry> iterator = map.iterator();
        while (iterator.hasNext()) {
          DictionaryBasedGroupKeyGenerator.IntGroupIdMap.Entry entry = iterator.next();
          sum += entry._rawKey;
          sum += entry._groupId;
        }
        globalSum.addAndGet(sum);
        operatorLatch.countDown();
      });
    }
    operatorLatch.await();
    return globalSum.get();
  }

  public static void main(String[] args) throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkIntOpenHashMap.class.getSimpleName()).build()).run();
  }
}
