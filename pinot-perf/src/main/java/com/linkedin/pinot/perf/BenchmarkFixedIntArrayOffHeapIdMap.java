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
import com.linkedin.pinot.core.util.FixedIntArray;
import com.linkedin.pinot.core.util.FixedIntArrayOffHeapIdMap;
import com.linkedin.pinot.core.util.IdMap;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
public class BenchmarkFixedIntArrayOffHeapIdMap {
  private static final int ROW_COUNT = 2_500_000;
  private static final int COLUMN_CARDINALITY = 100;
  private static final int NUM_COLUMNS = 3;
  private static final int CARDINALITY = (new Double(Math.pow(COLUMN_CARDINALITY, NUM_COLUMNS))).intValue();

  private FixedIntArray[] _values;

  @Setup
  public void setUp() {
    Random random = new Random(System.nanoTime());

    FixedIntArray[] uniqueValues = new FixedIntArray[CARDINALITY];
    for (int i = 0; i < uniqueValues.length; i++) {
      uniqueValues[i] = new FixedIntArray(
          new int[]{random.nextInt(COLUMN_CARDINALITY), random.nextInt(COLUMN_CARDINALITY), random.nextInt(
              COLUMN_CARDINALITY)});
    }

    _values = new FixedIntArray[ROW_COUNT];
    for (int i = 0; i < _values.length; i++) {
      _values[i] = uniqueValues[random.nextInt(CARDINALITY)];
    }
  }

  @TearDown
  public void tearDown()
      throws Exception {
  }

  // Start with mid size, with overflow
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public IdMap<FixedIntArray> benchmarkOffHeapWithResizeWithCache()
      throws IOException {
    PinotDataBufferMemoryManager memoryManager = new DirectMemoryManager("perfTest");

    IdMap<FixedIntArray> idMap =
        new FixedIntArrayOffHeapIdMap(CARDINALITY / 10, 1000, NUM_COLUMNS, memoryManager,
            "perfTestWithCache");

    for (FixedIntArray value : _values) {
      idMap.put(value);
    }

    memoryManager.close();
    return idMap;
  }

  // Start with max size, no cache
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public IdMap<FixedIntArray> benchmarkOffHeapWithReSizeWithoutCache()
      throws IOException {
    PinotDataBufferMemoryManager memoryManager = new DirectMemoryManager("perfTest");

    IdMap<FixedIntArray> idMap =
        new FixedIntArrayOffHeapIdMap(CARDINALITY / 10, 0, NUM_COLUMNS, memoryManager,
            "perfTestWithCache");

    for (FixedIntArray value : _values) {
      idMap.put(value);
    }

    memoryManager.close();
    return idMap;
  }

  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public IdMap<FixedIntArray> benchmarkOffHeapPreSizeWithCache()
      throws IOException {
    PinotDataBufferMemoryManager memoryManager = new DirectMemoryManager("perfTest");

    IdMap<FixedIntArray> idMap =
        new FixedIntArrayOffHeapIdMap(CARDINALITY, 1000, NUM_COLUMNS, memoryManager, "perfTestWithCache");

    for (FixedIntArray value : _values) {
      idMap.put(value);
    }

    memoryManager.close();
    return idMap;
  }

  // Start with max size, no cache
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public IdMap<FixedIntArray> benchmarkOffHeapPreSizeWithoutCache()
      throws IOException {
    PinotDataBufferMemoryManager memoryManager = new DirectMemoryManager("perfTest");

    IdMap<FixedIntArray> idMap =
        new FixedIntArrayOffHeapIdMap(CARDINALITY, 0, NUM_COLUMNS, memoryManager, "perfTestWithCache");

    for (FixedIntArray value : _values) {
      idMap.put(value);
    }

    memoryManager.close();
    return idMap;
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkFixedIntArrayOffHeapIdMap.class.getSimpleName())
        .warmupTime(TimeValue.seconds(10))
        .warmupIterations(2)
        .measurementTime(TimeValue.seconds(30))
        .measurementIterations(5)
        .forks(1);

    new Runner(opt.build()).run();
  }
}
