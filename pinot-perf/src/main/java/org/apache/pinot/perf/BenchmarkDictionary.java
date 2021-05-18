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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.dictionary.LongOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.LongOnHeapMutableDictionary;
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
public class BenchmarkDictionary {
  private static final int ROW_COUNT = 2_500_000;
  private static final int CARDINALITY = 1_000_000;

  private Long[] _colValues;
  private PinotDataBufferMemoryManager _memoryManager;

  @Setup
  public void setUp() {
    _memoryManager = new DirectMemoryManager(BenchmarkDictionary.class.getName());
    // Create a list of values to insert into the hash map
    long[] uniqueColValues = new long[CARDINALITY];
    for (int i = 0; i < uniqueColValues.length; i++) {
      uniqueColValues[i] = (long) (Math.random() * Long.MAX_VALUE);
    }
    _colValues = new Long[ROW_COUNT];
    for (int i = 0; i < _colValues.length; i++) {
      _colValues[i] = uniqueColValues[(int) (Math.random() * CARDINALITY)];
    }
  }

  @TearDown
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkOnHeap0ToN()
      throws IOException {
    try (LongOnHeapMutableDictionary dictionary = new LongOnHeapMutableDictionary()) {
      int value = 0;
      for (Long colValue : _colValues) {
        value += dictionary.index(colValue);
      }
      return value;
    }
  }

  // Start with mid size, with overflow
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkOffHeapMidSizeWithOverflow()
      throws IOException {
    try (LongOffHeapMutableDictionary dictionary = new LongOffHeapMutableDictionary(CARDINALITY / 3, 1000,
        _memoryManager, "longColumn")) {
      int value = 0;
      for (Long colValue : _colValues) {
        value += dictionary.index(colValue);
      }
      return value;
    }
  }

  // Start with max size, no overflow
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkOffHeapMidSizeWithoutOverflow()
      throws IOException {
    try (LongOffHeapMutableDictionary dictionary = new LongOffHeapMutableDictionary(CARDINALITY / 3, 0, _memoryManager,
        "longColumn")) {
      int value = 0;
      for (Long colValue : _colValues) {
        value += dictionary.index(colValue);
      }
      return value;
    }
  }

  // Start with max size, with overflow
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkOffHeapPreSizeWithOverflow()
      throws IOException {
    try (LongOffHeapMutableDictionary dictionary = new LongOffHeapMutableDictionary(CARDINALITY, 1000, _memoryManager,
        "longColumn")) {
      int value = 0;
      for (Long colValue : _colValues) {
        value += dictionary.index(colValue);
      }
      return value;
    }
  }

  // Start with max size, no overflow
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkOffHeapPreSizeWithoutOverflow()
      throws IOException {
    try (LongOffHeapMutableDictionary dictionary = new LongOffHeapMutableDictionary(CARDINALITY, 0, _memoryManager,
        "longColumn")) {
      int value = 0;
      for (Long colValue : _colValues) {
        value += dictionary.index(colValue);
      }
      return value;
    }
  }

  // Start with min size, and grow to full, no overflow
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkOffHeapMinSizeWithoutOverflow()
      throws IOException {
    try (LongOffHeapMutableDictionary dictionary = new LongOffHeapMutableDictionary(10000, 0, _memoryManager,
        "longColumn")) {
      int value = 0;
      for (Long colValue : _colValues) {
        value += dictionary.index(colValue);
      }
      return value;
    }
  }

  // Start with min size, and grow to full with overflow buffer
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkOffHeapMinSizeWithOverflow()
      throws IOException {
    try (LongOffHeapMutableDictionary dictionary = new LongOffHeapMutableDictionary(10000, 1000, _memoryManager,
        "longColumn")) {
      int value = 0;
      for (Long colValue : _colValues) {
        value += dictionary.index(colValue);
      }
      return value;
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkDictionary.class.getSimpleName()).warmupTime(TimeValue.seconds(60))
            .warmupIterations(8).measurementTime(TimeValue.seconds(60)).measurementIterations(8).forks(5);

    new Runner(opt.build()).run();
  }
}
