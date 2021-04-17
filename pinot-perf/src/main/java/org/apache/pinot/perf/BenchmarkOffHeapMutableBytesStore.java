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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MutableOffHeapByteArrayStore;
import org.apache.pinot.segment.local.realtime.impl.dictionary.OffHeapMutableBytesStore;
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
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 30)
@Measurement(iterations = 5, time = 30)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkOffHeapMutableBytesStore {
  private static final int NUM_VALUES = 1_000_000;

  @Param({"8", "16", "32", "64", "128", "256", "512", "1024"})
  private int _maxValueLength;

  private PinotDataBufferMemoryManager _memoryManager;
  private byte[][] _values;
  private OffHeapMutableBytesStore _offHeapMutableBytesStore;
  private MutableOffHeapByteArrayStore _mutableOffHeapByteArrayStore;

  @Setup
  public void setUp() {
    _memoryManager = new DirectMemoryManager("");

    _values = new byte[NUM_VALUES][];
    Random random = new Random();
    for (int i = 0; i < NUM_VALUES; i++) {
      int valueLength = random.nextInt(_maxValueLength + 1);
      byte[] value = new byte[valueLength];
      random.nextBytes(value);
      _values[i] = value;
    }

    _offHeapMutableBytesStore = new OffHeapMutableBytesStore(_memoryManager, null);
    for (byte[] value : _values) {
      _offHeapMutableBytesStore.add(value);
    }
    System.out
        .println("\nBytes allocated for OffHeapMutableBytesStore: " + _offHeapMutableBytesStore.getTotalBufferSize());

    _mutableOffHeapByteArrayStore =
        new MutableOffHeapByteArrayStore(_memoryManager, null, NUM_VALUES, _maxValueLength / 2);
    for (byte[] value : _values) {
      _mutableOffHeapByteArrayStore.add(value);
    }
    System.out.println("\nBytes allocated for MutableOffHeapByteArrayStore: "
        + _mutableOffHeapByteArrayStore.getTotalOffHeapMemUsed());
  }

  @TearDown
  public void tearDown() throws IOException {
    _mutableOffHeapByteArrayStore.close();
    _offHeapMutableBytesStore.close();
    _memoryManager.close();
  }

  @Benchmark
  public int offHeapMutableBytesStoreRead() {
    int sum = 0;
    for (int i = 0; i < NUM_VALUES; i++) {
      sum += _offHeapMutableBytesStore.get(i).length;
    }
    return sum;
  }

  @Benchmark
  public int mutableOffHeapByteArrayStoreRead() {
    int sum = 0;
    for (int i = 0; i < NUM_VALUES; i++) {
      sum += _mutableOffHeapByteArrayStore.get(i).length;
    }
    return sum;
  }

  @Benchmark
  public int offHeapMutableBytesStoreWrite() throws IOException {
    int sum = 0;
    try (OffHeapMutableBytesStore offHeapMutableBytesStore = new OffHeapMutableBytesStore(_memoryManager, null)) {
      for (byte[] value : _values) {
        sum += offHeapMutableBytesStore.add(value);
      }
    }
    return sum;
  }

  @Benchmark
  public int mutableOffHeapByteArrayStoreWrite() throws IOException {
    int sum = 0;
    try (MutableOffHeapByteArrayStore mutableOffHeapByteArrayStore =
        new MutableOffHeapByteArrayStore(_memoryManager, null, NUM_VALUES, _maxValueLength / 2)) {
      for (byte[] value : _values) {
        sum += mutableOffHeapByteArrayStore.add(value);
      }
    }
    return sum;
  }

  public static void main(String[] args) throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkOffHeapMutableBytesStore.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
