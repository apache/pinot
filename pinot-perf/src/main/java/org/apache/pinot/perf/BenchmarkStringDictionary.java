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
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOnHeapMutableDictionary;
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

import static java.nio.charset.StandardCharsets.UTF_8;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 30)
@Measurement(iterations = 5, time = 30)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkStringDictionary {
  private static final int NUM_RECORDS = 1_000_000;
  private static final int CARDINALITY = 200_000;
  private static final Random RANDOM = new Random();

  @Param({"8", "16", "32", "64", "128", "256", "512", "1024"})
  private int _maxValueLength;

  private PinotDataBufferMemoryManager _memoryManager;
  private String[] _values;
  private StringOffHeapMutableDictionary _offHeapDictionary;
  private StringOnHeapMutableDictionary _onHeapDictionary;

  @Setup
  public void setUp() {
    _memoryManager = new DirectMemoryManager("");
    _offHeapDictionary =
        new StringOffHeapMutableDictionary(CARDINALITY, CARDINALITY / 10, _memoryManager, null, _maxValueLength / 2);
    _onHeapDictionary = new StringOnHeapMutableDictionary();
    String[] uniqueValues = new String[CARDINALITY];
    for (int i = 0; i < CARDINALITY; i++) {
      String value = generateRandomString(RANDOM.nextInt(_maxValueLength + 1));
      uniqueValues[i] = value;
      _offHeapDictionary.index(value);
      _onHeapDictionary.index(value);
    }
    _values = new String[NUM_RECORDS];
    for (int i = 0; i < NUM_RECORDS; i++) {
      _values[i] = uniqueValues[RANDOM.nextInt(CARDINALITY)];
    }
  }

  @TearDown
  public void tearDown()
      throws Exception {
    _onHeapDictionary.close();
    _offHeapDictionary.close();
    _memoryManager.close();
  }

  // Generates a ascii displayable string of the given length
  private String generateRandomString(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (RANDOM.nextInt(0x7F - 0x20) + 0x20);
    }
    return new String(bytes, UTF_8);
  }

  @Benchmark
  public int offHeapStringDictionaryRead() {
    int sum = 0;
    for (String stringValue : _values) {
      sum += _offHeapDictionary.indexOf(stringValue);
    }
    return sum;
  }

  @Benchmark
  public int onHeapStringDictionaryRead() {
    int sum = 0;
    for (String stringValue : _values) {
      sum += _onHeapDictionary.indexOf(stringValue);
    }
    return sum;
  }

  @Benchmark
  public int offHeapStringDictionaryWrite()
      throws IOException {
    try (StringOffHeapMutableDictionary offHeapDictionary = new StringOffHeapMutableDictionary(CARDINALITY,
        CARDINALITY / 10, _memoryManager, null, _maxValueLength / 2)) {
      int value = 0;
      for (String stringValue : _values) {
        value += offHeapDictionary.index(stringValue);
      }
      return value;
    }
  }

  @Benchmark
  public int onHeapStringDictionaryWrite()
      throws IOException {
    try (StringOnHeapMutableDictionary onHeapDictionary = new StringOnHeapMutableDictionary()) {
      int value = 0;
      for (String stringValue : _values) {
        value += onHeapDictionary.index(stringValue);
      }
      return value;
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkStringDictionary.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
