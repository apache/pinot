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

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOnHeapMutableDictionary;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.index.readers.OnHeapStringDictionary;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
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
public class BenchmarkStringDictionary {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkStringDictionary");
  private static final int NUM_RECORDS = 1_000_000;
  private static final int CARDINALITY = 200_000;
  private static final Random RANDOM = new Random();

  @Param({"8", "16", "32", "64", "128", "256", "512", "1024"})
  private int _maxValueLength;

  private PinotDataBufferMemoryManager _memoryManager;
  private String[] _values;
  private StringDictionary _stringDictionary;
  private OnHeapStringDictionary _onHeapStringDictionary;
  private StringOffHeapMutableDictionary _offHeapMutableDictionary;
  private StringOnHeapMutableDictionary _onHeapMutableDictionary;

  @Setup
  public void setUp()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
    FileUtils.forceMkdir(INDEX_DIR);
    _memoryManager = new DirectMemoryManager("");
    _offHeapMutableDictionary =
        new StringOffHeapMutableDictionary(CARDINALITY, CARDINALITY / 10, _memoryManager, null, _maxValueLength / 2);
    _onHeapMutableDictionary = new StringOnHeapMutableDictionary();
    TreeSet<String> uniqueValues = new TreeSet<>();
    while (uniqueValues.size() < CARDINALITY) {
      String value = RandomStringUtils.randomAscii(RANDOM.nextInt(_maxValueLength + 1));
      if (uniqueValues.add(value)) {
        _offHeapMutableDictionary.index(value);
        _onHeapMutableDictionary.index(value);
      }
    }
    String[] sortedUniqueValues = uniqueValues.toArray(new String[0]);
    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(sortedUniqueValues,
        new DimensionFieldSpec("string", DataType.STRING, true), INDEX_DIR)) {
      dictionaryCreator.build();
      PinotDataBuffer dataBuffer =
          PinotDataBuffer.mapReadOnlyBigEndianFile(new File(INDEX_DIR, "string" + V1Constants.Dict.FILE_EXTENSION));
      int numBytesPerValue = dictionaryCreator.getNumBytesPerEntry();
      _stringDictionary = new StringDictionary(dataBuffer, CARDINALITY, numBytesPerValue, (byte) 0);
      _onHeapStringDictionary = new OnHeapStringDictionary(dataBuffer, CARDINALITY, numBytesPerValue, (byte) 0);
    }
    _values = new String[NUM_RECORDS];
    for (int i = 0; i < NUM_RECORDS; i++) {
      _values[i] = sortedUniqueValues[RANDOM.nextInt(CARDINALITY)];
    }
  }

  @TearDown
  public void tearDown()
      throws Exception {
    _onHeapMutableDictionary.close();
    _offHeapMutableDictionary.close();
    _memoryManager.close();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Benchmark
  public int stringDictionaryRead() {
    int sum = 0;
    for (String stringValue : _values) {
      sum += _stringDictionary.indexOf(stringValue);
    }
    return sum;
  }

  @Benchmark
  public int onHeapStringDictionaryRead() {
    int sum = 0;
    for (String stringValue : _values) {
      sum += _onHeapStringDictionary.indexOf(stringValue);
    }
    return sum;
  }

  @Benchmark
  public int offHeapMutableDictionaryRead() {
    int sum = 0;
    for (String stringValue : _values) {
      sum += _offHeapMutableDictionary.indexOf(stringValue);
    }
    return sum;
  }

  @Benchmark
  public int onHeapMutableDictionaryRead() {
    int sum = 0;
    for (String stringValue : _values) {
      sum += _onHeapMutableDictionary.indexOf(stringValue);
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
