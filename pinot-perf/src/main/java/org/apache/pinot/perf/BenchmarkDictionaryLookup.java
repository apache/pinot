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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
public class BenchmarkDictionaryLookup {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkDictionaryLookup");
  private static final int MAX_LENGTH = 10;
  private static final String COLUMN_NAME = "column";

  @Param({"100", "1000", "10000", "100000", "1000000"})
  private int _cardinality;

  @Param({"1", "2", "4", "8", "16", "32", "64", "100"})
  private int _lookupPercentage;

  private StringDictionary _dictionary;
  private List<String> _lookupValues;

  @Setup
  public void setUp()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
    Set<String> uniqueValues = new HashSet<>();
    while (uniqueValues.size() < _cardinality) {
      uniqueValues.add(RandomStringUtils.randomAscii(MAX_LENGTH));
    }
    String[] sortedValues = uniqueValues.toArray(new String[0]);
    Arrays.sort(sortedValues);
    int maxLength;
    try (SegmentDictionaryCreator creator = new SegmentDictionaryCreator(
        new DimensionFieldSpec(COLUMN_NAME, DataType.STRING, true), INDEX_DIR, false)) {
      creator.build(sortedValues);
      maxLength = creator.getNumBytesPerEntry();
    }
    _dictionary = new StringDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(new File(INDEX_DIR, COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)),
        _cardinality, maxLength);
    int numLookupValues = _cardinality * _lookupPercentage / 100;
    if (numLookupValues == _cardinality) {
      _lookupValues = Arrays.asList(sortedValues);
    } else {
      IntSet lookupValueIds = new IntOpenHashSet();
      while (lookupValueIds.size() < numLookupValues) {
        lookupValueIds.add((int) (Math.random() * _cardinality));
      }
      int[] sortedValueIds = lookupValueIds.toIntArray();
      Arrays.sort(sortedValueIds);
      _lookupValues = new ArrayList<>(numLookupValues);
      for (int lookupValueId : sortedValueIds) {
        _lookupValues.add(sortedValues[lookupValueId]);
      }
    }
  }

  @TearDown
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public int benchmarkDivideBinarySearch() {
    IntSet dictIds = new IntOpenHashSet();
    _dictionary.getDictIds(_lookupValues, dictIds, Dictionary.SortedBatchLookupAlgorithm.DIVIDE_BINARY_SEARCH);
    return dictIds.size();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public int benchmarkScan() {
    IntSet dictIds = new IntOpenHashSet();
    _dictionary.getDictIds(_lookupValues, dictIds, Dictionary.SortedBatchLookupAlgorithm.SCAN);
    return dictIds.size();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public int benchmarkPlainBinarySearch() {
    IntSet dictIds = new IntOpenHashSet();
    _dictionary.getDictIds(_lookupValues, dictIds);
    return dictIds.size();
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkDictionaryLookup.class.getSimpleName()).warmupTime(TimeValue.seconds(3))
            .warmupIterations(1).measurementTime(TimeValue.seconds(5)).measurementIterations(1).forks(1);
    new Runner(opt.build()).run();
  }
}
