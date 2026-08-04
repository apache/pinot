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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
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


/**
 * Micro-benchmark for resolving a large {@code IN} list against a high-cardinality STRING dictionary whose values are
 * bare signed 64-bit integers stored as strings (e.g. {@code "-7930618564724103528"}) with no shared prefix. This
 * mirrors the production hot path
 * {@code PredicateUtils.getDictIdSet -> BaseImmutableDictionary.getDictIdsDivideBinarySearch ->
 * FixedByteValueReaderWriter.compareUtf8Bytes -> ValueReaderComparisons.mismatch}.
 *
 * Run the same benchmark on the baseline tree and on the patched tree to compare throughput and, with
 * {@code -prof gc}, the per-probe allocation rate.
 */
@State(Scope.Benchmark)
public class BenchmarkStringInListLookup {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkStringInListLookup");
  private static final String COLUMN_NAME = "column";
  private static final long SEED = 42L;

  @Param({"100000", "1000000"})
  private int _cardinality;

  @Param({"100", "500"})
  private int _inListSize;

  private StringDictionary _dictionary;
  // Sorted (lexicographically) IN-list mixing values present and absent in the dictionary, matching the input shape
  // that PredicateUtils hands to getDictIds(..., DIVIDE_BINARY_SEARCH).
  private List<String> _inList;

  @Setup
  public void setUp()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
    Random random = new Random(SEED);

    // Generate `_cardinality` unique bare-numeric strings (full signed-long range -> no shared prefix).
    Set<Long> uniqueLongs = new HashSet<>(_cardinality * 2);
    while (uniqueLongs.size() < _cardinality) {
      uniqueLongs.add(random.nextLong());
    }
    String[] sortedValues = new String[_cardinality];
    int idx = 0;
    for (long value : uniqueLongs) {
      sortedValues[idx++] = Long.toString(value);
    }
    // The dictionary stores values sorted lexicographically, so sort the same way here.
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

    // Build an IN-list: ~half present (sampled from the dictionary), ~half absent (random longs not in the dictionary).
    List<String> inList = new ArrayList<>(_inListSize);
    int numPresent = _inListSize / 2;
    for (int i = 0; i < numPresent; i++) {
      inList.add(sortedValues[random.nextInt(_cardinality)]);
    }
    while (inList.size() < _inListSize) {
      long candidate = random.nextLong();
      if (!uniqueLongs.contains(candidate)) {
        inList.add(Long.toString(candidate));
      }
    }
    inList.sort(null);
    _inList = inList;
  }

  @TearDown
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  /** End-to-end IN-list resolution as used by the IN predicate evaluator. */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkDivideBinarySearch() {
    IntSet dictIds = new IntOpenHashSet();
    _dictionary.getDictIds(_inList, dictIds, Dictionary.SortedBatchLookupAlgorithm.DIVIDE_BINARY_SEARCH);
    return dictIds.size();
  }

  /** Isolates the comparison primitive: one independent binary search per value via indexOf. */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkIndexOf() {
    int sum = 0;
    List<String> inList = _inList;
    for (int i = 0, n = inList.size(); i < n; i++) {
      sum += _dictionary.indexOf(inList.get(i));
    }
    return sum;
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkStringInListLookup.class.getSimpleName()).warmupTime(TimeValue.seconds(3))
            .warmupIterations(3).measurementTime(TimeValue.seconds(5)).measurementIterations(5).forks(1)
            .addProfiler("gc");
    new Runner(opt.build()).run();
  }
}
