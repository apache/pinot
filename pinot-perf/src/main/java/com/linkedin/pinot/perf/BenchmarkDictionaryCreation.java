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

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
public class BenchmarkDictionaryCreation {
  private static final FieldSpec INT_FIELD = new DimensionFieldSpec("int", FieldSpec.DataType.INT, true);
  private static final FieldSpec LONG_FIELD = new DimensionFieldSpec("long", FieldSpec.DataType.LONG, true);
  private static final FieldSpec FLOAT_FIELD = new DimensionFieldSpec("float", FieldSpec.DataType.FLOAT, true);
  private static final FieldSpec DOUBLE_FIELD = new DimensionFieldSpec("double", FieldSpec.DataType.DOUBLE, true);
  private static final FieldSpec STRING_FIELD = new DimensionFieldSpec("string", FieldSpec.DataType.STRING, true);
  private static final int CARDINALITY = 1_000_000;
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkDictionaryCreation");

  private final int[] _sortedInts = new int[CARDINALITY];
  private final long[] _sortedLongs = new long[CARDINALITY];
  private final float[] _sortedFloats = new float[CARDINALITY];
  private final double[] _sortedDoubles = new double[CARDINALITY];
  private final String[] _sortedStrings = new String[CARDINALITY];

  @Setup
  public void setUp() throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
    for (int i = 0; i < CARDINALITY; i++) {
      _sortedInts[i] = i;
      _sortedLongs[i] = i;
      _sortedFloats[i] = i;
      _sortedDoubles[i] = i;
      _sortedStrings[i] = String.valueOf(i);
    }
    Arrays.sort(_sortedStrings);
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkIntDictionaryCreation() throws IOException {
    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_sortedInts, INT_FIELD, INDEX_DIR)) {
      dictionaryCreator.build();
      return dictionaryCreator.indexOfSV(0);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkLongDictionaryCreation() throws IOException {
    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_sortedLongs, LONG_FIELD,
        INDEX_DIR)) {
      dictionaryCreator.build();
      return dictionaryCreator.indexOfSV(0L);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkFloatDictionaryCreation() throws IOException {
    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_sortedFloats, FLOAT_FIELD,
        INDEX_DIR)) {
      dictionaryCreator.build();
      return dictionaryCreator.indexOfSV(0f);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkDoubleDictionaryCreation() throws IOException {
    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_sortedDoubles, DOUBLE_FIELD,
        INDEX_DIR)) {
      dictionaryCreator.build();
      return dictionaryCreator.indexOfSV(0d);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkStringDictionaryCreation() throws IOException {
    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_sortedStrings, STRING_FIELD,
        INDEX_DIR)) {
      dictionaryCreator.build();
      return dictionaryCreator.indexOfSV("0");
    }
  }

  @TearDown
  public void tearDown() throws Exception {
    FileUtils.forceDelete(INDEX_DIR);
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(BenchmarkDictionaryCreation.class.getSimpleName())
        .warmupTime(TimeValue.seconds(5))
        .warmupIterations(2)
        .measurementTime(TimeValue.seconds(5))
        .measurementIterations(3)
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
