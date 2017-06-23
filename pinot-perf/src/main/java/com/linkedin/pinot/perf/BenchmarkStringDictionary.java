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

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.HotspotMemoryProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import com.linkedin.pinot.core.realtime.impl.dictionary.StringOffHeapMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.StringOnHeapMutableDictionary;


@State(Scope.Benchmark)
public class BenchmarkStringDictionary {
  private final int ROW_COUNT = 2_500_000;
  private String[] stringValues;
  private String[] uniqueStrings;
  private final int CARDINALITY = 1_000_000;
  private final int MAX_STRING_LEN = 32;

  @Setup
  public void setUp() {
    // Create a list of values to insert into the hash map
    uniqueStrings = new String[CARDINALITY];
    Random r = new Random();
    for (int i = 0; i < uniqueStrings.length; i++) {
      uniqueStrings[i] = generateRandomString(r, r.nextInt(MAX_STRING_LEN+1));
    }
    stringValues = new String[ROW_COUNT];
    for (int i = 0; i < stringValues.length; i++) {
      int u = r.nextInt(CARDINALITY);
      stringValues[i] = uniqueStrings[u];
    }
  }

  // Generates a ascii displayable string of given length
  private String generateRandomString(Random r, final int len) {
    byte[] bytes = new byte[len];
    for (int i = 0; i < len; i++) {
      bytes[i] = (byte)(r.nextInt(92) + 32);
    }
    return new String(bytes);
  }


  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public StringOffHeapMutableDictionary benchmarkOffStringDictionary() {
    StringOffHeapMutableDictionary dictionary = new StringOffHeapMutableDictionary(10, 10);

    for (int i = 0; i < stringValues.length; i++) {
      dictionary.index(stringValues[i]);
    }

    return dictionary;
  }

  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public StringOnHeapMutableDictionary benchmarkOnHeapStringDictionary() {
    StringOnHeapMutableDictionary dictionary = new StringOnHeapMutableDictionary();

    for (int i = 0; i < stringValues.length; i++) {
      dictionary.index(stringValues[i]);
    }

    return dictionary;
  }

  public static void main(String[] args) throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkDictionary.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .addProfiler(HotspotMemoryProfiler.class)
        .warmupTime(TimeValue.seconds(60))
        .warmupIterations(8)
        .measurementTime(TimeValue.seconds(60))
        .measurementIterations(8)
        .forks(5)
        ;

    new Runner(opt.build()).run();
  }
}
