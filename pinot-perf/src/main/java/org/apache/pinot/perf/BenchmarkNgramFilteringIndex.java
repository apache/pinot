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

import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeNgramFilteringIndex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * This benchmark is a benchmark for testing the performance of N-gram filtering index vs a pure regex matcher
 */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@State(Scope.Benchmark)
public class BenchmarkNgramFilteringIndex {
  public static final String PREFIX = "somelonglongveryverylongword";
  RealtimeNgramFilteringIndex _realtimeNgramFilteringIndex;

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkNgramFilteringIndex.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    _realtimeNgramFilteringIndex = new RealtimeNgramFilteringIndex("col", 2, 3);
    // Load the index with 10000 words
    for (int i = 0; i < 10000; i++) {
      _realtimeNgramFilteringIndex.add(PREFIX + i);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.All)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkTextMatchingUsingNgram() {
    // First retrieve the docIds using the N-gram index
    MutableRoaringBitmap map = _realtimeNgramFilteringIndex.getDocIds("ord78");
    IntIterator intIterator = map.getIntIterator();
    // Next doing regex matching on the docIds to validate the results
    while (intIterator.hasNext()) {
      int docId = intIterator.next();
      // Simulate regex validate on th
      (PREFIX + docId).matches(".*ord78.*");
    }
    return 0;
  }

  @Benchmark
  @BenchmarkMode(Mode.All)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int benchmarkMatchingViaRegexScan() {
    // Matching the documents using regex scan
    for (int i = 0; i < 10000; i++) {
      (PREFIX + i).matches(".*ord78.*");
    }
    return 0;
  }

  public void tearDown()
      throws Exception {
    _realtimeNgramFilteringIndex.close();
  }
}
