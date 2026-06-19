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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;
import org.apache.pinot.segment.local.segment.index.readers.LuceneFSTIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.LuceneIFSTIndexReader;
import org.apache.pinot.segment.local.utils.fst.FSTBuilder;
import org.apache.pinot.segment.local.utils.fst.IFSTBuilder;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmark for FST/IFST regexp matching (REGEXP_LIKE backed by an FST index).
 *
 * This exercises {@code LuceneFSTIndexReader#getDictIds} / {@code LuceneIFSTIndexReader#getDictIds}, the production
 * path used by the FST-based regexp predicate evaluator. It is designed to highlight the memory footprint of the
 * automaton-over-FST traversal for high-cardinality, long-string columns (the case that caused server heap OOMs).
 *
 * Run with the GC profiler to see the allocation reduction, e.g.:
 *   java -cp 'pinot-perf/target/pinot-perf-pkg/lib/*' org.openjdk.jmh.Main \
 *     org.apache.pinot.perf.BenchmarkFSTRegexpMatcher -prof gc -wi 2 -i 3 -f 1 -r 5s -w 5s
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@State(Scope.Benchmark)
public class BenchmarkFSTRegexpMatcher {

  /// Number of distinct keys (column cardinality).
  @Param({"500000"})
  public int _cardinality;

  /// Length of the key prefix padding, simulating a "long string" column.
  @Param({"40"})
  public int _keyLength;

  /// Regexp patterns: a broad `.*` (worst case, matches everything), a selective prefix, and a selective suffix.
  @Param({".*", "key0001.*", ".*9999"})
  public String _regex;

  private File _tempDir;
  private PinotDataBuffer _fstBuffer;
  private PinotDataBuffer _ifstBuffer;
  private LuceneFSTIndexReader _fstReader;
  private LuceneIFSTIndexReader _ifstReader;

  @Setup(Level.Trial)
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "BenchmarkFSTRegexpMatcher");
    FileUtils.deleteDirectory(_tempDir);
    FileUtils.forceMkdir(_tempDir);

    // Build a high-cardinality dictionary of long string keys: "key" + zero-padded id, padded to _keyLength.
    SortedMap<String, Integer> entries = new TreeMap<>();
    for (int i = 0; i < _cardinality; i++) {
      String key = String.format("key%0" + (_keyLength - 3) + "d", i);
      entries.put(key, i);
    }

    FST<Long> fst = FSTBuilder.buildFST(entries);
    File fstFile = new File(_tempDir, "fst.lucene");
    try (FileOutputStream outputStream = new FileOutputStream(fstFile);
        OutputStreamDataOutput dataOutput = new OutputStreamDataOutput(outputStream)) {
      fst.save(dataOutput, dataOutput);
    }
    _fstBuffer = PinotDataBuffer.loadBigEndianFile(fstFile);
    _fstReader = new LuceneFSTIndexReader(_fstBuffer);

    FST<BytesRef> ifst = IFSTBuilder.buildIFST(entries);
    File ifstFile = new File(_tempDir, "ifst.lucene");
    try (FileOutputStream outputStream = new FileOutputStream(ifstFile);
        OutputStreamDataOutput dataOutput = new OutputStreamDataOutput(outputStream)) {
      ifst.save(dataOutput, dataOutput);
    }
    _ifstBuffer = PinotDataBuffer.loadBigEndianFile(ifstFile);
    _ifstReader = new LuceneIFSTIndexReader(_ifstBuffer);
  }

  @TearDown(Level.Trial)
  public void tearDown()
      throws IOException {
    if (_fstReader != null) {
      _fstReader.close();
    }
    if (_ifstReader != null) {
      _ifstReader.close();
    }
    // The readers' close() is a no-op; the off-heap buffers own the mapped memory and must be closed explicitly,
    // otherwise the mapping stays live and can prevent the temp dir from being deleted.
    if (_fstBuffer != null) {
      _fstBuffer.close();
    }
    if (_ifstBuffer != null) {
      _ifstBuffer.close();
    }
    FileUtils.deleteDirectory(_tempDir);
  }

  @Benchmark
  public void fstGetDictIds(Blackhole blackhole) {
    blackhole.consume(_fstReader.getDictIds(_regex));
  }

  @Benchmark
  public void ifstGetDictIds(Blackhole blackhole) {
    blackhole.consume(_ifstReader.getDictIds(_regex));
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkFSTRegexpMatcher.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
