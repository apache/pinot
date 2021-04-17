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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.reader.impl.FixedBitIntReader;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
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
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class BenchmarkFixedBitIntReader {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkFixedBitIntReader");
  private static final int NUM_VALUES = 5_000_000;
  private static final Random RANDOM = new Random();

  private PinotDataBuffer _dataBuffer;
  private PinotDataBitSet _bitSet;
  private FixedBitIntReader _intReader;

  @Param({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"})
  public int _numBits;

  @Setup
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    FileUtils.forceMkdir(INDEX_DIR);
    File indexFile = new File(INDEX_DIR, "bit-" + _numBits);
    int maxValue = _numBits < 31 ? 1 << _numBits : Integer.MAX_VALUE;
    try (FixedBitSVForwardIndexWriter indexWriter = new FixedBitSVForwardIndexWriter(indexFile, NUM_VALUES, _numBits)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        indexWriter.putDictId(RANDOM.nextInt(maxValue));
      }
    }
    _dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    _bitSet = new PinotDataBitSet(_dataBuffer);
    _intReader = FixedBitIntReader.getReader(_dataBuffer, _numBits);
  }

  @TearDown
  public void tearDown() throws Exception {
    _dataBuffer.close();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Benchmark
  public int bitset() {
    int sum = 0;
    for (int i = 0; i < NUM_VALUES - 32; i++) {
      sum += _bitSet.readInt(i, _numBits);
    }
    return sum;
  }

  @Benchmark
  public int intReader() {
    int sum = 0;
    for (int i = 0; i < NUM_VALUES - 32; i++) {
      sum += _intReader.read(i);
    }
    return sum;
  }

  @Benchmark
  public int intReaderUnchecked() {
    int sum = 0;
    for (int i = 0; i < NUM_VALUES - 32; i++) {
      sum += _intReader.readUnchecked(i);
    }
    return sum;
  }

  @Benchmark
  public int intReaderBulk() {
    int sum = 0;
    int[] buffer = new int[32];
    for (int i = 0; i < NUM_VALUES - 32; i += 32) {
      _intReader.read32(i, buffer, 0);
      for (int j = 0; j < 32; j++) {
        sum += buffer[j];
      }
    }
    return sum;
  }

  public static void main(String[] args) throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkFixedBitIntReader.class.getSimpleName()).build()).run();
  }
}
