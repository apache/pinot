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
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitSVForwardIndexReaderV2;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
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


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class BenchmarkFixedBitSVForwardIndexReader {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkPinotDataBitSet");
  private static final int NUM_VALUES = 100_000;
  private static final int NUM_DOC_IDS = DocIdSetPlanNode.MAX_DOC_PER_CALL;
  private static final Random RANDOM = new Random();

  private PinotDataBuffer _dataBuffer;
  private FixedBitSVForwardIndexReader _reader;
  private FixedBitSVForwardIndexReaderV2 _readerV2;

  private final int[] _sequentialDocIds = new int[NUM_DOC_IDS];
  private final int[] _denseDocIds = new int[NUM_DOC_IDS];
  private final int[] _sparseDocIds = new int[NUM_DOC_IDS];
  private final int[] _dictIdBuffer = new int[NUM_DOC_IDS];

  @Param({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"})
  public int _numBits;

  @Setup
  public void setUp()
      throws Exception {
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
    _reader = new FixedBitSVForwardIndexReader(_dataBuffer, NUM_VALUES, _numBits);
    _readerV2 = new FixedBitSVForwardIndexReaderV2(_dataBuffer, NUM_VALUES, _numBits);

    int sequentialDocId = RANDOM.nextInt(32);
    int denseDocId = RANDOM.nextInt(32);
    int sparseDocId = RANDOM.nextInt(32);
    for (int i = 0; i < NUM_DOC_IDS; i++) {
      _sequentialDocIds[i] = sequentialDocId;
      _denseDocIds[i] = denseDocId;
      _sparseDocIds[i] = sparseDocId;
      sequentialDocId++;
      denseDocId += 1 + RANDOM.nextInt(2);
      sparseDocId += 5 + RANDOM.nextInt(6);
    }
  }

  @TearDown
  public void tearDown()
      throws Exception {
    _dataBuffer.close();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Benchmark
  public int readerSequential() {
    _reader.readDictIds(_sequentialDocIds, NUM_DOC_IDS, _dictIdBuffer, null);
    return _dictIdBuffer[0];
  }

  @Benchmark
  public int readerDense() {
    _reader.readDictIds(_denseDocIds, NUM_DOC_IDS, _dictIdBuffer, null);
    return _dictIdBuffer[0];
  }

  @Benchmark
  public int readerSparse() {
    _reader.readDictIds(_sparseDocIds, NUM_DOC_IDS, _dictIdBuffer, null);
    return _dictIdBuffer[0];
  }

  @Benchmark
  public int readerV2Sequential() {
    _readerV2.readDictIds(_sequentialDocIds, NUM_DOC_IDS, _dictIdBuffer, null);
    return _dictIdBuffer[0];
  }

  @Benchmark
  public int readerV2Dense() {
    _readerV2.readDictIds(_denseDocIds, NUM_DOC_IDS, _dictIdBuffer, null);
    return _dictIdBuffer[0];
  }

  @Benchmark
  public int readerV2Sparse() {
    _readerV2.readDictIds(_sparseDocIds, NUM_DOC_IDS, _dictIdBuffer, null);
    return _dictIdBuffer[0];
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkFixedBitSVForwardIndexReader.class.getSimpleName()).build()).run();
  }
}
