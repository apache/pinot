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
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitSVForwardIndexReaderV2;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
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
import org.openjdk.jmh.infra.Blackhole;


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class BenchmarkFixedBitSVForwardIndexReaderMixedCardinalities {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "BenchmarkFixedBitSVForwardIndexReaderMixedCardinalities");
  private static final int NUM_VALUES = 100_000;
  private static final int NUM_DOC_IDS = DocIdSetPlanNode.MAX_DOC_PER_CALL;
  private static final Random RANDOM = new Random();

  private PinotDataBuffer _dataBuffer0;
  private FixedBitSVForwardIndexReaderV2 _reader0;
  private PinotDataBuffer _dataBuffer1;
  private FixedBitSVForwardIndexReaderV2 _reader1;
  private PinotDataBuffer _dataBuffer2;
  private FixedBitSVForwardIndexReaderV2 _reader2;

  private final int[] _sequentialDocIds = new int[NUM_DOC_IDS];
  private final int[] _denseDocIds = new int[NUM_DOC_IDS];
  private final int[] _sparseDocIds = new int[NUM_DOC_IDS];
  private final int[] _dictIdBuffer0 = new int[NUM_DOC_IDS];
  private final int[] _dictIdBuffer1 = new int[NUM_DOC_IDS];
  private final int[] _dictIdBuffer2 = new int[NUM_DOC_IDS];

  @Param({
      "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
      "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
  })
  public int _numBits0;

  @Param({
      "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
      "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
  })
  public int _numBits1;

  @Param({
      "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
      "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
  })
  public int _numBits2;

  @Setup
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    FileUtils.forceMkdir(INDEX_DIR);
    File indexFile0 = new File(INDEX_DIR, "bit-" + _numBits0 + "-0");
    File indexFile1 = new File(INDEX_DIR, "bit-" + _numBits1 + "-1");
    File indexFile2 = new File(INDEX_DIR, "bit-" + _numBits2 + "-2");
    int maxValue0 = _numBits0 < 31 ? 1 << _numBits0 : Integer.MAX_VALUE;
    int maxValue1 = _numBits1 < 31 ? 1 << _numBits1 : Integer.MAX_VALUE;
    int maxValue2 = _numBits2 < 31 ? 1 << _numBits2 : Integer.MAX_VALUE;
    try (
        FixedBitSVForwardIndexWriter indexWriter0 = new FixedBitSVForwardIndexWriter(indexFile0, NUM_VALUES, _numBits0);
        FixedBitSVForwardIndexWriter indexWriter1 = new FixedBitSVForwardIndexWriter(indexFile1, NUM_VALUES, _numBits1);
        FixedBitSVForwardIndexWriter indexWriter2 = new FixedBitSVForwardIndexWriter(indexFile2, NUM_VALUES,
            _numBits2)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        indexWriter0.putDictId(RANDOM.nextInt(maxValue0));
        indexWriter1.putDictId(RANDOM.nextInt(maxValue1));
        indexWriter2.putDictId(RANDOM.nextInt(maxValue2));
      }
    }
    _dataBuffer0 = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile0);
    _reader0 = new FixedBitSVForwardIndexReaderV2(_dataBuffer0, NUM_VALUES, _numBits0);
    _dataBuffer1 = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile1);
    _reader1 = new FixedBitSVForwardIndexReaderV2(_dataBuffer1, NUM_VALUES, _numBits1);
    _dataBuffer2 = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile2);
    _reader2 = new FixedBitSVForwardIndexReaderV2(_dataBuffer2, NUM_VALUES, _numBits2);

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
    _dataBuffer0.close();
    _dataBuffer1.close();
    _dataBuffer2.close();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void sequential(Blackhole bh) {
    _reader0.readDictIds(_sequentialDocIds, NUM_DOC_IDS, _dictIdBuffer0, null);
    bh.consume(_dictIdBuffer0);
    _reader1.readDictIds(_sequentialDocIds, NUM_DOC_IDS, _dictIdBuffer1, null);
    bh.consume(_dictIdBuffer1);
    _reader2.readDictIds(_sequentialDocIds, NUM_DOC_IDS, _dictIdBuffer2, null);
    bh.consume(_dictIdBuffer2);
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void dense(Blackhole bh) {
    _reader0.readDictIds(_denseDocIds, NUM_DOC_IDS, _dictIdBuffer0, null);
    bh.consume(_dictIdBuffer0);
    _reader1.readDictIds(_denseDocIds, NUM_DOC_IDS, _dictIdBuffer1, null);
    bh.consume(_dictIdBuffer1);
    _reader2.readDictIds(_denseDocIds, NUM_DOC_IDS, _dictIdBuffer2, null);
    bh.consume(_dictIdBuffer2);
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public void sparse(Blackhole bh) {
    _reader0.readDictIds(_sparseDocIds, NUM_DOC_IDS, _dictIdBuffer0, null);
    bh.consume(_dictIdBuffer0);
    _reader1.readDictIds(_sparseDocIds, NUM_DOC_IDS, _dictIdBuffer1, null);
    bh.consume(_dictIdBuffer1);
    _reader2.readDictIds(_sparseDocIds, NUM_DOC_IDS, _dictIdBuffer2, null);
    bh.consume(_dictIdBuffer2);
  }
}
