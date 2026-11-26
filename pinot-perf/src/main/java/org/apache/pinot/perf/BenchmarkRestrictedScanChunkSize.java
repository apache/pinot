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
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.RestrictedScanDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SVScanDocIdIterator;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitSVForwardIndexReaderV2;
import org.apache.pinot.segment.spi.Constants;
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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Measures how the size of the first chunk of [RestrictedScanDocIdIterator] changes its cost.
///
/// A small first chunk makes a query that stops early cheap, because it scans few candidates before it has its rows. A
/// large first chunk makes a query that reads every candidate cheap, because it calls the scan fewer times. The chunk
/// doubles after each call, so the first chunk mostly matters for the queries that stop early. `10000` is
/// `MAX_DOC_PER_CALL`: the chunk never grows.
///
/// The scan is a real [SVScanDocIdIterator] on a fixed-bit forward index that matches about a third of the documents.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class BenchmarkRestrictedScanChunkSize {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkRestrictedScanChunkSize");
  private static final int NUM_DOCS = 2_000_000;
  private static final int NUM_DICT_IDS = 1024;

  @Param({"64", "256", "1024", "10000"})
  private int _initialChunkSize;

  /// The number of matching documents that the consumer reads. `0` reads them all.
  @Param({"10", "1000", "0"})
  private int _limit;

  /// Fraction of documents in the candidate bitmap.
  @Param({"0.01", "0.5"})
  private double _candidateSelectivity;

  private ImmutableRoaringBitmap _candidateDocIds;
  private PinotDataBuffer _dataBuffer;
  private FixedBitSVForwardIndexReaderV2 _forwardIndexReader;
  private BenchmarkScanDocIdIterators.DummyPredicateEvaluator _predicateEvaluator;

  public static void main(String[] args)
      throws RunnerException {
    new Runner(new OptionsBuilder().include(BenchmarkRestrictedScanChunkSize.class.getSimpleName()).build()).run();
  }

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    FileUtils.forceMkdir(INDEX_DIR);
    File indexFile = new File(INDEX_DIR, "fwd-index");
    Random random = new Random(42);
    int numBits = 32 - Integer.numberOfLeadingZeros(NUM_DICT_IDS - 1);
    try (FixedBitSVForwardIndexWriter indexWriter = new FixedBitSVForwardIndexWriter(indexFile, NUM_DOCS, numBits)) {
      for (int i = 0; i < NUM_DOCS; i++) {
        indexWriter.putDictId(random.nextInt(NUM_DICT_IDS));
      }
    }
    _dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    _forwardIndexReader = new FixedBitSVForwardIndexReaderV2(_dataBuffer, NUM_DOCS, numBits);
    _predicateEvaluator = new BenchmarkScanDocIdIterators.DummyPredicateEvaluator(NUM_DICT_IDS / 3);
    MutableRoaringBitmap candidateDocIds = new MutableRoaringBitmap();
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      if (random.nextDouble() < _candidateSelectivity) {
        candidateDocIds.add(docId);
      }
    }
    _candidateDocIds = candidateDocIds;
  }

  @TearDown(Level.Trial)
  public void tearDown()
      throws Exception {
    _dataBuffer.close();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Benchmark
  public int restrictedScan(Blackhole bh) {
    SVScanDocIdIterator scan = new SVScanDocIdIterator(_predicateEvaluator, _forwardIndexReader, NUM_DOCS);
    int limit = _limit > 0 ? _limit : Integer.MAX_VALUE;
    int numDocs = 0;
    try (BlockDocIdIterator docIdIterator = new RestrictedScanDocIdIterator(_candidateDocIds.getIntIterator(),
        List.of(scan), _initialChunkSize)) {
      int docId;
      while ((docId = docIdIterator.next()) != Constants.EOF) {
        bh.consume(docId);
        if (++numDocs >= limit) {
          break;
        }
      }
    }
    return numDocs;
  }
}
