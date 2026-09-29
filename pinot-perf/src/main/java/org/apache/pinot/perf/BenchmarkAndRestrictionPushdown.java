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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.OrDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.RangelessBitmapDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.RestrictedScanDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SVScanDocIdIterator;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.OrDocIdSet;
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


/// Measures the AND restriction push-down (`andRestrictionPushdownMode`) in wall-clock time and, with `-prof gc`,
/// allocation -- not only in `numEntriesScannedInFilter`, which can fall while both of those rise.
///
/// The matrix covers the four dimensions that decide whether the push-down pays for itself:
///
/// - `_shape`: whether the OR subtree still has a scan to restrict (`SCAN_IN_OR`, the shape from
///   [issue 19339](https://github.com/apache/pinot/issues/19339)) or is entirely index-based (`INDEXED_OR`, where
///   there is nothing to gain and the question is only how much the machinery costs). `SCAN_ONLY_OR` is the third
///   case: scans directly under the OR with no index-based sibling, so the OR is already lazy today and a LIMIT
///   really does stop early -- the shape the AUTO mode exists to protect.
/// - `_consume`: whether the consumer drains the filter, as an aggregation does, or stops at a LIMIT, as a
///   selection does. The push-down materializes, so this is where it can lose.
/// - `_candidateSelectivity`: how much the enclosing AND narrows things before the OR is reached.
/// - `_pushdown`: the feature itself, off versus on.
///
/// Run with `-prof gc` to get `gc.alloc.rate.norm` (bytes per operation) alongside the timings.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class BenchmarkAndRestrictionPushdown {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkAndRestrictionPushdown");
  private static final int NUM_DOCS = 2_000_000;
  private static final int LIMIT = 10;
  private static final int NUM_DICT_IDS = 1024;
  private static final int CHUNK_SIZE = 256;

  /// OFF is master's behaviour; EAGER is the push-down as #19408 ships it; STREAMING models the spike, where the
  /// candidate set still reaches the scan but a chunk at a time, so nothing is materialized.
  @Param({"OFF", "EAGER", "STREAMING"})
  private String _strategy;

  @Param({"SCAN_IN_OR", "SCAN_ONLY_OR"})
  private String _shape;

  @Param({"DRAIN", "LIMIT"})
  private String _consume;

  /// Fraction of documents the enclosing AND's index-based child matches: sparse candidates versus dense.
  @Param({"0.01", "0.5"})
  private double _candidateSelectivity;

  private ImmutableRoaringBitmap _candidateDocIds;
  private ImmutableRoaringBitmap _firstBranchDocIds;
  private ImmutableRoaringBitmap _secondBranchDocIds;
  private PinotDataBuffer _dataBuffer;
  private FixedBitSVForwardIndexReaderV2 _forwardIndexReader;
  private BenchmarkScanDocIdIterators.DummyPredicateEvaluator _predicateEvaluator;

  public static void main(String[] args)
      throws RunnerException {
    new Runner(new OptionsBuilder().include(BenchmarkAndRestrictionPushdown.class.getSimpleName())
        .addProfiler("gc")
        .build()).run();
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
    // Matches roughly a third of the documents, so the scan is neither trivially empty nor a match-all
    _predicateEvaluator = new BenchmarkScanDocIdIterators.DummyPredicateEvaluator(NUM_DICT_IDS / 3);

    _candidateDocIds = randomBitmap(random, _candidateSelectivity);
    _firstBranchDocIds = randomBitmap(random, 0.3);
    _secondBranchDocIds = randomBitmap(random, 0.2);

    verifyStrategiesAgree();
  }

  /// A timing comparison between strategies only means something if they return the same documents. STREAMING in
  /// particular is a hand-assembled model of the push-down, so a mistake there would show up as an impressive and
  /// meaningless number.
  private void verifyStrategiesAgree() {
    String strategy = _strategy;
    try {
      _strategy = "OFF";
      int[] off = drain(buildFilter().iterator());
      _strategy = "EAGER";
      int[] eager = drain(buildFilter().iterator());
      _strategy = "STREAMING";
      int[] streaming = drain(buildStreamingFilter());
      if (!Arrays.equals(off, eager) || !Arrays.equals(off, streaming)) {
        throw new IllegalStateException(
            String.format("Strategies disagree for shape %s: off=%d, eager=%d, streaming=%d", _shape, off.length,
                eager.length, streaming.length));
      }
    } finally {
      _strategy = strategy;
    }
  }

  private static int[] drain(BlockDocIdIterator docIdIterator) {
    IntArrayList docIds = new IntArrayList();
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      docIds.add(docId);
    }
    return docIds.toIntArray();
  }

  @TearDown(Level.Trial)
  public void tearDown()
      throws Exception {
    _dataBuffer.close();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Benchmark
  public int evaluateFilter(Blackhole bh) {
    BlockDocIdIterator docIdIterator =
        "STREAMING".equals(_strategy) ? buildStreamingFilter() : buildFilter().iterator();
    int limit = "LIMIT".equals(_consume) ? LIMIT : Integer.MAX_VALUE;
    int numDocs = 0;
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      bh.consume(docId);
      if (++numDocs >= limit) {
        break;
      }
    }
    return numDocs;
  }

  /// `AND(candidates, OR(...))`, rebuilt on every invocation because a DocIdSet is single-use.
  private BlockDocIdSet buildFilter() {
    BlockDocIdSet orDocIdSet;
    if ("INDEXED_OR".equals(_shape)) {
      orDocIdSet = new OrDocIdSet(List.of(new BitmapDocIdSet(_firstBranchDocIds, NUM_DOCS),
          new BitmapDocIdSet(_secondBranchDocIds, NUM_DOCS)), NUM_DOCS);
    } else if ("SCAN_ONLY_OR".equals(_shape)) {
      // No index-based sibling inside the branches, so without the push-down the OR stays lazy and a LIMIT really
      // does stop early. This is the shape AUTO exists to protect.
      orDocIdSet = new OrDocIdSet(List.of(newScanDocIdSet(), newScanDocIdSet()), NUM_DOCS);
    } else {
      // The reported shape: a scan sits next to an index-based predicate inside an OR branch
      BlockDocIdSet branch = new AndDocIdSet(
          List.of(new BitmapDocIdSet(_firstBranchDocIds, NUM_DOCS), newScanDocIdSet()), null, isEager());
      orDocIdSet =
          new OrDocIdSet(List.of(branch, new BitmapDocIdSet(_secondBranchDocIds, NUM_DOCS)), NUM_DOCS);
    }
    return new AndDocIdSet(List.of(new BitmapDocIdSet(_candidateDocIds, NUM_DOCS), orDocIdSet), null, isEager());
  }

  private boolean isEager() {
    return "EAGER".equals(_strategy);
  }

  /// What a streaming push-down would produce. The spike's kernel is not wired into AndDocIdSet yet, so the tree is
  /// assembled here by hand, using the same identities the push-down applies:
  ///
  /// - `AND(C, OR(s1, s2))` is `OR(s1 restricted to C, s2 restricted to C)`
  /// - `AND(C, OR(AND(A, s), B))` is `OR(s restricted to C AND A, C AND B)`
  ///
  /// so the scans see exactly the candidate documents the eager push-down would give them, only lazily.
  private BlockDocIdIterator buildStreamingFilter() {
    if ("SCAN_ONLY_OR".equals(_shape)) {
      return new OrDocIdIterator(
          new BlockDocIdIterator[]{restrictedScan(_candidateDocIds), restrictedScan(_candidateDocIds)});
    }
    ImmutableRoaringBitmap firstBranchCandidates = ImmutableRoaringBitmap.and(_candidateDocIds, _firstBranchDocIds);
    ImmutableRoaringBitmap secondBranchDocIds = ImmutableRoaringBitmap.and(_candidateDocIds, _secondBranchDocIds);
    return new OrDocIdIterator(new BlockDocIdIterator[]{
        restrictedScan(firstBranchCandidates), new RangelessBitmapDocIdIterator(secondBranchDocIds)});
  }

  private BlockDocIdIterator restrictedScan(ImmutableRoaringBitmap candidateDocIds) {
    return new RestrictedScanDocIdIterator(new RangelessBitmapDocIdIterator(candidateDocIds),
        new SVScanDocIdIterator(_predicateEvaluator, _forwardIndexReader, NUM_DOCS), CHUNK_SIZE);
  }

  private BlockDocIdSet newScanDocIdSet() {
    SVScanDocIdIterator docIdIterator =
        new SVScanDocIdIterator(_predicateEvaluator, _forwardIndexReader, NUM_DOCS);
    return new BlockDocIdSet() {
      @Override
      public BlockDocIdIterator iterator() {
        return docIdIterator;
      }

      @Override
      public boolean isScanBased() {
        return true;
      }

      @Override
      public long getNumEntriesScannedInFilter() {
        return docIdIterator.getNumEntriesScanned();
      }
    };
  }

  private static ImmutableRoaringBitmap randomBitmap(Random random, double selectivity) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      if (random.nextDouble() < selectivity) {
        bitmap.add(docId);
      }
    }
    return bitmap;
  }
}
