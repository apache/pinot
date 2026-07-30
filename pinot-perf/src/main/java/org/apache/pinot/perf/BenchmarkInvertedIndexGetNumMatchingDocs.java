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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Isolates the work performed by {@code InvertedIndexFilterOperator.getNumMatchingDocs()}
 * on the multi-dictId (numDictIds >= 3) path:
 *
 * <pre>
 *   MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
 *   for (int dictId : dictIds) {
 *     bitmap.or(_invertedIndexReader.getDocIds(dictId));
 *   }
 *   count = bitmap.getCardinality();
 * </pre>
 *
 * <p>Three variants are compared on the same input:
 * <ul>
 *   <li>{@code currentMaterialize} — the baseline (what the operator did before this change, and what the
 *       MV path still does). Allocates a fresh accumulator, ORs in every per-dictId bitmap, then takes the
 *       cardinality of the materialized union.</li>
 *   <li>{@code svSumCardinalities} — the implemented fix for single-value columns. Bitmaps are guaranteed
 *       disjoint, so the union cardinality is the sum of per-bitmap cardinalities. No allocation, no OR
 *       pass. Correctness is undefined for MV columns (would double-count overlapping docIds); we include
 *       it in the MV runs purely for comparison.</li>
 *   <li>{@code mvFastOrCardinality} — a prototyped (but not implemented) variant for multi-value columns.
 *       {@link BufferFastAggregation#orCardinality} streams through containers and computes the union
 *       cardinality without materializing the result. Wins inside K in [~256, ~10K] but regresses
 *       outside that window — see the commit message for the threshold rationale.</li>
 * </ul>
 *
 * <p>Sweep parameters reflect realistic settings for {@code COUNT(*) WHERE col IN (...)}:
 * <ul>
 *   <li>{@code _numDocs} — segment size (5M ~ one realtime segment).</li>
 *   <li>{@code _dictionaryCardinality} — distinct values in the column.</li>
 *   <li>{@code _numDictIdsMatched} — width of the IN clause (the K in N-K OR).</li>
 *   <li>{@code _multiValue} — SV columns produce disjoint bitmaps; MV columns produce overlapping bitmaps.</li>
 * </ul>
 *
 * <p>Cells where numDictIdsMatched > cardinality are skipped via a null sentinel set in {@link #setup()}.
 *
 * <p>Run: {@code java -jar pinot-perf/target/benchmarks.jar BenchmarkInvertedIndexGetNumMatchingDocs}
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, warmups = 0)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
public class BenchmarkInvertedIndexGetNumMatchingDocs {

  @Param({"5000000"})
  int _numDocs;

  @Param({"1024", "100000", "1000000"})
  int _dictionaryCardinality;

  @Param({"4", "16", "64", "256", "1000", "10000", "100000"})
  int _numDictIdsMatched;

  @Param({"false", "true"})
  boolean _multiValue;

  /** For MV columns, average number of dictIds per doc. Ignored for SV. */
  @Param({"4"})
  int _mvValuesPerDoc;

  /** The K bitmaps the operator will OR together — exactly what
   * {@code _invertedIndexReader.getDocIds(dictIds[i])} returns inside the operator.
   * Null for invalid cells (K > cardinality). */
  private ImmutableRoaringBitmap[] _bitmaps;

  // Cache the full per-dictId index across @Param combinations within a single JVM (fork). JMH runs all
  // combos sequentially per fork, so building the 5M-doc index once per (cardinality, multiValue) tuple
  // turns ~42 rebuilds into 6 actual builds.
  private static int _cachedNumDocs = -1;
  private static int _cachedCardinality = -1;
  private static boolean _cachedMultiValue;
  private static int _cachedMvValuesPerDoc = -1;
  private static MutableRoaringBitmap[] _cachedFull;
  // Packed (cardinality << 32) | index, ascending. Top-K is the last K entries.
  private static long[] _cachedSorted;

  @Setup(Level.Trial)
  public void setup() {
    if (_numDictIdsMatched > _dictionaryCardinality) {
      // K can't exceed dictionary cardinality; benchmark methods short-circuit on null.
      _bitmaps = null;
      return;
    }
    MutableRoaringBitmap[] full = getOrBuildFull();
    long[] sorted = _cachedSorted;
    _bitmaps = new ImmutableRoaringBitmap[_numDictIdsMatched];
    int n = _dictionaryCardinality;
    // Pick top-K (last K entries of the ascending sort) so the workload targets common values.
    for (int i = 0; i < _numDictIdsMatched; i++) {
      int idx = (int) (sorted[n - 1 - i] & 0xFFFFFFFFL);
      _bitmaps[i] = full[idx];
    }
  }

  private MutableRoaringBitmap[] getOrBuildFull() {
    if (_cachedFull != null && _cachedNumDocs == _numDocs && _cachedCardinality == _dictionaryCardinality
        && _cachedMultiValue == _multiValue && _cachedMvValuesPerDoc == _mvValuesPerDoc) {
      return _cachedFull;
    }
    Random random = new Random(42);
    MutableRoaringBitmap[] full = new MutableRoaringBitmap[_dictionaryCardinality];
    for (int i = 0; i < _dictionaryCardinality; i++) {
      full[i] = new MutableRoaringBitmap();
    }
    if (_multiValue) {
      // MV: each doc gets _mvValuesPerDoc random dictIds. Bitmaps overlap.
      for (int docId = 0; docId < _numDocs; docId++) {
        for (int v = 0; v < _mvValuesPerDoc; v++) {
          full[random.nextInt(_dictionaryCardinality)].add(docId);
        }
      }
    } else {
      // SV: each doc gets exactly one dictId. Bitmaps are disjoint.
      for (int docId = 0; docId < _numDocs; docId++) {
        full[random.nextInt(_dictionaryCardinality)].add(docId);
      }
    }
    // Optimize containers (matches what offline segment loading produces).
    for (MutableRoaringBitmap b : full) {
      b.runOptimize();
    }
    // Sort by cardinality ascending; top-K = last K. Arrays.sort(long[]) is O(n log n) — critical at
    // cardinality=1M where the original selection-sort approach would have been O(K * n) ~ 10^11 ops.
    long[] sorted = new long[_dictionaryCardinality];
    for (int i = 0; i < _dictionaryCardinality; i++) {
      sorted[i] = ((long) full[i].getCardinality() << 32) | (i & 0xFFFFFFFFL);
    }
    Arrays.sort(sorted);

    _cachedFull = full;
    _cachedSorted = sorted;
    _cachedNumDocs = _numDocs;
    _cachedCardinality = _dictionaryCardinality;
    _cachedMultiValue = _multiValue;
    _cachedMvValuesPerDoc = _mvValuesPerDoc;
    return full;
  }

  /**
   * Baseline: what the operator did before this change, and what the MV path still does. Allocates a
   * fresh accumulator, ORs in every per-dictId bitmap, then takes the cardinality of the materialized
   * union.
   */
  @Benchmark
  public int currentMaterialize() {
    if (_bitmaps == null) {
      return 0;
    }
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (ImmutableRoaringBitmap b : _bitmaps) {
      bitmap.or(b);
    }
    return bitmap.getCardinality();
  }

  /**
   * Shipped fix for single-value columns. Bitmaps from a per-dictId inverted index on an SV column are
   * disjoint, so |⋃ b_i| = Σ |b_i|. No allocation, no OR pass. Correctness is undefined for MV columns
   * (would double-count overlapping docIds); included in the MV runs purely for comparison.
   */
  @Benchmark
  public long svSumCardinalities() {
    if (_bitmaps == null) {
      return 0;
    }
    long count = 0;
    for (ImmutableRoaringBitmap b : _bitmaps) {
      count += b.getCardinality();
    }
    return count;
  }

  /**
   * Prototyped (but not shipped) variant for multi-value columns. RoaringBitmap's streaming
   * union-cardinality avoids materializing the merged bitmap. Wins inside K in [~256, ~10K] but regresses
   * at low K (4-16) and at very high K (~100K) on high-cardinality columns where horizontal-OR's fixed
   * scratch-buffer allocation and two-pass scan amortize poorly.
   */
  @Benchmark
  public int mvFastOrCardinality() {
    if (_bitmaps == null) {
      return 0;
    }
    return BufferFastAggregation.orCardinality(_bitmaps);
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder()
        .include(BenchmarkInvertedIndexGetNumMatchingDocs.class.getSimpleName())
        .build())
        .run();
  }
}
