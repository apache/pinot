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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Benchmark comparing three execution paths for single-column DISTINCT queries:
 *
 * <ul>
 *   <li><b>Sorted index path</b>: merge-iterates filter bitmap against contiguous doc ranges per dictId.
 *       Cost ~ O(dictionaryCardinality + filterCardinality). Only applicable when the column is sorted.</li>
 *   <li><b>Bitmap inverted index path</b>: iterates all dictionary entries, uses {@code intersects()} to check
 *       filter membership per entry. Cost ~ O(dictionaryCardinality * bitmapIntersectionCost).</li>
 *   <li><b>Scan path</b>: iterates all filtered docIds, looks up dictId from forward index, deduplicates.
 *       Cost ~ O(filterCardinality * forwardIndexLookupCost).</li>
 * </ul>
 *
 * <p>The sorted index path is always the fastest when applicable (column is sorted), as it avoids both
 * bitmap intersection and per-doc forward index lookups.
 *
 * <p>Usage: {@code java -jar pinot-perf/target/benchmarks.jar BenchmarkInvertedIndexDistinct}
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2, warmups = 0)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 3, time = 3)
public class BenchmarkInvertedIndexDistinct {

  @Param({"1000000"})
  int _numDocs;

  @Param({"100", "1000", "10000", "100000"})
  int _dictionaryCardinality;

  // Filter selectivity: fraction of total docs that pass the filter (0.001 = 0.1%, 0.01 = 1%, 0.1 = 10%, 1.0 = 100%)
  @Param({"0.001", "0.01", "0.1", "0.5", "1.0"})
  double _filterSelectivity;

  // -- Bitmap inverted index: dictId -> docIds bitmap (non-sorted, random distribution)
  private ImmutableRoaringBitmap[] _invertedIndex;

  // -- Forward index: docId -> dictId (simulates column forward index for scan path)
  private int[] _forwardIndex;

  // -- Sorted index ranges: dictId -> [startDocId, endDocId] (inclusive)
  // Simulates SortedIndexReader.getDocIds(dictId) which returns contiguous doc ranges
  private int[] _sortedRangeStarts;
  private int[] _sortedRangeEnds;

  // -- Filter bitmap: which docIds pass the filter
  private ImmutableRoaringBitmap _filterBitmap;

  private int _filterCardinality;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);

    // ---- Non-sorted data (for inverted index and scan paths) ----

    // Build forward index: assign each doc a random dictId
    _forwardIndex = new int[_numDocs];
    for (int docId = 0; docId < _numDocs; docId++) {
      _forwardIndex[docId] = random.nextInt(_dictionaryCardinality);
    }

    // Build inverted index from forward index
    MutableRoaringBitmap[] mutableInvertedIndex = new MutableRoaringBitmap[_dictionaryCardinality];
    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      mutableInvertedIndex[dictId] = new MutableRoaringBitmap();
    }
    for (int docId = 0; docId < _numDocs; docId++) {
      mutableInvertedIndex[_forwardIndex[docId]].add(docId);
    }

    // Optimize and store as ImmutableRoaringBitmap
    _invertedIndex = new ImmutableRoaringBitmap[_dictionaryCardinality];
    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      mutableInvertedIndex[dictId].runOptimize();
      _invertedIndex[dictId] = mutableInvertedIndex[dictId];
    }

    // ---- Sorted data (for sorted index path) ----

    // Each dictId maps to a contiguous doc range, simulating a sorted column.
    // dictId=0: docs [0, docsPerValue-1], dictId=1: docs [docsPerValue, 2*docsPerValue-1], etc.
    int docsPerValue = _numDocs / _dictionaryCardinality;
    _sortedRangeStarts = new int[_dictionaryCardinality];
    _sortedRangeEnds = new int[_dictionaryCardinality];
    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      _sortedRangeStarts[dictId] = dictId * docsPerValue;
      _sortedRangeEnds[dictId] = (dictId + 1) * docsPerValue - 1;
    }
    // Last range absorbs any remainder
    _sortedRangeEnds[_dictionaryCardinality - 1] = _numDocs - 1;

    // ---- Filter bitmap (shared across all paths) ----

    _filterCardinality = Math.max(1, (int) (_numDocs * _filterSelectivity));
    MutableRoaringBitmap mutableFilterBitmap = new MutableRoaringBitmap();
    if (_filterSelectivity >= 1.0) {
      // Match all
      mutableFilterBitmap.add(0L, _numDocs);
    } else {
      // Random selection of filterCardinality docs
      // Use reservoir-style: shuffle and pick first N
      int[] docIds = new int[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        docIds[i] = i;
      }
      // Fisher-Yates partial shuffle for first _filterCardinality elements
      for (int i = 0; i < _filterCardinality; i++) {
        int j = i + random.nextInt(_numDocs - i);
        int tmp = docIds[i];
        docIds[i] = docIds[j];
        docIds[j] = tmp;
        mutableFilterBitmap.add(docIds[i]);
      }
    }
    mutableFilterBitmap.runOptimize();
    _filterBitmap = mutableFilterBitmap;
  }

  /**
   * Sorted index path: merge-iterate filter bitmap against contiguous doc ranges.
   * Uses PeekableIntIterator.advanceIfNeeded() to skip filter docs between ranges.
   * Cost ~ O(dictionaryCardinality + filterCardinality).
   */
  @Benchmark
  public int sortedIndexPath(Blackhole bh) {
    MutableRoaringBitmap seenDictIds = new MutableRoaringBitmap();
    int valuesProcessed = 0;

    PeekableIntIterator filterIter = _filterBitmap.getIntIterator();
    for (int dictId = 0; dictId < _dictionaryCardinality && filterIter.hasNext(); dictId++) {
      int startDocId = _sortedRangeStarts[dictId];
      int endDocId = _sortedRangeEnds[dictId];

      // Skip filter docs before this range
      filterIter.advanceIfNeeded(startDocId);

      // Check if any filter doc falls within this range
      if (filterIter.hasNext() && filterIter.peekNext() <= endDocId) {
        seenDictIds.add(dictId);
        valuesProcessed++;
        // Advance past the current range for next dictId
        filterIter.advanceIfNeeded(endDocId + 1);
      }
    }

    bh.consume(seenDictIds);
    return valuesProcessed;
  }

  /**
   * Bitmap inverted index path: iterate all dictionary entries, intersect each with filter bitmap.
   * Uses intersects() for early termination instead of computing full intersection.
   * Cost ~ O(dictionaryCardinality * bitmapIntersectionCost).
   */
  @Benchmark
  public int invertedIndexPath(Blackhole bh) {
    MutableRoaringBitmap seenDictIds = new MutableRoaringBitmap();
    int valuesProcessed = 0;

    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      ImmutableRoaringBitmap docIds = _invertedIndex[dictId];
      // Bitmap intersection to check if any filtered doc has this value
      if (ImmutableRoaringBitmap.intersects(docIds, _filterBitmap)) {
        seenDictIds.add(dictId);
        valuesProcessed++;
      }
    }

    bh.consume(seenDictIds);
    return valuesProcessed;
  }

  /**
   * Scan path: iterate all filtered docIds, look up dictId from forward index, dedup.
   * Cost ~ O(filterCardinality * forwardIndexLookupCost).
   */
  @Benchmark
  public int scanPath(Blackhole bh) {
    MutableRoaringBitmap seenDictIds = new MutableRoaringBitmap();
    int docsScanned = 0;

    var iter = _filterBitmap.getIntIterator();
    while (iter.hasNext()) {
      int docId = iter.next();
      int dictId = _forwardIndex[docId];
      seenDictIds.add(dictId);
      docsScanned++;
    }

    bh.consume(seenDictIds);
    return docsScanned;
  }

  /**
   * Inverted index path with ORDER BY ASC LIMIT: iterate dictIds forward, stop after finding
   * {@code limit} matching values. Exploits dictId order = value order for early termination.
   * Cost ~ O(limit / hitRate * bitmapIntersectionCost) where hitRate = distinctInFilter / cardinality.
   */
  @Benchmark
  public int invertedIndexPathLimitAsc(Blackhole bh) {
    int limit = 10;
    int found = 0;

    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      ImmutableRoaringBitmap docIds = _invertedIndex[dictId];
      if (ImmutableRoaringBitmap.intersects(docIds, _filterBitmap)) {
        found++;
        if (found >= limit) {
          break;
        }
      }
    }

    bh.consume(found);
    return found;
  }

  /**
   * Inverted index path with ORDER BY DESC LIMIT: iterate dictIds backward, stop after finding
   * {@code limit} matching values. Exploits reverse dictId order for early termination.
   * Cost ~ O(limit / hitRate * bitmapIntersectionCost).
   */
  @Benchmark
  public int invertedIndexPathLimitDesc(Blackhole bh) {
    int limit = 10;
    int found = 0;

    for (int dictId = _dictionaryCardinality - 1; dictId >= 0; dictId--) {
      ImmutableRoaringBitmap docIds = _invertedIndex[dictId];
      if (ImmutableRoaringBitmap.intersects(docIds, _filterBitmap)) {
        found++;
        if (found >= limit) {
          break;
        }
      }
    }

    bh.consume(found);
    return found;
  }

  /**
   * Sorted index path with ORDER BY DESC LIMIT: iterate dictIds backward, check filter presence
   * using rangeCardinality. Exploits reverse iteration for early termination.
   * Cost ~ O(limit / hitRate) since rangeCardinality on contiguous ranges is O(1).
   */
  @Benchmark
  public int sortedIndexPathLimitDesc(Blackhole bh) {
    int limit = 10;
    int found = 0;

    for (int dictId = _dictionaryCardinality - 1; dictId >= 0; dictId--) {
      int startDocId = _sortedRangeStarts[dictId];
      int endDocId = _sortedRangeEnds[dictId];
      if (_filterBitmap.rangeCardinality(startDocId, endDocId + 1L) > 0) {
        found++;
        if (found >= limit) {
          break;
        }
      }
    }

    bh.consume(found);
    return found;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder()
        .include(BenchmarkInvertedIndexDistinct.class.getSimpleName())
        .build())
        .run();
  }
}
