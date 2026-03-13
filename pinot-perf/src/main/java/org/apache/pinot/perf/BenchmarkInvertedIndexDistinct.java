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
import org.roaringbitmap.RoaringBitmap;


/**
 * Benchmark to compare inverted-index-based distinct vs scan-based distinct execution paths.
 *
 * <p>Inverted index path: iterates all dictionary entries, does bitmap intersection with filter for each.
 * Cost ~ O(dictionaryCardinality * bitmapIntersectionCost).
 *
 * <p>Scan path: iterates all filtered docIds, looks up dictId from forward index (int[]), deduplicates.
 * Cost ~ O(filterCardinality * forwardIndexLookupCost).
 *
 * <p>The goal is to determine the crossover ratio (dictionaryCardinality / filterCardinality) at which
 * the inverted index path becomes faster than the scan path.
 *
 * <p>Usage: {@code java -jar pinot-perf/target/benchmarks.jar BenchmarkInvertedIndexDistinct}
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, warmups = 3)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class BenchmarkInvertedIndexDistinct {

  @Param({"1000000"})
  int _numDocs;

  @Param({"100", "1000", "10000", "100000"})
  int _dictionaryCardinality;

  // Filter selectivity: fraction of total docs that pass the filter (0.001 = 0.1%, 0.01 = 1%, 0.1 = 10%, 1.0 = 100%)
  @Param({"0.001", "0.01", "0.1", "0.5", "1.0"})
  double _filterSelectivity;

  // -- Inverted index: dictId -> docIds bitmap
  private RoaringBitmap[] _invertedIndex;

  // -- Forward index: docId -> dictId (simulates column forward index)
  private int[] _forwardIndex;

  // -- Filter bitmap: which docIds pass the filter
  private RoaringBitmap _filterBitmap;

  private int _filterCardinality;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);

    // Build forward index: assign each doc a random dictId
    _forwardIndex = new int[_numDocs];
    for (int docId = 0; docId < _numDocs; docId++) {
      _forwardIndex[docId] = random.nextInt(_dictionaryCardinality);
    }

    // Build inverted index from forward index
    _invertedIndex = new RoaringBitmap[_dictionaryCardinality];
    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      _invertedIndex[dictId] = new RoaringBitmap();
    }
    for (int docId = 0; docId < _numDocs; docId++) {
      _invertedIndex[_forwardIndex[docId]].add(docId);
    }

    // Build filter bitmap: randomly select filterSelectivity fraction of docs
    _filterCardinality = Math.max(1, (int) (_numDocs * _filterSelectivity));
    _filterBitmap = new RoaringBitmap();
    if (_filterSelectivity >= 1.0) {
      // Match all
      _filterBitmap.add(0L, _numDocs);
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
        _filterBitmap.add(docIds[i]);
      }
    }
    _filterBitmap.runOptimize();

    // Optimize inverted index bitmaps
    for (RoaringBitmap bitmap : _invertedIndex) {
      bitmap.runOptimize();
    }
  }

  /**
   * Inverted index path: iterate all dictionary entries, intersect each with filter bitmap.
   * For each matching value, add to a dedup set (simulated by a RoaringBitmap of dictIds).
   */
  @Benchmark
  public int invertedIndexPath(Blackhole bh) {
    RoaringBitmap seenDictIds = new RoaringBitmap();
    int valuesProcessed = 0;

    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      RoaringBitmap docIds = _invertedIndex[dictId];
      if (docIds.isEmpty()) {
        continue;
      }
      // Bitmap intersection to check if any filtered doc has this value
      if (RoaringBitmap.intersects(docIds, _filterBitmap)) {
        seenDictIds.add(dictId);
        valuesProcessed++;
      }
    }

    bh.consume(seenDictIds);
    return valuesProcessed;
  }

  /**
   * Scan path: iterate all filtered docIds, look up dictId from forward index, dedup.
   */
  @Benchmark
  public int scanPath(Blackhole bh) {
    RoaringBitmap seenDictIds = new RoaringBitmap();
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

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder()
        .include(BenchmarkInvertedIndexDistinct.class.getSimpleName())
        .build())
        .run();
  }
}
