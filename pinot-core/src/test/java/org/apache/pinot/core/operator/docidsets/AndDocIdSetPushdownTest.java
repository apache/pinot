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
package org.apache.pinot.core.operator.docidsets;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.Random;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.utils.Pairs.IntPair;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Unit test for the restriction push-down: an AND hands the document ids matched by its index-based children to its
/// composite (AND/OR/NOT) children, so that a scan-based predicate nested inside one of them is only evaluated on the
/// documents that can still match.
///
/// See [issue 19339](https://github.com/apache/pinot/issues/19339).
public class AndDocIdSetPushdownTest {
  private static final int NUM_DOCS = 10000;
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  /// The shape reported in the issue: a scan-based predicate sits in an OR branch next to an index-based predicate.
  /// That makes the branch take the eager path in [AndDocIdSet#iterator], materializing it over the whole segment and
  /// evaluating the scan on every document its sibling matches, regardless of how selective the enclosing AND is.
  @Test
  public void testScanNestedInsideOrBranchIsRestrictedByEnclosingAnd() {
    ImmutableRoaringBitmap selective = range(0, 100);
    ImmutableRoaringBitmap indexedInBranch = range(0, 5000);
    ImmutableRoaringBitmap otherBranch = range(9000, 9010);
    ImmutableRoaringBitmap scanMatches = multiplesOf(3);

    // Expected: selective AND ((indexedInBranch AND scanMatches) OR otherBranch)
    MutableRoaringBitmap expected = MutableRoaringBitmap.and(indexedInBranch, scanMatches);
    expected.or(otherBranch);
    expected.and(selective);

    CountingScanDocIdSet pushedDownScan = new CountingScanDocIdSet(scanMatches);
    BlockDocIdSet withPushdown = orBranchTree(true, pushedDownScan, selective, indexedInBranch, otherBranch);
    assertEquals(collectDocIds(withPushdown), expected.toArray());
    long scannedWithPushdown = pushedDownScan.getNumEntriesScannedInFilter();
    assertEquals(scannedWithPushdown, selective.getCardinality(),
        "The scan must only visit the documents matching the enclosing AND");
    assertEquals(withPushdown.getNumEntriesScannedInFilter(), scannedWithPushdown,
        "Entries scanned inside the OR must be reported by the enclosing AND");

    CountingScanDocIdSet unrestrictedScan = new CountingScanDocIdSet(scanMatches);
    BlockDocIdSet withoutPushdown =
        orBranchTree(false, unrestrictedScan, selective, indexedInBranch, otherBranch);
    assertEquals(collectDocIds(withoutPushdown), expected.toArray(),
        "Disabling the push-down must not change the result");
    long scannedWithoutPushdown = unrestrictedScan.getNumEntriesScannedInFilter();
    assertEquals(scannedWithoutPushdown, indexedInBranch.getCardinality(),
        "Without the push-down the scan visits every document matching its sibling index predicate");
    assertTrue(scannedWithPushdown < scannedWithoutPushdown,
        "The push-down must reduce the number of scanned entries");
  }

  /// AND(selective, NOT(scan)): the scan only has to be evaluated on the candidates, because
  /// `NOT(child) AND candidates == candidates MINUS (child AND candidates)`.
  @Test
  public void testScanUnderNotIsRestrictedByEnclosingAnd() {
    ImmutableRoaringBitmap selective = range(0, 100);
    ImmutableRoaringBitmap scanMatches = multiplesOf(3);

    MutableRoaringBitmap expected = selective.toMutableRoaringBitmap();
    expected.andNot(scanMatches);

    CountingScanDocIdSet scan = new CountingScanDocIdSet(scanMatches);
    BlockDocIdSet docIdSet = new AndDocIdSet(
        List.of(new BitmapDocIdSet(selective, NUM_DOCS), new NotDocIdSet(scan, NUM_DOCS)), null, true);

    assertEquals(collectDocIds(docIdSet), expected.toArray());
    assertEquals(scan.getNumEntriesScannedInFilter(), selective.getCardinality(),
        "The scan under the NOT must only visit the documents matching the enclosing AND");
  }

  /// With no index-based child there is nothing to seed the push-down with, so the AND must fall back to the lazy
  /// iterator over all of its children, including the deferred composite ones.
  @Test
  public void testFallsBackToLazyPathWithoutIndexBasedChild() {
    ImmutableRoaringBitmap firstBranch = range(0, 50);
    ImmutableRoaringBitmap secondBranch = range(40, 90);
    ImmutableRoaringBitmap scanMatches = multiplesOf(3);

    MutableRoaringBitmap expected = firstBranch.toMutableRoaringBitmap();
    expected.or(secondBranch);
    expected.and(scanMatches);

    BlockDocIdSet or = new OrDocIdSet(
        List.of(new BitmapDocIdSet(firstBranch, NUM_DOCS), new BitmapDocIdSet(secondBranch, NUM_DOCS)), NUM_DOCS);
    BlockDocIdSet docIdSet = new AndDocIdSet(List.of(new CountingScanDocIdSet(scanMatches), or), null, true);

    assertEquals(collectDocIds(docIdSet), expected.toArray());
  }

  /// The push-down must never change the matching documents, whatever the shape of the filter tree.
  @Test
  public void testRandomTreesMatchTheSameDocumentsWithAndWithoutPushdown() {
    for (int i = 0; i < 200; i++) {
      long seed = RANDOM_SEED + i;
      BlockDocIdSet withPushdown = randomTree(new Random(seed), 3, true);
      BlockDocIdSet withoutPushdown = randomTree(new Random(seed), 3, false);
      assertEquals(collectDocIds(withPushdown), collectDocIds(withoutPushdown), ERROR_MESSAGE + ", iteration: " + i);
    }
  }

  private static BlockDocIdSet orBranchTree(boolean pushdownEnabled, BlockDocIdSet scan,
      ImmutableRoaringBitmap selective, ImmutableRoaringBitmap indexedInBranch, ImmutableRoaringBitmap otherBranch) {
    BlockDocIdSet branch =
        new AndDocIdSet(List.of(new BitmapDocIdSet(indexedInBranch, NUM_DOCS), scan), null, pushdownEnabled);
    BlockDocIdSet or = new OrDocIdSet(List.of(branch, new BitmapDocIdSet(otherBranch, NUM_DOCS)), NUM_DOCS);
    return new AndDocIdSet(List.of(new BitmapDocIdSet(selective, NUM_DOCS), or), null, pushdownEnabled);
  }

  /// Builds a random filter tree. Two calls with equally seeded [Random] instances build the same tree, which is what
  /// lets the same tree be evaluated with and without the push-down.
  private static BlockDocIdSet randomTree(Random random, int depth, boolean pushdownEnabled) {
    if (depth == 0) {
      return randomLeaf(random);
    }
    switch (random.nextInt(5)) {
      case 0: {
        List<BlockDocIdSet> children = randomChildren(random, depth, pushdownEnabled);
        return new AndDocIdSet(children, null, pushdownEnabled);
      }
      case 1: {
        List<BlockDocIdSet> children = randomChildren(random, depth, pushdownEnabled);
        return new OrDocIdSet(children, NUM_DOCS);
      }
      case 2:
        return new NotDocIdSet(randomTree(random, depth - 1, pushdownEnabled), NUM_DOCS);
      default:
        return randomLeaf(random);
    }
  }

  private static List<BlockDocIdSet> randomChildren(Random random, int depth, boolean pushdownEnabled) {
    int numChildren = 2 + random.nextInt(2);
    List<BlockDocIdSet> children = new ArrayList<>(numChildren);
    for (int i = 0; i < numChildren; i++) {
      children.add(randomTree(random, depth - 1, pushdownEnabled));
    }
    return children;
  }

  /// Builds a leaf of every kind that [BlockDocIdSet#applyAnd] dispatches on, so that the bitmap, scan, sorted-range
  /// and match-all/empty arms are all exercised by the randomized equivalence test.
  private static BlockDocIdSet randomLeaf(Random random) {
    switch (random.nextInt(8)) {
      case 0:
        return new MatchAllDocIdSet(NUM_DOCS);
      case 1:
        return EmptyDocIdSet.getInstance();
      case 2:
      case 3: {
        // Sorted leaf: inclusive ranges, which applyAnd has to convert to an exclusive upper bound
        List<IntPair> docIdRanges = new ArrayList<>();
        int start = random.nextInt(NUM_DOCS / 2);
        int numRanges = 1 + random.nextInt(3);
        for (int i = 0; i < numRanges && start < NUM_DOCS; i++) {
          int end = Math.min(start + random.nextInt(500), NUM_DOCS - 1);
          docIdRanges.add(new IntPair(start, end));
          start = end + 2 + random.nextInt(500);
        }
        return new SortedDocIdSet(docIdRanges);
      }
      case 4:
      case 5:
        return new CountingScanDocIdSet(randomBitmap(random));
      default:
        return new BitmapDocIdSet(randomBitmap(random), NUM_DOCS);
    }
  }

  private static MutableRoaringBitmap randomBitmap(Random random) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    int numRuns = 1 + random.nextInt(8);
    for (int i = 0; i < numRuns; i++) {
      int start = random.nextInt(NUM_DOCS);
      bitmap.add(start, Math.min((long) start + 1 + random.nextInt(2000), NUM_DOCS));
    }
    return bitmap;
  }

  /// A sorted leaf whose ranges end exactly at the candidate boundaries, pinning the inclusive-to-exclusive
  /// conversion in [BlockDocIdSet#applyAnd] against an off-by-one.
  @Test
  public void testSortedLeafRangeBoundariesAreInclusive() {
    BlockDocIdSet sorted = new SortedDocIdSet(List.of(new IntPair(10, 20), new IntPair(30, 30)));
    MutableRoaringBitmap candidates = MutableRoaringBitmap.bitmapOf(9, 10, 20, 21, 29, 30, 31);

    assertEquals(sorted.applyAnd(candidates).toArray(), new int[]{10, 20, 30});
  }

  private static int[] collectDocIds(BlockDocIdSet docIdSet) {
    BlockDocIdIterator iterator = docIdSet.iterator();
    List<Integer> docIds = new ArrayList<>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      docIds.add(docId);
    }
    return docIds.stream().mapToInt(Integer::intValue).toArray();
  }

  private static MutableRoaringBitmap range(int start, int endExclusive) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(start, endExclusive);
    return bitmap;
  }

  private static MutableRoaringBitmap multiplesOf(int divisor) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int docId = 0; docId < NUM_DOCS; docId += divisor) {
      bitmap.add(docId);
    }
    return bitmap;
  }

  /// A scan-based DocIdSet that records how many documents it was asked about, standing in for a predicate with no
  /// usable index (a full scan, or `IN_SUBQUERY` evaluated through `ExpressionScanDocIdIterator`).
  private static final class CountingScanDocIdSet implements BlockDocIdSet {
    private final CountingScanDocIdIterator _iterator;

    private CountingScanDocIdSet(ImmutableRoaringBitmap matchingDocIds) {
      _iterator = new CountingScanDocIdIterator(matchingDocIds);
    }

    @Override
    public ScanBasedDocIdIterator iterator() {
      return _iterator;
    }

    @Override
    public long getNumEntriesScannedInFilter() {
      return _iterator.getNumEntriesScanned();
    }
  }

  private static final class CountingScanDocIdIterator implements ScanBasedDocIdIterator {
    private final ImmutableRoaringBitmap _matchingDocIds;
    private long _numEntriesScanned;
    private int _nextDocId;

    private CountingScanDocIdIterator(ImmutableRoaringBitmap matchingDocIds) {
      _matchingDocIds = matchingDocIds;
    }

    @Override
    public MutableRoaringBitmap applyAnd(BatchIterator batchIterator, OptionalInt firstDoc, OptionalInt lastDoc) {
      MutableRoaringBitmap docIds = new MutableRoaringBitmap();
      int[] buffer = new int[OPTIMAL_ITERATOR_BATCH_SIZE];
      while (batchIterator.hasNext()) {
        int numDocIds = batchIterator.nextBatch(buffer);
        for (int i = 0; i < numDocIds; i++) {
          _numEntriesScanned++;
          if (_matchingDocIds.contains(buffer[i])) {
            docIds.add(buffer[i]);
          }
        }
      }
      return docIds;
    }

    @Override
    public int next() {
      while (_nextDocId < NUM_DOCS) {
        int docId = _nextDocId++;
        _numEntriesScanned++;
        if (_matchingDocIds.contains(docId)) {
          return docId;
        }
      }
      return Constants.EOF;
    }

    @Override
    public int advance(int targetDocId) {
      _nextDocId = targetDocId;
      return next();
    }

    @Override
    public long getNumEntriesScanned() {
      return _numEntriesScanned;
    }
  }
}
