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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.BitmapBasedDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.OrDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SortedDocIdIterator;
import org.apache.pinot.spi.utils.Pairs;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// The BlockDocIdSet to perform OR on all child BlockDocIdSets.
///
/// The OrBlockDocIdSet will construct the BlockDocIdIterator based on the BlockDocIdIterators from the child
/// BlockDocIdSets:
///
/// - When there are more than one index-base BlockDocIdIterator (SortedDocIdIterator or BitmapBasedDocIdIterator),
///   merge them and construct a BitmapDocIdIterator from the merged document ids. If there is no remaining
///   BlockDocIdIterator, directly return the merged BitmapDocIdIterator; otherwise, construct and return an
///   OrDocIdIterator with the merged BitmapDocIdIterator and the remaining BlockDocIdIterators.
/// - Otherwise, construct and return an OrDocIdIterator with all BlockDocIdIterators.
public final class OrDocIdSet implements BlockDocIdSet {
  // Keep the scan based BlockDocIdSets to be accessed when collecting query execution stats
  private final AtomicReference<List<BlockDocIdSet>> _scanBasedDocIdSets = new AtomicReference<>();
  private final int _numDocs;
  private List<BlockDocIdSet> _docIdSets;
  private volatile long _numEntriesScannedInFilter = 0L;

  public OrDocIdSet(List<BlockDocIdSet> docIdSets, int numDocs) {
    _docIdSets = docIdSets;
    _numDocs = numDocs;
  }

  @Override
  public BlockDocIdIterator iterator() {
    Preconditions.checkState(_docIdSets != null, "iterator() called on an already consumed OrDocIdSet");
    int numDocIdSets = _docIdSets.size();
    BlockDocIdIterator[] allDocIdIterators = new BlockDocIdIterator[numDocIdSets];
    List<SortedDocIdIterator> sortedDocIdIterators = new ArrayList<>();
    List<BitmapBasedDocIdIterator> bitmapBasedDocIdIterators = new ArrayList<>();
    List<BlockDocIdIterator> remainingDocIdIterators = new ArrayList<>();
    long numEntriesScannedForNonScanBasedDocIdSets = 0L;
    List<BlockDocIdSet> scanBasedDocIdSets = new ArrayList<>();

    for (int i = 0; i < numDocIdSets; i++) {
      BlockDocIdSet docIdSet = _docIdSets.get(i);
      BlockDocIdIterator docIdIterator = docIdSet.iterator();
      allDocIdIterators[i] = docIdIterator;
      if (docIdIterator instanceof SortedDocIdIterator) {
        sortedDocIdIterators.add((SortedDocIdIterator) docIdIterator);
        numEntriesScannedForNonScanBasedDocIdSets += docIdSet.getNumEntriesScannedInFilter();
      } else if (docIdIterator instanceof BitmapBasedDocIdIterator) {
        bitmapBasedDocIdIterators.add((BitmapBasedDocIdIterator) docIdIterator);
        numEntriesScannedForNonScanBasedDocIdSets += docIdSet.getNumEntriesScannedInFilter();
      } else {
        remainingDocIdIterators.add(docIdIterator);
        scanBasedDocIdSets.add(docIdSet);
      }
    }

    // Publish the stats state before dropping the branches, so that a concurrent reader of
    // getNumEntriesScannedInFilter() never sees it disappear, then set _docIdSets to null so that the underlying
    // BlockDocIdSets can be garbage collected
    _numEntriesScannedInFilter = numEntriesScannedForNonScanBasedDocIdSets;
    _scanBasedDocIdSets.set(scanBasedDocIdSets);
    _docIdSets = null;

    int numSortedDocIdIterators = sortedDocIdIterators.size();
    int numBitmapBasedDocIdIterators = bitmapBasedDocIdIterators.size();
    if (numSortedDocIdIterators + numBitmapBasedDocIdIterators > 1) {
      // When there are more than one index-base BlockDocIdIterator (SortedDocIdIterator or BitmapBasedDocIdIterator),
      // merge them and construct a BitmapDocIdIterator from the merged document ids. If there is no remaining
      // BlockDocIdIterator, directly return the merged BitmapDocIdIterator; otherwise, construct and return an
      // OrDocIdIterator with the merged BitmapDocIdIterator and the remaining BlockDocIdIterators.

      MutableRoaringBitmap docIds = new MutableRoaringBitmap();
      for (SortedDocIdIterator sortedDocIdIterator : sortedDocIdIterators) {
        for (Pairs.IntPair docIdRange : sortedDocIdIterator.getDocIdRanges()) {
          // NOTE: docIdRange has inclusive start and end.
          docIds.add(docIdRange.getLeft(), docIdRange.getRight() + 1L);
        }
      }
      for (BitmapBasedDocIdIterator bitmapBasedDocIdIterator : bitmapBasedDocIdIterators) {
        docIds.or(bitmapBasedDocIdIterator.getDocIds());
      }
      BitmapDocIdIterator bitmapDocIdIterator = new BitmapDocIdIterator(docIds, _numDocs);
      int numRemainingDocIdIterators = remainingDocIdIterators.size();
      if (numRemainingDocIdIterators == 0) {
        return bitmapDocIdIterator;
      } else {
        BlockDocIdIterator[] docIdIterators = new BlockDocIdIterator[numRemainingDocIdIterators + 1];
        docIdIterators[0] = bitmapDocIdIterator;
        for (int i = 0; i < numRemainingDocIdIterators; i++) {
          docIdIterators[i + 1] = remainingDocIdIterators.get(i);
        }
        return new OrDocIdIterator(docIdIterators);
      }
    } else {
      // Otherwise, construct and return an OrDocIdIterator with all BlockDocIdIterators.

      return new OrDocIdIterator(allDocIdIterators);
    }
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    List<BlockDocIdSet> scanBasedDocIdSets = _scanBasedDocIdSets.get();
    long numEntriesScannedForScanBasedDocIdSets = 0L;
    if (scanBasedDocIdSets != null) {
      for (BlockDocIdSet scanBasedDocIdSet : scanBasedDocIdSets) {
        numEntriesScannedForScanBasedDocIdSets += scanBasedDocIdSet.getNumEntriesScannedInFilter();
      }
    }
    return _numEntriesScannedInFilter + numEntriesScannedForScanBasedDocIdSets;
  }

  @Override
  public boolean isApplyAndDeferrable() {
    return true;
  }

  /// Unions the branches, each restricted to the candidate document ids.
  ///
  /// A branch only has to look at the candidates no earlier branch has matched yet, because
  /// `(A OR B) AND S == (A AND S) OR (B AND (S MINUS (A AND S)))`. That matters because the cost of a scan-based
  /// branch is linear in the size of the candidate set it is given, so handing every branch the full candidate set
  /// would scan it once per branch.
  @Override
  public ImmutableRoaringBitmap applyAnd(ImmutableRoaringBitmap docIds) {
    List<BlockDocIdSet> docIdSets = _docIdSets;
    Preconditions.checkState(docIdSets != null, "applyAnd() called on an already consumed OrDocIdSet");
    // Publish the stats state before dropping the branches, so that a concurrent reader of
    // getNumEntriesScannedInFilter() never sees it disappear. Branches left unevaluated by the short-circuit below
    // report zero.
    _scanBasedDocIdSets.set(docIdSets);
    _docIdSets = null;
    MutableRoaringBitmap docIdsToReturn = new MutableRoaringBitmap();
    if (docIds.isEmpty()) {
      return docIdsToReturn;
    }
    int numCandidates = docIds.getCardinality();
    ImmutableRoaringBitmap remainingDocIds = docIds;
    for (BlockDocIdSet docIdSet : docIdSets) {
      ImmutableRoaringBitmap matchingDocIds = docIdSet.applyAnd(remainingDocIds);
      if (matchingDocIds.isEmpty()) {
        continue;
      }
      docIdsToReturn.or(matchingDocIds);
      if (docIdsToReturn.getCardinality() == numCandidates) {
        // Every candidate is already matched, the remaining branches cannot add anything
        break;
      }
      remainingDocIds = ImmutableRoaringBitmap.andNot(remainingDocIds, matchingDocIds);
    }
    return docIdsToReturn;
  }

  @Override
  public BlockDocIdSet getOptimizedDocIdSet() {
    return this;
  }
}
