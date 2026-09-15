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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.AndDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.BitmapBasedDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.RangelessBitmapDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SortedDocIdIterator;
import org.apache.pinot.core.util.SortedRangeIntersection;
import org.apache.pinot.spi.utils.Pairs.IntPair;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// The BlockDocIdSet to perform AND on all child BlockDocIdSets.
///
/// The AndBlockDocIdSet will construct the BlockDocIdIterator based on the BlockDocIdIterators from the child
/// BlockDocIdSets:
///
/// - When there are at least one index-base BlockDocIdIterator (SortedDocIdIterator or BitmapBasedDocIdIterator) and
///   at least one ScanBasedDocIdIterator, or more than one index-based BlockDocIdIterator, merge them and construct a
///   RangelessBitmapDocIdIterator from the merged document ids. If there is no remaining BlockDocIdIterator, directly
///   return the merged RangelessBitmapDocIdIterator; otherwise, construct and return an AndDocIdIterator with the
///   merged RangelessBitmapDocIdIterator and the remaining BlockDocIdIterators.
/// - Otherwise, construct and return an AndDocIdIterator with all BlockDocIdIterators.
public final class AndDocIdSet implements BlockDocIdSet {
  // Keep the scan based BlockDocIdSets to be accessed when collecting query execution stats
  private final AtomicReference<List<BlockDocIdSet>> _scanBasedDocIdSets = new AtomicReference<>();
  private final boolean _cardinalityBasedRankingForScan;
  private final boolean _restrictionPushdownEnabled;
  private List<BlockDocIdSet> _docIdSets;
  private volatile long _numEntriesScannedInFilter;

  /// Creates an AndDocIdSet that does not push its matching document ids into its composite children.
  public AndDocIdSet(List<BlockDocIdSet> docIdSets, @Nullable Map<String, String> queryOptions) {
    this(docIdSets, queryOptions, false);
  }

  /// @param restrictionPushdownEnabled whether the matching document ids may be handed to the composite (AND/OR/NOT)
  ///                                   children. Resolved once per query from the `andRestrictionPushdown` query
  ///                                   option and the server default, see `QueryContext`.
  public AndDocIdSet(List<BlockDocIdSet> docIdSets, @Nullable Map<String, String> queryOptions,
      boolean restrictionPushdownEnabled) {
    _docIdSets = docIdSets;
    _cardinalityBasedRankingForScan =
        queryOptions != null && QueryOptionsUtils.isAndScanReorderingEnabled(queryOptions);
    _restrictionPushdownEnabled = restrictionPushdownEnabled;
  }

  @Override
  public BlockDocIdIterator iterator() {
    List<BlockDocIdSet> docIdSets = _docIdSets;
    Preconditions.checkState(docIdSets != null, "iterator() called on an already consumed AndDocIdSet");
    int numDocIdSets = docIdSets.size();
    // NOTE: Keep the order of BlockDocIdSets to preserve the order decided within FilterOperatorUtils.
    // TODO: Consider deciding the order based on the stats of BlockDocIdIterators
    BlockDocIdIterator[] allDocIdIterators = new BlockDocIdIterator[numDocIdSets];
    List<SortedDocIdIterator> sortedDocIdIterators = new ArrayList<>();
    List<BitmapBasedDocIdIterator> bitmapBasedDocIdIterators = new ArrayList<>();
    List<ScanBasedDocIdIterator> scanBasedDocIdIterators = new ArrayList<>();
    List<BlockDocIdIterator> remainingDocIdIterators = new ArrayList<>();
    // Composite (AND/OR/NOT) children whose evaluation is deferred so that the merged index document ids can be
    // pushed into them. Their slot in allDocIdIterators stays null until the lazy fallback below fills it.
    List<BlockDocIdSet> restrictableDocIdSets = new ArrayList<>();
    long numEntriesScannedForNonScanBasedDocIdSets = 0L;
    List<BlockDocIdSet> scanBasedDocIdSets = new ArrayList<>();

    for (int i = 0; i < numDocIdSets; i++) {
      BlockDocIdSet docIdSet = docIdSets.get(i);
      if (_restrictionPushdownEnabled && docIdSet.isApplyAndDeferrable()) {
        // NOTE: Do not call iterator() here. That evaluates the whole subtree over the segment, which is exactly what
        //       the push-down below avoids.
        restrictableDocIdSets.add(docIdSet);
        scanBasedDocIdSets.add(docIdSet);
        continue;
      }
      BlockDocIdIterator docIdIterator = docIdSet.iterator();
      allDocIdIterators[i] = docIdIterator;
      if (docIdIterator instanceof SortedDocIdIterator) {
        sortedDocIdIterators.add((SortedDocIdIterator) docIdIterator);
        numEntriesScannedForNonScanBasedDocIdSets += docIdSet.getNumEntriesScannedInFilter();
      } else if (docIdIterator instanceof BitmapBasedDocIdIterator) {
        bitmapBasedDocIdIterators.add((BitmapBasedDocIdIterator) docIdIterator);
        numEntriesScannedForNonScanBasedDocIdSets += docIdSet.getNumEntriesScannedInFilter();
      } else if (docIdIterator instanceof ScanBasedDocIdIterator) {
        scanBasedDocIdIterators.add((ScanBasedDocIdIterator) docIdIterator);
        scanBasedDocIdSets.add(docIdSet);
      } else {
        remainingDocIdIterators.add(docIdIterator);
        scanBasedDocIdSets.add(docIdSet);
      }
    }

    // Publish the stats state before dropping the children, so that a concurrent reader of
    // getNumEntriesScannedInFilter() never sees it disappear, then set _docIdSets to null so that the underlying
    // BlockDocIdSets can be garbage collected
    _numEntriesScannedInFilter = numEntriesScannedForNonScanBasedDocIdSets;
    _scanBasedDocIdSets.set(scanBasedDocIdSets);
    _docIdSets = null;

    // evaluate the bitmaps in the order of the lowest matching num docIds comes first, so that we minimize the number
    // of containers (range) for comparison from the beginning, as will minimize the effort of bitmap AND application
    bitmapBasedDocIdIterators.sort(Comparator.comparing(x -> x.getDocIds().getCardinality()));

    // Evaluate the scan based operator with the highest cardinality coming first, this potentially reduce the range of
    // scanning from the beginning. Automatically place N/A cardinality column (negative infinity) to the back as we
    // want to evaluate these unestimated predicates in the end.
    // TODO: 1. remainingDocIdIterators currently doesn't report cardinality; therefore, it cannot be
    //          prioritized even if it provides high effective cardinality, one way to do this is to let AND/OR
    //          DocIdIterators bubble up cardinality for the sort to happen recursively for nested AND-OR predicates
    if (_cardinalityBasedRankingForScan) {
      scanBasedDocIdIterators.sort(Comparator.comparing(x -> (-x.getEstimatedCardinality(true))));
    }

    int numSortedDocIdIterators = sortedDocIdIterators.size();
    int numBitmapBasedDocIdIterators = bitmapBasedDocIdIterators.size();
    int numScanBasedDocIdIterators = scanBasedDocIdIterators.size();
    int numRemainingDocIdIterators = remainingDocIdIterators.size();
    int numRestrictableDocIdSets = restrictableDocIdSets.size();
    int numIndexBasedDocIdIterators = numSortedDocIdIterators + numBitmapBasedDocIdIterators;
    if ((numIndexBasedDocIdIterators > 0 && (numScanBasedDocIdIterators > 0 || numRestrictableDocIdSets > 0))
        || numIndexBasedDocIdIterators > 1) {
      // When there are at least one index-base BlockDocIdIterator (SortedDocIdIterator or BitmapBasedDocIdIterator)
      // and at least one ScanBasedDocIdIterator, or more than one index-based BlockDocIdIterator, merge them and
      // construct a RangelessBitmapDocIdIterator from the merged document ids. If there is no remaining
      // BlockDocIdIterator, directly return the merged RangelessBitmapDocIdIterator; otherwise, construct and return
      // an AndDocIdIterator with the merged RangelessBitmapDocIdIterator and the remaining BlockDocIdIterators.

      ImmutableRoaringBitmap docIds;
      if (numSortedDocIdIterators > 0) {
        List<IntPair> docIdRanges;
        if (numSortedDocIdIterators == 1) {
          docIdRanges = sortedDocIdIterators.get(0).getDocIdRanges();
        } else {
          List<List<IntPair>> docIdRangesList = new ArrayList<>(numSortedDocIdIterators);
          for (SortedDocIdIterator sortedDocIdIterator : sortedDocIdIterators) {
            docIdRangesList.add(sortedDocIdIterator.getDocIdRanges());
          }
          // TODO: Optimize this
          docIdRanges = SortedRangeIntersection.intersectSortedRangeSets(docIdRangesList);
        }
        MutableRoaringBitmap mutableDocIds = new MutableRoaringBitmap();
        for (IntPair docIdRange : docIdRanges) {
          // NOTE: docIdRange has inclusive start and end.
          mutableDocIds.add(docIdRange.getLeft(), docIdRange.getRight() + 1L);
        }
        for (BitmapBasedDocIdIterator bitmapBasedDocIdIterator : bitmapBasedDocIdIterators) {
          mutableDocIds.and(bitmapBasedDocIdIterator.getDocIds());
        }
        docIds = mutableDocIds;
      } else {
        if (numBitmapBasedDocIdIterators == 1) {
          docIds = bitmapBasedDocIdIterators.get(0).getDocIds();
        } else {
          // NOTE: Intersect the two lowest-cardinality bitmaps (guaranteed by the sort above) into a fresh result
          //       with the static and(), which allocates only the intersection's containers, then intersect the
          //       remaining bitmaps into it in place (the intersection is usually much smaller than any operand).
          //       Inputs are never mutated.
          MutableRoaringBitmap mutableDocIds = ImmutableRoaringBitmap.and(
              bitmapBasedDocIdIterators.get(0).getDocIds(), bitmapBasedDocIdIterators.get(1).getDocIds());
          for (int i = 2; i < numBitmapBasedDocIdIterators; i++) {
            mutableDocIds.and(bitmapBasedDocIdIterators.get(i).getDocIds());
          }
          docIds = mutableDocIds;
        }
      }
      for (ScanBasedDocIdIterator scanBasedDocIdIterator : scanBasedDocIdIterators) {
        docIds = scanBasedDocIdIterator.applyAnd(docIds);
      }
      // Hand the matching document ids to the composite children, so that a scan-based predicate nested inside one of
      // them (typically under an OR) is only evaluated on the documents that can still match this AND.
      for (int i = 0; i < numRestrictableDocIdSets; i++) {
        if (docIds.isEmpty()) {
          // Nothing can match any more, but the remaining children still hold reader contexts
          for (int j = i; j < numRestrictableDocIdSets; j++) {
            restrictableDocIdSets.get(j).release();
          }
          break;
        }
        docIds = restrictableDocIdSets.get(i).applyAnd(docIds);
      }
      RangelessBitmapDocIdIterator rangelessBitmapDocIdIterator = new RangelessBitmapDocIdIterator(docIds);
      if (numRemainingDocIdIterators == 0) {
        return rangelessBitmapDocIdIterator;
      } else {
        BlockDocIdIterator[] docIdIterators = new BlockDocIdIterator[numRemainingDocIdIterators + 1];
        docIdIterators[0] = rangelessBitmapDocIdIterator;
        for (int i = 0; i < numRemainingDocIdIterators; i++) {
          docIdIterators[i + 1] = remainingDocIdIterators.get(i);
        }
        return new AndDocIdIterator(docIdIterators);
      }
    } else {
      // Otherwise, construct and return an AndDocIdIterator with all BlockDocIdIterators.
      // There is no index-based document id set to seed the push-down with, so evaluate the deferred composite
      // children now and let AndDocIdIterator drive them lazily, as it did before the push-down existed.
      for (int i = 0; i < numDocIdSets; i++) {
        if (allDocIdIterators[i] == null) {
          allDocIdIterators[i] = docIdSets.get(i).iterator();
        }
      }
      return new AndDocIdIterator(allDocIdIterators);
    }
  }

  @Override
  public boolean isApplyAndDeferrable() {
    return _restrictionPushdownEnabled;
  }

  /// Intersects this AND with the candidate set by handing the candidates to [#iterator] as one more index-based
  /// child. Everything [#iterator] does -- merging the index children first, sorting bitmaps by cardinality,
  /// [org.apache.pinot.common.utils.config.QueryOptionsUtils#isAndScanReorderingEnabled], running every scan against
  /// the merged document ids -- then applies to the restricted evaluation too, with one implementation of AND.
  @Override
  public ImmutableRoaringBitmap applyAnd(ImmutableRoaringBitmap docIds) {
    List<BlockDocIdSet> docIdSets = _docIdSets;
    Preconditions.checkState(docIdSets != null, "applyAnd() called on an already consumed AndDocIdSet");
    if (docIds.isEmpty()) {
      // No child is evaluated, so none of them will close its own iterator
      for (BlockDocIdSet docIdSet : docIdSets) {
        docIdSet.release();
      }
      _scanBasedDocIdSets.set(docIdSets);
      _docIdSets = null;
      return new MutableRoaringBitmap();
    }
    List<BlockDocIdSet> docIdSetsWithCandidates = new ArrayList<>(docIdSets.size() + 1);
    docIdSetsWithCandidates.add(new RangelessBitmapDocIdSet(docIds));
    docIdSetsWithCandidates.addAll(docIdSets);
    _docIdSets = docIdSetsWithCandidates;
    BlockDocIdIterator docIdIterator = iterator();
    // iterator() returns the merged document ids directly whenever it has no lazy child left, which is the usual case
    // once the candidate set has forced the merge branch
    return docIdIterator instanceof BitmapBasedDocIdIterator ? ((BitmapBasedDocIdIterator) docIdIterator).getDocIds()
        : BlockDocIdSet.collect(docIdIterator);
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
  public void release() {
    List<BlockDocIdSet> docIdSets = _docIdSets;
    if (docIdSets != null) {
      for (BlockDocIdSet docIdSet : docIdSets) {
        docIdSet.release();
      }
    }
  }

  @Override
  public BlockDocIdSet getOptimizedDocIdSet() {
    return this;
  }
}
