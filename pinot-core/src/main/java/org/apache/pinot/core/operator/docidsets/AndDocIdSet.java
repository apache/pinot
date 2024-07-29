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


/**
 * The BlockDocIdSet to perform AND on all child BlockDocIdSets.
 * <p>The AndBlockDocIdSet will construct the BlockDocIdIterator based on the BlockDocIdIterators from the child
 * BlockDocIdSets:
 * <ul>
 *   <li>
 *     When there are at least one index-base BlockDocIdIterator (SortedDocIdIterator or BitmapBasedDocIdIterator) and
 *     at least one ScanBasedDocIdIterator, or more than one index-based BlockDocIdIterator, merge them and construct a
 *     RangelessBitmapDocIdIterator from the merged document ids. If there is no remaining BlockDocIdIterator, directly
 *     return the merged RangelessBitmapDocIdIterator; otherwise, construct and return an AndDocIdIterator with the
 *     merged RangelessBitmapDocIdIterator and the remaining BlockDocIdIterators.
 *   </li>
 *   <li>
 *     Otherwise, construct and return an AndDocIdIterator with all BlockDocIdIterators.
 *   </li>
 * </ul>
 */
public final class AndDocIdSet implements BlockDocIdSet {
  // Keep the scan based BlockDocIdSets to be accessed when collecting query execution stats
  private final AtomicReference<List<BlockDocIdSet>> _scanBasedDocIdSets = new AtomicReference<>();
  private final boolean _cardinalityBasedRankingForScan;
  private List<BlockDocIdSet> _docIdSets;
  private volatile long _numEntriesScannedInFilter;

  public AndDocIdSet(List<BlockDocIdSet> docIdSets, @Nullable Map<String, String> queryOptions) {
    _docIdSets = docIdSets;
    _cardinalityBasedRankingForScan =
        queryOptions != null && QueryOptionsUtils.isAndScanReorderingEnabled(queryOptions);
  }

  @Override
  public BlockDocIdIterator iterator() {
    int numDocIdSets = _docIdSets.size();
    // NOTE: Keep the order of BlockDocIdSets to preserve the order decided within FilterOperatorUtils.
    // TODO: Consider deciding the order based on the stats of BlockDocIdIterators
    BlockDocIdIterator[] allDocIdIterators = new BlockDocIdIterator[numDocIdSets];
    List<SortedDocIdIterator> sortedDocIdIterators = new ArrayList<>();
    List<BitmapBasedDocIdIterator> bitmapBasedDocIdIterators = new ArrayList<>();
    List<ScanBasedDocIdIterator> scanBasedDocIdIterators = new ArrayList<>();
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
      } else if (docIdIterator instanceof ScanBasedDocIdIterator) {
        scanBasedDocIdIterators.add((ScanBasedDocIdIterator) docIdIterator);
        scanBasedDocIdSets.add(docIdSet);
      } else {
        remainingDocIdIterators.add(docIdIterator);
        scanBasedDocIdSets.add(docIdSet);
      }
    }

    // Set _docIdSets to null so that underlying BlockDocIdSets can be garbage collected
    _docIdSets = null;
    _numEntriesScannedInFilter = numEntriesScannedForNonScanBasedDocIdSets;
    _scanBasedDocIdSets.set(scanBasedDocIdSets);

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
    int numIndexBasedDocIdIterators = numSortedDocIdIterators + numBitmapBasedDocIdIterators;
    if ((numIndexBasedDocIdIterators > 0 && numScanBasedDocIdIterators > 0) || numIndexBasedDocIdIterators > 1) {
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
          MutableRoaringBitmap mutableDocIds = bitmapBasedDocIdIterators.get(0).getDocIds().toMutableRoaringBitmap();
          for (int i = 1; i < numBitmapBasedDocIdIterators; i++) {
            mutableDocIds.and(bitmapBasedDocIdIterators.get(i).getDocIds());
          }
          docIds = mutableDocIds;
        }
      }
      for (ScanBasedDocIdIterator scanBasedDocIdIterator : scanBasedDocIdIterators) {
        docIds = scanBasedDocIdIterator.applyAnd(docIds);
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

      return new AndDocIdIterator(allDocIdIterators);
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
}
