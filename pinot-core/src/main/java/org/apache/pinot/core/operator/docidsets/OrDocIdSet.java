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
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.BitmapBasedDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.OrDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SortedDocIdIterator;
import org.apache.pinot.spi.utils.Pairs;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * The BlockDocIdSet to perform OR on all child BlockDocIdSets.
 * <p>The OrBlockDocIdSet will construct the BlockDocIdIterator based on the BlockDocIdIterators from the child
 * BlockDocIdSets:
 * <ul>
 *   <li>
 *     When there are more than one index-base BlockDocIdIterator (SortedDocIdIterator or BitmapBasedDocIdIterator),
 *     merge them and construct a BitmapDocIdIterator from the merged document ids. If there is no remaining
 *     BlockDocIdIterator, directly return the merged BitmapDocIdIterator; otherwise, construct and return an
 *     OrDocIdIterator with the merged BitmapDocIdIterator and the remaining BlockDocIdIterators.
 *   </li>
 *   <li>
 *     Otherwise, construct and return an OrDocIdIterator with all BlockDocIdIterators.
 *   </li>
 * </ul>
 */
public final class OrDocIdSet implements BlockDocIdSet {
  private final List<BlockDocIdSet> _docIdSets;
  private final int _numDocs;
  private long _numEntriesScannedInFilter = 0L;

  public OrDocIdSet(List<BlockDocIdSet> docIdSets, int numDocs) {
    _docIdSets = docIdSets instanceof ArrayList ? docIdSets : new ArrayList<>(docIdSets);
    _numDocs = numDocs;
  }

  @Override
  public BlockDocIdIterator iterator() {
    int numDocIdSets = _docIdSets.size();
    BlockDocIdIterator[] allDocIdIterators = new BlockDocIdIterator[numDocIdSets];
    List<SortedDocIdIterator> sortedDocIdIterators = new ArrayList<>();
    List<BitmapBasedDocIdIterator> bitmapBasedDocIdIterators = new ArrayList<>();
    List<BlockDocIdIterator> remainingDocIdIterators = new ArrayList<>();

    Iterator<BlockDocIdSet> iterator = _docIdSets.iterator();
    for (int i = 0; iterator.hasNext(); i++) {
      BlockDocIdSet blockDocIdSet = iterator.next();
      BlockDocIdIterator docIdIterator = blockDocIdSet.iterator();
      allDocIdIterators[i] = docIdIterator;
      if (docIdIterator instanceof SortedDocIdIterator) {
        sortedDocIdIterators.add((SortedDocIdIterator) docIdIterator);
        // aggregate the number of entries scanned in filter before removing the iterator
        _numEntriesScannedInFilter += blockDocIdSet.getNumEntriesScannedInFilter();
        // do not keep holding on to the _docIdRanges since they will occupy heap space during the query execution
        iterator.remove();
      } else if (docIdIterator instanceof BitmapBasedDocIdIterator) {
        bitmapBasedDocIdIterators.add((BitmapBasedDocIdIterator) docIdIterator);
        // aggregate the number of entries scanned in filter before removing the iterator
        // some BitmapBasedDocIdIterator may be generated from underlying index types (e.g. H3Index) that actually
        // scans documents, so we need to aggregate them here
        _numEntriesScannedInFilter += blockDocIdSet.getNumEntriesScannedInFilter();
        // do not keep holding on to the bitmaps since they will occupy heap space during the query execution
        iterator.remove();
      } else {
        remainingDocIdIterators.add(docIdIterator);
      }
    }
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
    for (BlockDocIdSet docIdSet : _docIdSets) {
      _numEntriesScannedInFilter += docIdSet.getNumEntriesScannedInFilter();
    }
    return _numEntriesScannedInFilter;
  }
}
