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
import org.apache.pinot.common.utils.Pairs.IntPair;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.AndDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.RangelessBitmapDocIdIterator;
import org.apache.pinot.core.util.SortedRangeIntersection;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public final class AndBlockDocIdSet implements FilterBlockDocIdSet {
  private final List<FilterBlockDocIdSet> _children;

  private int _minDocId = Integer.MIN_VALUE;
  // Inclusive
  // TODO: Make it exclusive
  private int _maxDocId = Integer.MAX_VALUE;

  public AndBlockDocIdSet(List<FilterBlockDocIdSet> children) {
    _children = children;

    // Gather start/end doc id from the children
    for (FilterBlockDocIdSet child : _children) {
      _minDocId = Math.max(_minDocId, child.getMinDocId());
      _maxDocId = Math.min(_maxDocId, child.getMaxDocId());
    }

    // Update the start/end doc id for the children
    for (FilterBlockDocIdSet child : _children) {
      child.setStartDocId(_minDocId);
      child.setEndDocId(_maxDocId);
    }
  }

  @Override
  public int getMinDocId() {
    return _minDocId;
  }

  @Override
  public int getMaxDocId() {
    return _maxDocId;
  }

  @Override
  public void setStartDocId(int startDocId) {
    if (startDocId > _minDocId) {
      _minDocId = startDocId;
      // Update the start doc id for the children
      for (FilterBlockDocIdSet child : _children) {
        child.setStartDocId(startDocId);
      }
    }
  }

  @Override
  public void setEndDocId(int endDocId) {
    if (endDocId < _maxDocId) {
      _maxDocId = endDocId;
      // Update the end doc id for the children
      for (FilterBlockDocIdSet child : _children) {
        child.setEndDocId(endDocId);
      }
    }
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    long numEntriesScannedInFilter = 0L;
    for (FilterBlockDocIdSet child : _children) {
      numEntriesScannedInFilter += child.getNumEntriesScannedInFilter();
    }
    return numEntriesScannedInFilter;
  }

  // TODO: Optimize this
  @Override
  public BlockDocIdIterator iterator() {
    List<List<IntPair>> sortedRangeSets = new ArrayList<>();
    List<ImmutableRoaringBitmap> childBitmaps = new ArrayList<>();
    List<ScanBasedDocIdSet> scanBasedDocIdSets = new ArrayList<>();
    List<BlockDocIdIterator> remainingIterators = new ArrayList<>();

    for (BlockDocIdSet child : _children) {
      if (child instanceof SortedDocIdSet) {
        SortedDocIdSet sortedDocIdSet = (SortedDocIdSet) child;
        List<IntPair> pairs = sortedDocIdSet.getRaw();
        sortedRangeSets.add(pairs);
      } else if (child instanceof BitmapDocIdSet) {
        BitmapDocIdSet bitmapDocIdSet = (BitmapDocIdSet) child;
        ImmutableRoaringBitmap childBitmap = bitmapDocIdSet.getRaw();
        childBitmaps.add(childBitmap);
      } else if (child instanceof ScanBasedDocIdSet) {
        scanBasedDocIdSets.add((ScanBasedDocIdSet) child);
      } else {
        // TODO:handle child OR/AND as bitmap if possible
        remainingIterators.add(child.iterator());
      }
    }
    if (childBitmaps.size() == 0 && sortedRangeSets.size() == 0) {
      // When one or more of the operands are operators themselves, then we don't have a sorted or
      // bitmap index. In that case, just use the AndDocIdIterator to iterate over all of of the subtree.
      BlockDocIdIterator[] docIdIterators = new BlockDocIdIterator[_children.size()];
      for (int srcId = 0; srcId < _children.size(); srcId++) {
        docIdIterators[srcId] = _children.get(srcId).iterator();
      }
      return new AndDocIdIterator(docIdIterators);
    } else {
      MutableRoaringBitmap answer = null;

      // handle sorted ranges
      // TODO: will be nice to re-order sorted and bitmap index based on size
      if (sortedRangeSets.size() > 0) {
        List<IntPair> pairList;
        pairList = SortedRangeIntersection.intersectSortedRangeSets(sortedRangeSets);
        answer = new MutableRoaringBitmap();
        for (IntPair pair : pairList) {
          // end is exclusive
          answer.add(pair.getLeft(), pair.getRight() + 1);
        }
      }
      // handle bitmaps
      if (childBitmaps.size() > 0) {
        if (answer == null) {
          answer = childBitmaps.get(0).toMutableRoaringBitmap();
          for (int i = 1; i < childBitmaps.size(); i++) {
            answer.and(childBitmaps.get(i));
          }
        } else {
          for (int i = 0; i < childBitmaps.size(); i++) {
            answer.and(childBitmaps.get(i));
          }
        }
      }

      // handle raw iterators
      for (ScanBasedDocIdSet scanBasedDocIdSet : scanBasedDocIdSets) {
        answer = scanBasedDocIdSet.iterator().applyAnd(answer);
      }
      // if other iterators exists resort to iterator style intersection
      BlockDocIdIterator answerDocIdIterator = new RangelessBitmapDocIdIterator(answer.getIntIterator());
      if (remainingIterators.size() == 0) {
        return answerDocIdIterator;
      } else {
        BlockDocIdIterator[] docIdIterators = new BlockDocIdIterator[remainingIterators.size() + 1];
        docIdIterators[0] = answerDocIdIterator;
        for (int i = 0; i < remainingIterators.size(); i++) {
          docIdIterators[i + 1] = remainingIterators.get(i);
        }
        return new AndDocIdIterator(docIdIterators);
      }
    }
  }

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException();
  }
}
