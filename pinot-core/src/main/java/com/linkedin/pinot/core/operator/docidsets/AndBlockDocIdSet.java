/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.docidsets;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.dociditerators.AndDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.RangelessBitmapDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import com.linkedin.pinot.core.operator.filter.AndOperator;
import com.linkedin.pinot.core.util.SortedRangeIntersection;

public final class AndBlockDocIdSet implements FilterBlockDocIdSet {
  /**
   *
   */
  static final Logger LOGGER = LoggerFactory.getLogger(AndOperator.class);
  public final AtomicLong timeMeasure = new AtomicLong(0);
  private List<FilterBlockDocIdSet> blockDocIdSets;
  private int minDocId = Integer.MIN_VALUE;
  private int maxDocId = Integer.MAX_VALUE;
  MutableRoaringBitmap answer = null;
  boolean validate = false;

  public AndBlockDocIdSet(List<FilterBlockDocIdSet> blockDocIdSets) {
    this.blockDocIdSets = blockDocIdSets;
    updateMinMaxRange();
  }

  private void updateMinMaxRange() {
    for (FilterBlockDocIdSet blockDocIdSet : blockDocIdSets) {
      minDocId = Math.max(minDocId, blockDocIdSet.getMinDocId());
      maxDocId = Math.min(maxDocId, blockDocIdSet.getMaxDocId());
    }
    for (FilterBlockDocIdSet blockDocIdSet : blockDocIdSets) {
      blockDocIdSet.setStartDocId(minDocId);
      blockDocIdSet.setEndDocId(maxDocId);
    }
  }

  @Override
  public BlockDocIdIterator iterator() {
    //TODO: Remove this validation code once we have enough testing
    if (validate) {
      BlockDocIdIterator slowIterator = slowIterator();
      BlockDocIdIterator fastIterator = fastIterator();
      List<Integer> matchedIds = new ArrayList<>();
      while (true) {
        int docId1 = slowIterator.next();
        int docId2 = fastIterator.next();
        if (docId1 != docId2) {
          LOGGER.error("ERROR docId1:" + docId1 + " docId2:" + docId2);
        } else {
          matchedIds.add(docId1);
        }
        if (docId1 == Constants.EOF || docId2 == Constants.EOF) {
          break;
        }
      }
      answer = null;
    }
    return fastIterator();

  }

  public BlockDocIdIterator slowIterator() {
    List<BlockDocIdIterator> rawIterators = new ArrayList<>();
    boolean useBitmapBasedIntersection = false;
    for (BlockDocIdSet docIdSet : blockDocIdSets) {
      if (docIdSet instanceof BitmapDocIdSet) {
        useBitmapBasedIntersection = true;
      }
    }
    BlockDocIdIterator[] docIdIterators;
    if (useBitmapBasedIntersection) {
      List<ImmutableRoaringBitmap> allBitmaps = new ArrayList<ImmutableRoaringBitmap>();
      for (BlockDocIdSet docIdSet : blockDocIdSets) {
        if (docIdSet instanceof SortedDocIdSet) {
          MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
          SortedDocIdSet sortedDocIdSet = (SortedDocIdSet) docIdSet;
          List<IntPair> pairs = sortedDocIdSet.getRaw();
          for (IntPair pair : pairs) {
            bitmap.add(pair.getLeft(), pair.getRight() + 1); // add takes [start, end) i.e inclusive
                                                             // start, exclusive end.
          }
          allBitmaps.add(bitmap);
        } else if (docIdSet instanceof BitmapDocIdSet) {
          BitmapDocIdSet bitmapDocIdSet = (BitmapDocIdSet) docIdSet;
          ImmutableRoaringBitmap childBitmap = bitmapDocIdSet.getRaw();
          allBitmaps.add(childBitmap);
        } else {
          BlockDocIdIterator iterator = docIdSet.iterator();
          rawIterators.add(iterator);
        }
      }
      IntIterator intIterator;
      if (allBitmaps.size() > 1) {
        MutableRoaringBitmap answer = (MutableRoaringBitmap) allBitmaps.get(0).clone();
        for (int i = 1; i < allBitmaps.size(); i++) {
          answer.and(allBitmaps.get(i));
        }
        intIterator = answer.getIntIterator();
      } else {
        intIterator = allBitmaps.get(0).getIntIterator();
      }

      BitmapDocIdIterator singleBitmapBlockIdIterator = new BitmapDocIdIterator(intIterator);
      singleBitmapBlockIdIterator.setStartDocId(minDocId);
      singleBitmapBlockIdIterator.setEndDocId(maxDocId);
      rawIterators.add(0, singleBitmapBlockIdIterator);
      docIdIterators = new BlockDocIdIterator[rawIterators.size()];
      rawIterators.toArray(docIdIterators);
    } else {
      docIdIterators = new BlockDocIdIterator[blockDocIdSets.size()];
      for (int srcId = 0; srcId < blockDocIdSets.size(); srcId++) {
        docIdIterators[srcId] = blockDocIdSets.get(srcId).iterator();
      }
    }
    return new AndDocIdIterator(docIdIterators);
  }

  public BlockDocIdIterator fastIterator() {
    long start = System.currentTimeMillis();
    List<List<IntPair>> sortedRangeSets = new ArrayList<>();
    List<ImmutableRoaringBitmap> childBitmaps = new ArrayList<ImmutableRoaringBitmap>();
    List<FilterBlockDocIdSet> scanBasedDocIdSets = new ArrayList<>();
    List<BlockDocIdIterator> remainingIterators = new ArrayList<>();

    for (BlockDocIdSet docIdSet : blockDocIdSets) {
      if (docIdSet instanceof SortedDocIdSet) {
        SortedDocIdSet sortedDocIdSet = (SortedDocIdSet) docIdSet;
        List<IntPair> pairs = sortedDocIdSet.getRaw();
        sortedRangeSets.add(pairs);
      } else if (docIdSet instanceof BitmapDocIdSet) {
        BitmapDocIdSet bitmapDocIdSet = (BitmapDocIdSet) docIdSet;
        ImmutableRoaringBitmap childBitmap = bitmapDocIdSet.getRaw();
        childBitmaps.add(childBitmap);
      } else if (docIdSet instanceof ScanBasedSingleValueDocIdSet) {
        scanBasedDocIdSets.add((ScanBasedSingleValueDocIdSet) docIdSet);
      } else if (docIdSet instanceof ScanBasedMultiValueDocIdSet) {
        scanBasedDocIdSets.add((ScanBasedMultiValueDocIdSet) docIdSet);
      } else {
        // TODO:handle child OR/AND as bitmap if possible
        remainingIterators.add(docIdSet.iterator());
      }
    }
    if (childBitmaps.size() == 0 && sortedRangeSets.size() == 0) {
      // When one or more of the operands are operators themselves, then we don't have a sorted or
      // bitmap index. In that case, just use the AndDocIdIterator to iterate over all of of the subtree.
      BlockDocIdIterator[] docIdIterators = new BlockDocIdIterator[blockDocIdSets.size()];
      for (int srcId = 0; srcId < blockDocIdSets.size(); srcId++) {
        docIdIterators[srcId] = blockDocIdSets.get(srcId).iterator();
      }
      return new AndDocIdIterator(docIdIterators);
    } else {
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

      // At this point, we must have 'answer' to be non-null.
      assert (answer != null) : "sortedRangeSets=" + sortedRangeSets.size() + ",childBitmaps=" + childBitmaps.size();

      // handle raw iterators
      for (FilterBlockDocIdSet scanBasedDocIdSet : scanBasedDocIdSets) {
        ScanBasedDocIdIterator iterator = (ScanBasedDocIdIterator) scanBasedDocIdSet.iterator();
        MutableRoaringBitmap scanAnswer = iterator.applyAnd(answer);
        answer.and(scanAnswer);
      }
      long end = System.currentTimeMillis();
      LOGGER.debug("Time to evaluate and Filter:{}", (end - start));
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

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getRaw() {
    return (T) answer;
  }

  @Override
  public int getMinDocId() {
    return minDocId;
  }

  @Override
  public int getMaxDocId() {
    return maxDocId;
  }

  @Override
  public void setStartDocId(int startDocId) {
    minDocId = Math.max(minDocId, startDocId);
    updateMinMaxRange();
  }

  @Override
  public void setEndDocId(int endDocId) {
    maxDocId = Math.min(maxDocId, endDocId);
    updateMinMaxRange();
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    long numEntriesScannedInFilter = 0L;
    for (FilterBlockDocIdSet blockDocIdSet : blockDocIdSets) {
      numEntriesScannedInFilter += blockDocIdSet.getNumEntriesScannedInFilter();
    }
    return numEntriesScannedInFilter;
  }
}
