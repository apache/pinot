/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.dociditerators.AndDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import com.linkedin.pinot.core.operator.filter.AndOperator;


public final class AndBlockDocIdSet implements FilterBlockDocIdSet {
  /**
   * 
   */
  static final Logger LOGGER = LoggerFactory.getLogger(AndOperator.class);
  public final AtomicLong timeMeasure = new AtomicLong(0);
  private List<FilterBlockDocIdSet> blockDocIdSets;
  private int minDocId = Integer.MIN_VALUE;
  private int maxDocId = Integer.MAX_VALUE;
  private IntIterator intIterator;
  private BlockDocIdIterator[] docIdIterators;

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
    long start, end;
    start = System.currentTimeMillis();
    List<BlockDocIdIterator> rawIterators = new ArrayList<>();
    boolean useBitmapBasedIntersection = false;
    for (BlockDocIdSet docIdSet : blockDocIdSets) {
      if (docIdSet instanceof BitmapDocIdSet) {
        useBitmapBasedIntersection = true;
      }
    }
    if (useBitmapBasedIntersection) {
      List<ImmutableRoaringBitmap> allBitmaps = new ArrayList<ImmutableRoaringBitmap>();
      for (BlockDocIdSet docIdSet : blockDocIdSets) {
        if (docIdSet instanceof SortedDocIdSet) {
          MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
          SortedDocIdSet sortedDocIdSet = (SortedDocIdSet) docIdSet;
          List<Pair<Integer, Integer>> pairs = sortedDocIdSet.getRaw();
          for (Pair<Integer, Integer> pair : pairs) {
            bitmap.add(pair.getLeft(), pair.getRight() + 1); //add takes [start, end) i.e inclusive start, exclusive end.
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
      MutableRoaringBitmap answer = (MutableRoaringBitmap) allBitmaps.get(0).clone();
      for (int i = 1; i < allBitmaps.size(); i++) {
        answer.and(allBitmaps.get(i));
      }
      intIterator = answer.getIntIterator();
      BitmapDocIdIterator singleBitmapBlockIdIterator = new BitmapDocIdIterator(intIterator);
      singleBitmapBlockIdIterator.setStartDocId(minDocId);
      singleBitmapBlockIdIterator.setEndDocId(maxDocId);
      end = System.currentTimeMillis();
      rawIterators.add(0,singleBitmapBlockIdIterator);
      docIdIterators = new BlockDocIdIterator[rawIterators.size()];
      rawIterators.toArray(docIdIterators);
    } else {
      docIdIterators = new BlockDocIdIterator[blockDocIdSets.size()];
      for (int srcId = 0; srcId < blockDocIdSets.size(); srcId++) {
        docIdIterators[srcId] = blockDocIdSets.get(srcId).iterator();
      }
    }
//    if (docIdIterators.length == 1) {
//      return docIdIterators[0];
//    } else {
      return new AndDocIdIterator(docIdIterators);
//    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getRaw() {
    return (T) intIterator;
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
}
