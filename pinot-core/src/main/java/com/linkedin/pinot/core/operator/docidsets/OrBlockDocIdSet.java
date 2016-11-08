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

import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.operator.dociditerators.BitmapDocIdIterator;
import com.linkedin.pinot.core.operator.dociditerators.OrDocIdIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public final class OrBlockDocIdSet implements FilterBlockDocIdSet {
  /**
   *
   */
  final public AtomicLong timeMeasure = new AtomicLong(0);
  private List<FilterBlockDocIdSet> docIdSets;
  private int maxDocId = Integer.MIN_VALUE;
  private int minDocId = Integer.MAX_VALUE;
  private IntIterator intIterator;
  private BlockDocIdIterator[] docIdIterators;

  public OrBlockDocIdSet(List<FilterBlockDocIdSet> blockDocIdSets) {
    this.docIdSets = blockDocIdSets;
    updateMinMaxRange();
  }

  private void updateMinMaxRange() {
    for (FilterBlockDocIdSet blockDocIdSet : docIdSets) {
      minDocId = Math.min(minDocId, blockDocIdSet.getMinDocId());
      maxDocId = Math.max(maxDocId, blockDocIdSet.getMaxDocId());
    }
    for (FilterBlockDocIdSet blockDocIdSet : docIdSets) {
      blockDocIdSet.setStartDocId(minDocId);
      blockDocIdSet.setEndDocId(maxDocId);
    }
  }

  @Override
  public int getMaxDocId() {
    return maxDocId;
  }

  @Override
  public int getMinDocId() {
    return minDocId;
  }

  @Override
  public BlockDocIdIterator iterator() {
    List<BlockDocIdIterator> rawIterators = new ArrayList<>();
    boolean useBitmapOr = false;
    for (BlockDocIdSet docIdSet : docIdSets) {
      if (docIdSet instanceof BitmapDocIdSet) {
        useBitmapOr = true;
      }
    }
    if (useBitmapOr) {
      List<ImmutableRoaringBitmap> allBitmaps = new ArrayList<ImmutableRoaringBitmap>();
      for (BlockDocIdSet docIdSet : docIdSets) {
        if (docIdSet instanceof SortedDocIdSet) {
          MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
          SortedDocIdSet sortedDocIdSet = (SortedDocIdSet) docIdSet;
          List<Pairs.IntPair> pairs = sortedDocIdSet.getRaw();
          for (Pairs.IntPair pair : pairs) {
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
      MutableRoaringBitmap answer = allBitmaps.get(0).toMutableRoaringBitmap();
      for (int i = 1; i < allBitmaps.size(); i++) {
        answer.or(allBitmaps.get(i));
      }
      intIterator = answer.getIntIterator();
      BitmapDocIdIterator singleBitmapBlockIdIterator = new BitmapDocIdIterator(intIterator);
      singleBitmapBlockIdIterator.setStartDocId(minDocId);
      singleBitmapBlockIdIterator.setEndDocId(maxDocId);
      rawIterators.add(singleBitmapBlockIdIterator);
      docIdIterators = new BlockDocIdIterator[rawIterators.size()];
      rawIterators.toArray(docIdIterators);
    } else {
      docIdIterators = new BlockDocIdIterator[docIdSets.size()];
      for (int srcId = 0; srcId < docIdSets.size(); srcId++) {
        docIdIterators[srcId] = docIdSets.get(srcId).iterator();
      }
    }
//    if (docIdIterators.length == 1) {
//      return docIdIterators[0];
//    } else {
      OrDocIdIterator orDocIdIterator = new OrDocIdIterator(docIdIterators);
      orDocIdIterator.setStartDocId(minDocId);
      orDocIdIterator.setEndDocId(maxDocId);
      return orDocIdIterator;
//    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getRaw() {
    return (T) this.docIdSets;
  }

  @Override
  public void setStartDocId(int startDocId) {
    minDocId = Math.min(minDocId, startDocId);
    updateMinMaxRange();
  }

  @Override
  public void setEndDocId(int endDocId) {
    maxDocId = Math.max(maxDocId, endDocId);
    updateMinMaxRange();
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    long numEntriesScannedInFilter = 0L;
    for (FilterBlockDocIdSet docIdSet : docIdSets) {
      numEntriesScannedInFilter += docIdSet.getNumEntriesScannedInFilter();
    }
    return numEntriesScannedInFilter;
  }
}
