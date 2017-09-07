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
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public final class OrBlockDocIdSet implements FilterBlockDocIdSet {
  private final List<FilterBlockDocIdSet> _docIdSets;

  private int _minDocId = Integer.MAX_VALUE;
  private int _maxDocId = Integer.MIN_VALUE;

  public OrBlockDocIdSet(List<FilterBlockDocIdSet> docIdSets) {
    _docIdSets = docIdSets;

    for (FilterBlockDocIdSet docIdSet : docIdSets) {
      _minDocId = Math.min(_minDocId, docIdSet.getMinDocId());
      _maxDocId = Math.max(_maxDocId, docIdSet.getMaxDocId());
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
    _minDocId = Math.max(_minDocId, startDocId);
  }

  @Override
  public void setEndDocId(int endDocId) {
    _maxDocId = Math.min(_maxDocId, endDocId);
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    long numEntriesScannedInFilter = 0L;
    for (FilterBlockDocIdSet docIdSet : _docIdSets) {
      numEntriesScannedInFilter += docIdSet.getNumEntriesScannedInFilter();
    }
    return numEntriesScannedInFilter;
  }

  @Override
  public BlockDocIdIterator iterator() {
    boolean useBitmapOr = false;
    for (BlockDocIdSet docIdSet : _docIdSets) {
      if (docIdSet instanceof BitmapDocIdSet) {
        useBitmapOr = true;
        break;
      }
    }
    if (useBitmapOr) {
      List<BlockDocIdIterator> iterators = new ArrayList<>();
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      for (BlockDocIdSet docIdSet : _docIdSets) {
        if (docIdSet instanceof SortedDocIdSet) {
          List<Pairs.IntPair> pairs = docIdSet.getRaw();
          for (Pairs.IntPair pair : pairs) {
            // Add takes [start, end) i.e inclusive start, exclusive end
            bitmap.add(pair.getLeft(), pair.getRight() + 1);
          }
        } else if (docIdSet instanceof BitmapDocIdSet) {
          bitmap.or((ImmutableRoaringBitmap) docIdSet.getRaw());
        } else {
          iterators.add(docIdSet.iterator());
        }
      }
      IntIterator intIterator = bitmap.getIntIterator();
      BitmapDocIdIterator bitmapDocIdIterator = new BitmapDocIdIterator(intIterator);
      bitmapDocIdIterator.setStartDocId(_minDocId);
      bitmapDocIdIterator.setEndDocId(_maxDocId);
      if (iterators.isEmpty()) {
        return bitmapDocIdIterator;
      } else {
        iterators.add(bitmapDocIdIterator);
      }
      return new OrDocIdIterator(iterators.toArray(new BlockDocIdIterator[iterators.size()]), _minDocId, _maxDocId);
    } else {
      int numDocIdSets = _docIdSets.size();
      BlockDocIdIterator[] iterators = new BlockDocIdIterator[numDocIdSets];
      for (int i = 0; i < numDocIdSets; i++) {
        iterators[i] = _docIdSets.get(i).iterator();
      }
      return new OrDocIdIterator(iterators, _minDocId, _maxDocId);
    }
  }

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException();
  }
}
