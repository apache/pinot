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

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.Constants;


public final class OrBlockDocIdSet implements BlockDocIdSet {
  /**
   * 
   */
  private final BlockDocIdIterator[] docIdIterators;
  final public AtomicLong timeMeasure = new AtomicLong(0);
  private List<BlockDocIdSet> docIdSets;
  private int maxDocId;
  private int minDocId;

  public OrBlockDocIdSet(List<BlockDocIdSet> blockDocIdSets) {
    this.docIdSets = blockDocIdSets;
    final BlockDocIdIterator[] docIdIterators = new BlockDocIdIterator[blockDocIdSets.size()];
    for (int srcId = 0; srcId < blockDocIdSets.size(); srcId++) {
      docIdIterators[srcId] = blockDocIdSets.get(srcId).iterator();
    }
    this.docIdIterators = docIdIterators;
    minDocId = Integer.MAX_VALUE;
    maxDocId = Integer.MIN_VALUE;
    for (BlockDocIdSet blockDocIdSet : blockDocIdSets) {
      if (blockDocIdSet instanceof SortedDocIdSet) {
        SortedDocIdSet sortedDocIdSet = (SortedDocIdSet) blockDocIdSet;
        minDocId = Math.min(minDocId, sortedDocIdSet.getMinDocId());
        maxDocId = Math.max(maxDocId, sortedDocIdSet.getMaxDocId());
      } else if (blockDocIdSet instanceof AndBlockDocIdSet) {
        AndBlockDocIdSet andBlockDocIdSet = (AndBlockDocIdSet) blockDocIdSet;
        minDocId = Math.min(minDocId, andBlockDocIdSet.getMinDocId());
        maxDocId = Math.max(maxDocId, andBlockDocIdSet.getMaxDocId());
      }
    }
    for (BlockDocIdSet blockDocIdSet : blockDocIdSets) {
      if (blockDocIdSet instanceof SortedDocIdSet) {
        SortedDocIdSet sortedDocIdSet = (SortedDocIdSet) blockDocIdSet;
        sortedDocIdSet.setStartDocId(minDocId);
        sortedDocIdSet.setEndDocId(maxDocId);
      } else if (blockDocIdSet instanceof OrBlockDocIdSet) {
        OrBlockDocIdSet orBlockDocIdSet = (OrBlockDocIdSet) blockDocIdSet;
        orBlockDocIdSet.setStartDocId(minDocId);
        orBlockDocIdSet.setEndDocId(maxDocId);
      } else if (blockDocIdSet instanceof ScanBasedSingleValueDocIdSet) {
        ScanBasedSingleValueDocIdSet scanBasedSingleValueDocIdSet = (ScanBasedSingleValueDocIdSet) blockDocIdSet;
        scanBasedSingleValueDocIdSet.setStartDocId(minDocId);
        scanBasedSingleValueDocIdSet.setEndDocId(maxDocId);
      } else if (blockDocIdSet instanceof ScanBasedMultiValueDocIdSet) {
        ScanBasedMultiValueDocIdSet scanBasedMultiValueDocIdSet = (ScanBasedMultiValueDocIdSet) blockDocIdSet;
        scanBasedMultiValueDocIdSet.setStartDocId(minDocId);
        scanBasedMultiValueDocIdSet.setEndDocId(maxDocId);
      }
    }
  }

  @Override
  public BlockDocIdIterator iterator() {

    return new BlockDocIdIterator() {
      final PriorityQueue<IntPair> queue = new PriorityQueue<IntPair>(docIdIterators.length,
          new Pairs.AscendingIntPairComparator());
      final boolean[] iteratorIsInQueue = new boolean[docIdIterators.length];
      int currentDocId = 0;

      @Override
      public int advance(int targetDocId) {
        if (currentDocId == Constants.EOF) {
          return Constants.EOF;
        }

        long start = System.nanoTime();

        // Remove iterators that are before the target document id from the queue
        Iterator<IntPair> iterator = queue.iterator();
        while (iterator.hasNext()) {
          IntPair pair = iterator.next();
          if (pair.getA() < targetDocId) {
            iterator.remove();
            iteratorIsInQueue[pair.getB()] = false;
          }
        }

        // Advance all iterators that are not in the queue to the target document id
        for (int i = 0; i < docIdIterators.length; i++) {
          if (!iteratorIsInQueue[i]) {
            int next = docIdIterators[i].advance(targetDocId);
            if (next != Constants.EOF) {
              queue.add(new IntPair(next, i));
            }
            iteratorIsInQueue[i] = true;
          }
        }

        // Consume the first element, removing other iterators pointing to the same document id
        if (queue.size() > 0) {
          IntPair pair = queue.remove();
          iteratorIsInQueue[pair.getB()] = false;
          currentDocId = pair.getA();

          while (queue.size() > 0 && queue.peek().getA() == currentDocId) {
            IntPair remove = queue.remove();
            iteratorIsInQueue[remove.getB()] = false;
          }
        } else {
          currentDocId = Constants.EOF;
        }

        long end = System.nanoTime();
        timeMeasure.addAndGet(end - start);
        return currentDocId;
      }

      @Override
      public int next() {
        long start = System.nanoTime();

        // Grab the next value from each iterator, if it's not in the queue
        for (int i = 0; i < docIdIterators.length; i++) {
          if (!iteratorIsInQueue[i]) {
            int next = docIdIterators[i].next();
            if (next != Constants.EOF) {
              queue.add(new IntPair(next, i));
            }
            iteratorIsInQueue[i] = true;
          }
        }

        // Consume the first element, removing other iterators pointing to the same document id
        if (queue.size() > 0) {
          IntPair pair = queue.remove();
          iteratorIsInQueue[pair.getB()] = false;
          currentDocId = pair.getA();

          while (queue.size() > 0 && queue.peek().getA() == currentDocId) {
            pair = queue.remove();
            iteratorIsInQueue[pair.getB()] = false;
          }
        } else {
          currentDocId = Constants.EOF;
        }

        long end = System.nanoTime();
        timeMeasure.addAndGet(end - start);
        return currentDocId;
      }

      @Override
      public int currentDocId() {
        return currentDocId;
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getRaw() {
    return (T) this.docIdSets;
  }

  public void setStartDocId(int startDocId) {
    for (BlockDocIdSet blockDocIdSet : docIdSets) {
      if (blockDocIdSet instanceof ScanBasedSingleValueDocIdSet) {
        ScanBasedSingleValueDocIdSet scanBasedSingleValueDocIdSet = (ScanBasedSingleValueDocIdSet) blockDocIdSet;
        scanBasedSingleValueDocIdSet.setStartDocId(startDocId);
      }
      if (blockDocIdSet instanceof ScanBasedMultiValueDocIdSet) {
        ScanBasedMultiValueDocIdSet scanBasedMultiValueDocIdSet = (ScanBasedMultiValueDocIdSet) blockDocIdSet;
        scanBasedMultiValueDocIdSet.setStartDocId(startDocId);
      }
      if (blockDocIdSet instanceof BitmapDocIdSet) {
        BitmapDocIdSet bitmapDocIdSet = (BitmapDocIdSet) blockDocIdSet;
        bitmapDocIdSet.setStartDocId(startDocId);
      }
    }
  }

  public void setEndDocId(int endDocId) {
    for (BlockDocIdSet blockDocIdSet : docIdSets) {
      if (blockDocIdSet instanceof ScanBasedSingleValueDocIdSet) {
        ScanBasedSingleValueDocIdSet scanBasedSingleValueDocIdSet = (ScanBasedSingleValueDocIdSet) blockDocIdSet;
        scanBasedSingleValueDocIdSet.setEndDocId(endDocId);
      }
      if (blockDocIdSet instanceof ScanBasedMultiValueDocIdSet) {
        ScanBasedMultiValueDocIdSet scanBasedMultiValueDocIdSet = (ScanBasedMultiValueDocIdSet) blockDocIdSet;
        scanBasedMultiValueDocIdSet.setEndDocId(endDocId);
      }
      if (blockDocIdSet instanceof BitmapDocIdSet) {
        BitmapDocIdSet bitmapDocIdSet = (BitmapDocIdSet) blockDocIdSet;
        bitmapDocIdSet.setEndDocId(endDocId);
      }
    }
  }
}
