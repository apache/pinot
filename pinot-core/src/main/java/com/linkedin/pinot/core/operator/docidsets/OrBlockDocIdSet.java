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
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;


public final class OrBlockDocIdSet implements FilterBlockDocIdSet {
  /**
   * 
   */
  private final BlockDocIdIterator[] docIdIterators;
  final public AtomicLong timeMeasure = new AtomicLong(0);
  private List<FilterBlockDocIdSet> docIdSets;
  private int maxDocId = Integer.MIN_VALUE;
  private int minDocId = Integer.MAX_VALUE;

  public OrBlockDocIdSet(List<FilterBlockDocIdSet> blockDocIdSets) {
    this.docIdSets = blockDocIdSets;
    final BlockDocIdIterator[] docIdIterators = new BlockDocIdIterator[blockDocIdSets.size()];
    for (int srcId = 0; srcId < blockDocIdSets.size(); srcId++) {
      docIdIterators[srcId] = blockDocIdSets.get(srcId).iterator();
    }
    this.docIdIterators = docIdIterators;
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

    return new BlockDocIdIterator() {
      final PriorityQueue<IntPair> queue = new PriorityQueue<IntPair>(docIdIterators.length,
          new Pairs.AscendingIntPairComparator());
      final boolean[] iteratorIsInQueue = new boolean[docIdIterators.length];
      int currentDocId = -1;

      @Override
      public int advance(int targetDocId) {
        if (currentDocId == Constants.EOF) {
          return Constants.EOF;
        }
        if (targetDocId < getMinDocId()) {
          targetDocId = getMinDocId();
        } else if (targetDocId > getMaxDocId()) {
          currentDocId = Constants.EOF;
          return currentDocId;
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
            int nextDocId = docIdIterators[i].advance(targetDocId);
            if (nextDocId != Constants.EOF) {
              queue.add(new IntPair(nextDocId, i));
            }
            iteratorIsInQueue[i] = true;
          }
        }

        // Return the first element
        if (queue.size() > 0) {
          currentDocId = queue.peek().getA();
        } else {
          currentDocId = Constants.EOF;
        }

        long end = System.nanoTime();
        timeMeasure.addAndGet(end - start);
        return currentDocId;
      }

      @Override
      public int next() {
        long start = System.currentTimeMillis();

        if (currentDocId == Constants.EOF) {
          return currentDocId;
        }
        while (queue.size() > 0 && queue.peek().getA() <= currentDocId) {
          IntPair pair = queue.remove();
          iteratorIsInQueue[pair.getB()] = false;
        }
        currentDocId++;
        // Grab the next value from each iterator, if it's not in the queue
        for (int i = 0; i < docIdIterators.length; i++) {
          if (!iteratorIsInQueue[i]) {
            int nextDocId = docIdIterators[i].advance(currentDocId);
            if (nextDocId != Constants.EOF) {
              if (!(nextDocId <= getMaxDocId() && nextDocId >= getMinDocId() && nextDocId >= currentDocId)) {
                throw new RuntimeException("next Doc : " + nextDocId + " should never crossing the range : [ " + getMinDocId() + ", " + getMaxDocId() + " ]");
              }
              queue.add(new IntPair(nextDocId, i));
            }
            iteratorIsInQueue[i] = true;
          }
        }

        if (queue.size() > 0) {
          currentDocId = queue.peek().getA();
        } else {
          currentDocId = Constants.EOF;
        }
        long end = System.currentTimeMillis();
        timeMeasure.addAndGet(end - start);
        //Remove this after tracing is added
//      if (currentDocId == Constants.EOF) {
//        LOGGER.debug("OR took:" + timeMeasure.get());
//      }

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

}
