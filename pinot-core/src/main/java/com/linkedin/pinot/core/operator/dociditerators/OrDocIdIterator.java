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
package com.linkedin.pinot.core.operator.dociditerators;

import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;

public final class OrDocIdIterator implements BlockDocIdIterator {
  private BlockDocIdIterator[] docIdIterators;

  final PriorityQueue<IntPair> queue;
  final boolean[] iteratorIsInQueue;
  int currentDocId = -1;
  private int minDocId;
  private int maxDocId;
  private AtomicLong timeMeasure = new AtomicLong(0);
  IntPair[] pointers;

  /**
   * @param docIdIterators
   */
  public OrDocIdIterator(BlockDocIdIterator[] docIdIterators) {
    this.docIdIterators = docIdIterators;
    queue =
        new PriorityQueue<IntPair>(docIdIterators.length, new Pairs.AscendingIntPairComparator());
    iteratorIsInQueue = new boolean[docIdIterators.length];
    pointers= new IntPair[docIdIterators.length];
    for (int i = 0; i < docIdIterators.length; i++) {
      pointers[i] = new IntPair(0, i);
    }
  }

  @Override
  public int advance(int targetDocId) {
    if (currentDocId == Constants.EOF) {
      return Constants.EOF;
    }
    if (targetDocId < minDocId) {
      targetDocId = minDocId;
    } else if (targetDocId > maxDocId) {
      currentDocId = Constants.EOF;
      return currentDocId;
    }
    long start = System.nanoTime();

    // Remove iterators that are before the target document id from the queue
    Iterator<IntPair> iterator = queue.iterator();
    while (iterator.hasNext()) {
      IntPair pair = iterator.next();
      if (pair.getLeft() < targetDocId) {
        iterator.remove();
        iteratorIsInQueue[pair.getRight()] = false;
      }
    }

    // Advance all iterators that are not in the queue to the target document id
    for (int i = 0; i < docIdIterators.length; i++) {
      if (!iteratorIsInQueue[i]) {
        int nextDocId = docIdIterators[i].advance(targetDocId);
        if (nextDocId != Constants.EOF) {
          pointers[i].setLeft(nextDocId);
          queue.add(pointers[i]);
        }
        iteratorIsInQueue[i] = true;
      }
    }

    // Return the first element
    if (queue.size() > 0) {
      currentDocId = queue.peek().getLeft();
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
    while (queue.size() > 0 && queue.peek().getLeft() <= currentDocId) {
      IntPair pair = queue.remove();
      iteratorIsInQueue[pair.getRight()] = false;
    }
    currentDocId++;
    // Grab the next value from each iterator, if it's not in the queue
    for (int i = 0; i < docIdIterators.length; i++) {
      if (!iteratorIsInQueue[i]) {
        int nextDocId = docIdIterators[i].advance(currentDocId);
        if (nextDocId != Constants.EOF) {
          if (!(nextDocId <= maxDocId && nextDocId >= minDocId) && nextDocId >= currentDocId) {
            throw new RuntimeException("next Doc : " + nextDocId
                + " should never crossing the range : [ " + minDocId + ", " + maxDocId + " ]");
          }
          queue.add(new IntPair(nextDocId, i));
        }
        iteratorIsInQueue[i] = true;
      }
    }

    if (queue.size() > 0) {
      currentDocId = queue.peek().getLeft();
    } else {
      currentDocId = Constants.EOF;
    }
    long end = System.currentTimeMillis();
    timeMeasure.addAndGet(end - start);
    // Remove this after tracing is added
    // if (currentDocId == Constants.EOF) {
    // LOGGER.debug("OR took:" + timeMeasure.get());
    // }

    return currentDocId;
  }

  @Override
  public int currentDocId() {
    return currentDocId;
  }

  public void setStartDocId(int minDocId) {
    this.minDocId = minDocId;
  }

  public void setEndDocId(int maxDocId) {
    this.maxDocId = maxDocId;
  }
}
