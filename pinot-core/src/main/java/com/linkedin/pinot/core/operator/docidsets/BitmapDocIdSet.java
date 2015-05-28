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

import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;


public class BitmapDocIdSet implements FilterBlockDocIdSet {

  final private ImmutableRoaringBitmap[] raw;
  public final AtomicLong timeMeasure = new AtomicLong(0);
  private BlockMetadata blockMetadata;
  BitmapBasedBlockIdSetIterator bitmapBasedBlockIdSetIterator;

  public BitmapDocIdSet(BlockMetadata blockMetadata, ImmutableRoaringBitmap... bitmaps) {
    this.blockMetadata = blockMetadata;
    raw = bitmaps;
    IntIterator[] iterators = new IntIterator[bitmaps.length];
    for (int i = 0; i < bitmaps.length; i++) {
      iterators[i] = bitmaps[i].getIntIterator();
    }
    bitmapBasedBlockIdSetIterator = new BitmapBasedBlockIdSetIterator(iterators);
    bitmapBasedBlockIdSetIterator.setStartDocId(blockMetadata.getStartDocId());
    bitmapBasedBlockIdSetIterator.setEndDocId(blockMetadata.getEndDocId());
  }

  public BitmapDocIdSet(BlockMetadata blockMetadata, MutableRoaringBitmap... bitmaps) {
    this.blockMetadata = blockMetadata;
    raw = bitmaps;
    IntIterator[] iterators = new IntIterator[bitmaps.length];
    for (int i = 0; i < bitmaps.length; i++) {
      iterators[i] = bitmaps[i].getIntIterator();
    }
    bitmapBasedBlockIdSetIterator = new BitmapBasedBlockIdSetIterator(iterators);
    bitmapBasedBlockIdSetIterator.setStartDocId(blockMetadata.getStartDocId());
    bitmapBasedBlockIdSetIterator.setEndDocId(blockMetadata.getEndDocId());
  }

  @Override
  public int getMinDocId() {
    return blockMetadata.getStartDocId();
  }

  @Override
  public int getMaxDocId() {
    return blockMetadata.getEndDocId();
  }

  /**
   * After setting the startDocId, next calls will always return from &gt;=startDocId
   * @param startDocId
   */
  public void setStartDocId(int startDocId) {
    bitmapBasedBlockIdSetIterator.setStartDocId(startDocId);
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds endDocId
   * @param endDocId
   */
  public void setEndDocId(int endDocId) {
    bitmapBasedBlockIdSetIterator.setEndDocId(endDocId);
  }

  @Override
  public BlockDocIdIterator iterator() {
    return bitmapBasedBlockIdSetIterator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Object> T getRaw() {
    return (T) raw;
  }

  @Override
  public String toString() {

    return Arrays.toString(raw);
  }

  public final class BitmapBasedBlockIdSetIterator implements BlockDocIdIterator {
    final private IntIterator[] iterators;
    private int endDocId;
    private int startDocId;

    public BitmapBasedBlockIdSetIterator(IntIterator[] iterators) {
      this.iterators = iterators;
    }

    public void setEndDocId(int endDocId) {
      this.endDocId = endDocId;
    }

    public void setStartDocId(int startDocId) {
      this.startDocId = startDocId;
    }
    //<docId Counter, postinglist Id> Int Pair
    PriorityQueue<IntPair> queue = new PriorityQueue<IntPair>(Math.max(1, raw.length), new Pairs.AscendingIntPairComparator());
    boolean[] iteratorIsInQueue = new boolean[raw.length];
    int currentDocId = -1;

    @Override
    public int advance(int targetDocId) {
      long start = System.nanoTime();
      if (targetDocId < startDocId) {
        targetDocId = startDocId;
      } else if (targetDocId > endDocId) {
        currentDocId = Constants.EOF;
      }
      if (currentDocId == Constants.EOF) {
        return currentDocId;
      }
      Iterator<IntPair> iterator = queue.iterator();
      //remove everything from the queue that is less than targetDocId
      while (iterator.hasNext()) {
        IntPair pair = iterator.next();
        if (pair.getA() < targetDocId) {
          iterator.remove();
          iteratorIsInQueue[pair.getB()] = false;
        }
      }
      //move the pointer until its great than or equal to targetDocId
      for (int i = 0; i < iterators.length; i++) {
        if (!iteratorIsInQueue[i]) {
          int next;
          while (iterators[i].hasNext()) {
            next = iterators[i].next();
            if (next > endDocId) {
              break;
            }
            if (next >= targetDocId) {
              queue.add(new IntPair(next, i));
              break;
            }
          }
          iteratorIsInQueue[i] = true;
        }
      }
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
      long start = System.nanoTime();
      if (currentDocId == Constants.EOF) {
        return currentDocId;
      }
      while (queue.size() > 0 && queue.peek().getA() <= currentDocId) {
        IntPair pair = queue.remove();
        iteratorIsInQueue[pair.getB()] = false;
      }
      currentDocId++;
      for (int i = 0; i < iterators.length; i++) {
        if (!iteratorIsInQueue[i]) {
          while (iterators[i].hasNext()) {
            int next = iterators[i].next();
            if (next >= startDocId && next <= endDocId && next >= currentDocId) {
              queue.add(new IntPair(next, i));
              break;
            }
            if (next > endDocId) {
              break;
            }
          }
          iteratorIsInQueue[i] = true;
        }
      }
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
    public int currentDocId() {
      return currentDocId;
    }
  }
}
