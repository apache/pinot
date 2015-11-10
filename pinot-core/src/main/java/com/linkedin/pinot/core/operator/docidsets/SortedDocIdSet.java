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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.segment.index.block.BlockUtils;


public class SortedDocIdSet implements FilterBlockDocIdSet {

  private final List<Pair<Integer, Integer>> pairs;
  public final AtomicLong timeMeasure = new AtomicLong(0);
  int startDocId;
  int endDocId;

  public SortedDocIdSet(List<Pair<Integer, Integer>> pairs) {
    this.pairs = pairs;
  }

  @Override
  public int getMinDocId() {
    if (pairs.size() > 0) {
      return pairs.get(0).getLeft();
    } else {
      return 0;
    }
  }

  @Override
  public int getMaxDocId() {
    if (pairs.size() > 0) {
      return pairs.get(pairs.size() - 1).getRight();
    } else {
      return 0;
    }
  }

  /**
   * After setting the startDocId, next calls will always return from &gt;=startDocId
   * @param startDocId
   */
  @Override
  public void setStartDocId(int startDocId) {
    this.startDocId = startDocId;
  }

  /**
   * After setting the endDocId, next call will return Constants.EOF after currentDocId exceeds endDocId
   * @param endDocId
   */
  @Override
  public void setEndDocId(int endDocId) {
    this.endDocId = endDocId;
  }

  @Override
  public BlockDocIdIterator iterator() {
    if (pairs == null || pairs.isEmpty()) {
      return BlockUtils.emptyBlockDocIdSetIterator();
    }
    return new BlockDocIdIterator() {
      int pairPointer = 0;
      int currentDocId = -1;

      @Override
      public int advance(int targetDocId) {
        if (pairPointer == pairs.size() || targetDocId > pairs.get(pairs.size() - 1).getRight()) {
          pairPointer = pairs.size();
          return (currentDocId = Constants.EOF);
        }
        long start = System.nanoTime();

        if (currentDocId >= targetDocId) {
          return currentDocId;
        }
        // couter < targetDocId
        while (pairPointer < pairs.size()) {
          if (pairs.get(pairPointer).getLeft() > targetDocId) {
            // targetDocId in the gap between two valid pairs.
            currentDocId = pairs.get(pairPointer).getLeft();
            break;
          } else if (targetDocId >= pairs.get(pairPointer).getLeft() && targetDocId <= pairs.get(pairPointer).getRight()) {
            // targetDocId in the future valid pair.
            currentDocId = targetDocId;
            break;
          }
          pairPointer++;
        }
        if (pairPointer == pairs.size()) {
          currentDocId = Constants.EOF;
        }
        long end = System.nanoTime();
        timeMeasure.addAndGet(end - start);
        return currentDocId;
      }

      @Override
      public int next() {
        if (pairPointer == pairs.size() || currentDocId > pairs.get(pairs.size() - 1).getRight()) {
          pairPointer = pairs.size();
          return (currentDocId = Constants.EOF);
        }
        long start = System.nanoTime();
        currentDocId = currentDocId + 1;
        if (pairPointer < pairs.size() && currentDocId > pairs.get(pairPointer).getRight()) {
          pairPointer++;
          if (pairPointer == pairs.size()) {
            currentDocId = Constants.EOF;
          } else {
            currentDocId = pairs.get(pairPointer).getLeft();
          }
        } else if (currentDocId < pairs.get(pairPointer).getLeft()) {
          currentDocId = pairs.get(pairPointer).getLeft();
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
    return (T) pairs;
  }

  @Override
  public String toString() {
    return pairs.toString();
  }
}
