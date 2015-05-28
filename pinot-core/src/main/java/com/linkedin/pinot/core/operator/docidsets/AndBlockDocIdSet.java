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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.filter.AndOperator;


public final class AndBlockDocIdSet implements FilterBlockDocIdSet {
  /**
   * 
   */
  private final BlockDocIdIterator[] docIdIterators;
  private final int[] docIdPointers;
  private static final Logger LOGGER = LoggerFactory.getLogger(AndOperator.class);
  boolean reachedEnd = false;
  int currentDocId = -1;
  public final AtomicLong timeMeasure = new AtomicLong(0);
  private List<FilterBlockDocIdSet> blockDocIdSets;
  private int minDocId = Integer.MIN_VALUE;
  private int maxDocId = Integer.MAX_VALUE;

  public AndBlockDocIdSet(List<FilterBlockDocIdSet> blockDocIdSets) {
    this.blockDocIdSets = blockDocIdSets;
    final int[] docIdPointers = new int[blockDocIdSets.size()];
    final BlockDocIdIterator[] docIdIterators = new BlockDocIdIterator[blockDocIdSets.size()];
    for (int srcId = 0; srcId < blockDocIdSets.size(); srcId++) {
      docIdIterators[srcId] = blockDocIdSets.get(srcId).iterator();
    }
    Arrays.fill(docIdPointers, -1);
    this.docIdIterators = docIdIterators;
    this.docIdPointers = docIdPointers;
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
    return new BlockDocIdIterator() {
      int currentMax = -1;

      @Override
      public int advance(int targetDocId) {
        if (currentDocId == Constants.EOF) {
          return currentDocId;
        }
        if (currentDocId >= targetDocId) {
          return currentDocId;
        }
        // next() method will always increment currentMax by 1.
        currentMax = targetDocId - 1;
        return next();
      }

      @Override
      public int next() {
        long start = System.currentTimeMillis();
        if (currentDocId == Constants.EOF) {
          return currentDocId;
        }
        currentMax = currentMax + 1;
        //always increment the pointer to current max, when this is called first time, every one will be set to start of posting list.
        for (int i = 0; i < docIdIterators.length; i++) {
          docIdPointers[i] = docIdIterators[i].advance(currentMax);
          if (docIdPointers[i] == Constants.EOF) {
            reachedEnd = true;
            currentMax = Constants.EOF;
            break;
          }
          if (docIdPointers[i] > currentMax) {
            currentMax = docIdPointers[i];
            if (i > 0) {
              i = -1;
            }
          } else if (docIdPointers[i] < currentMax) {
            LOGGER.warn("Should never happen, {} returns docIdPointer : {} should always >= currentMax : {}", docIdIterators[i], docIdPointers[i], currentMax);
            throw new IllegalStateException("Should never happen, docIdPointer should always >= currentMax");
          }
        }
        currentDocId = currentMax;
        long end = System.currentTimeMillis();
        timeMeasure.addAndGet(end - start);
        //Remove this after tracing is added
//      if (currentDocId == Constants.EOF) {
//        LOGGER.debug("AND operator took:" + timeMeasure.get());
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
    return (T) this.blockDocIdSets;
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
