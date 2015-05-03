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
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.filter.AndOperator;


public final class AndBlockDocIdSet implements BlockDocIdSet {
  /**
   * 
   */
  private final BlockDocIdIterator[] docIdIterators;
  private final int[] docIdPointers;
  private Logger logger = LoggerFactory.getLogger(AndOperator.class);
  boolean reachedEnd = false;
  int currentDocId = Constants.EOF;
  public final AtomicLong timeMeasure = new AtomicLong(0);
  private List<BlockDocIdSet> blockDocIdSets;
  private int minDocId;
  private int maxDocId;

  public AndBlockDocIdSet(List<BlockDocIdSet> blockDocIdSets) {
    this.blockDocIdSets = blockDocIdSets;
    final int[] docIdPointers = new int[blockDocIdSets.size()];
    final BlockDocIdIterator[] docIdIterators = new BlockDocIdIterator[blockDocIdSets.size()];
    for (int srcId = 0; srcId < blockDocIdSets.size(); srcId++) {
      docIdIterators[srcId] = blockDocIdSets.get(srcId).iterator();
    }
    Arrays.fill(docIdPointers, -1);
    this.docIdIterators = docIdIterators;
    this.docIdPointers = docIdPointers;
    minDocId = Integer.MIN_VALUE;
    maxDocId = Integer.MAX_VALUE;
    for (BlockDocIdSet blockDocIdSet : blockDocIdSets) {
      if (blockDocIdSet instanceof SortedDocIdSet) {
        SortedDocIdSet sortedDocIdSet = (SortedDocIdSet) blockDocIdSet;
        minDocId = Math.max(minDocId, sortedDocIdSet.getMinDocId());
        maxDocId = Math.min(maxDocId, sortedDocIdSet.getMaxDocId());
      } else if (blockDocIdSet instanceof AndBlockDocIdSet) {
        AndBlockDocIdSet andBlockDocIdSet = (AndBlockDocIdSet) blockDocIdSet;
        minDocId = Math.max(minDocId, andBlockDocIdSet.getMinDocId());
        maxDocId = Math.min(maxDocId, andBlockDocIdSet.getMaxDocId());
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
      int currentMax = 0;

      @Override
      public int advance(int targetDocId) {
        if (currentDocId == Constants.EOF) {
          return currentDocId;
        }
        long start = System.nanoTime();
        for (int srcId = 0; srcId < docIdIterators.length; srcId++) {
          docIdPointers[srcId] = docIdIterators[srcId].advance(targetDocId);
          if (docIdPointers[srcId] == Constants.EOF) {
            reachedEnd = true;
            break;
          }
          if (docIdPointers[srcId] > currentMax) {
            currentMax = docIdPointers[srcId];
          }
        }
        //find the next matching docId
        findNextMatch();
        long end = System.nanoTime();
        timeMeasure.addAndGet(end - start);
        return currentDocId;
      }

      @Override
      public int next() {
        long start = System.nanoTime();

        //always increment the pointer to current max, when this is called first time, every one will be set to start of posting list.
        for (int i = 0; i < docIdIterators.length; i++) {
          docIdPointers[i] = docIdIterators[i].advance(currentMax);
          if (docIdPointers[i] == Constants.EOF) {
            reachedEnd = true;
            currentDocId = Constants.EOF;
            break;
          }
          if (docIdPointers[i] > currentMax) {
            currentMax = docIdPointers[i];
          }
        }
        //find the next matching docId
        findNextMatch();
        long end = System.nanoTime();
        timeMeasure.addAndGet(end - start);
        if (currentDocId == Constants.EOF) {
          logger.info("AND operator took:" + timeMeasure.get());
        }
        return currentDocId;
      }

      private void findNextMatch() {
        boolean found = false;
        while (!found && !reachedEnd) {
          found = true;
          for (int i = 0; i < docIdIterators.length; i++) {
            if (docIdPointers[i] != currentMax) {
              found = false;
              docIdPointers[i] = docIdIterators[i].advance(currentMax);
              if (docIdPointers[i] == Constants.EOF) {
                reachedEnd = true;
                currentDocId = Constants.EOF;
                //update the max docId matched here
                maxDocId = currentDocId;
                break;
              }
              if (docIdPointers[i] > currentMax) {
                currentMax = docIdPointers[i];
                break;
              } else if (docIdPointers[i] != currentMax) {
                throw new RuntimeException("DocIdIterator returning invalid result:" + docIdPointers[i]
                    + " after invoking skipTo:" + currentMax);
              }
            }
          }
          if (found) {
            currentDocId = currentMax;
            currentMax = currentMax + 1;
          }
        }
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

  public int getMinDocId() {
    return minDocId;
  }

  public int getMaxDocId() {
    // TODO Auto-generated method stub
    return maxDocId;
  }

}
