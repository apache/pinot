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
package com.linkedin.pinot.core.operator.dociditerators;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.docidsets.SortedDocIdSet;

public final class SortedDocIdIterator implements BlockDocIdIterator {
  /**
   * 
   */
  private final SortedDocIdSet sortedDocIdSet;

  /**
   * @param sortedDocIdSet
   */
  public SortedDocIdIterator(SortedDocIdSet sortedDocIdSet) {
    this.sortedDocIdSet = sortedDocIdSet;
  }

  int pairPointer = 0;
  int currentDocId = -1;

  @Override
  public int advance(int targetDocId) {
    if (pairPointer == this.sortedDocIdSet.pairs.size() || targetDocId > this.sortedDocIdSet.pairs.get(this.sortedDocIdSet.pairs.size() - 1).getRight()) {
      pairPointer = this.sortedDocIdSet.pairs.size();
      return (currentDocId = Constants.EOF);
    }
    long start = System.nanoTime();

    if (currentDocId >= targetDocId) {
      return currentDocId;
    }
    // couter < targetDocId
    while (pairPointer < this.sortedDocIdSet.pairs.size()) {
      if (this.sortedDocIdSet.pairs.get(pairPointer).getLeft() > targetDocId) {
        // targetDocId in the gap between two valid pairs.
        currentDocId = this.sortedDocIdSet.pairs.get(pairPointer).getLeft();
        break;
      } else if (targetDocId >= this.sortedDocIdSet.pairs.get(pairPointer).getLeft() && targetDocId <= this.sortedDocIdSet.pairs.get(pairPointer).getRight()) {
        // targetDocId in the future valid pair.
        currentDocId = targetDocId;
        break;
      }
      pairPointer++;
    }
    if (pairPointer == this.sortedDocIdSet.pairs.size()) {
      currentDocId = Constants.EOF;
    }
    long end = System.nanoTime();
    this.sortedDocIdSet.timeMeasure.addAndGet(end - start);
    return currentDocId;
  }

  @Override
  public int next() {
    if (pairPointer == this.sortedDocIdSet.pairs.size() || currentDocId > this.sortedDocIdSet.pairs.get(this.sortedDocIdSet.pairs.size() - 1).getRight()) {
      pairPointer = this.sortedDocIdSet.pairs.size();
      return (currentDocId = Constants.EOF);
    }
    long start = System.nanoTime();
    currentDocId = currentDocId + 1;
    if (pairPointer < this.sortedDocIdSet.pairs.size() && currentDocId > this.sortedDocIdSet.pairs.get(pairPointer).getRight()) {
      pairPointer++;
      if (pairPointer == this.sortedDocIdSet.pairs.size()) {
        currentDocId = Constants.EOF;
      } else {
        currentDocId = this.sortedDocIdSet.pairs.get(pairPointer).getLeft();
      }
    } else if (currentDocId < this.sortedDocIdSet.pairs.get(pairPointer).getLeft()) {
      currentDocId = this.sortedDocIdSet.pairs.get(pairPointer).getLeft();
    }
    long end = System.nanoTime();
    this.sortedDocIdSet.timeMeasure.addAndGet(end - start);
    return currentDocId;
  }

  @Override
  public int currentDocId() {
    return currentDocId;
  }
}