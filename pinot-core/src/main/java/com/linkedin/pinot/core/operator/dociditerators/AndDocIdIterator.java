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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;

public final class AndDocIdIterator implements BlockDocIdIterator {

  static final Logger LOGGER = LoggerFactory.getLogger(AndDocIdIterator.class);
  public final BlockDocIdIterator[] docIdIterators;
  public final int[] docIdPointers;
  public boolean reachedEnd = false;
  public int currentDocId = -1;
  int currentMax = -1;
  private AtomicLong timeMeasure = new AtomicLong();
  private AtomicLong[] timeMeasures;
  
  /**
   * @param andBlockDocIdSet
   */
  public AndDocIdIterator(BlockDocIdIterator[] docIdIterators) {
    this.docIdIterators = docIdIterators;
    this.docIdPointers = new int[docIdIterators.length];
    Arrays.fill(docIdPointers, -1);
    timeMeasures = new AtomicLong[docIdIterators.length];
    for (int i = 0; i < docIdIterators.length; i++) {
      timeMeasures[i] = new AtomicLong(0);
    }
  }

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
    // always increment the pointer to current max, when this is called first time, every one will
    // be set to start of posting list.
    long childStart,childEnd;//to measure time spent in child iterators
    for (int i = 0; i < docIdIterators.length; i++) {
      childStart = System.currentTimeMillis();
      docIdPointers[i] = docIdIterators[i].advance(currentMax);
      childEnd = System.currentTimeMillis();
      timeMeasures[i].addAndGet((childEnd - childStart));
      if (docIdPointers[i] == Constants.EOF) {
        reachedEnd = true;
        currentMax = Constants.EOF;
        break;
      }
      if (docIdPointers[i] > currentMax) {
        currentMax = docIdPointers[i];
        if (i > 0) {
          // we need to advance all pointer since we found a new max
          i = -1;
        }
      } else if (docIdPointers[i] < currentMax) {
        LOGGER.warn(
            "Should never happen, {} returns docIdPointer : {} should always >= currentMax : {}",
            docIdIterators[i], docIdPointers[i], currentMax);
        throw new IllegalStateException(
            "Should never happen, docIdPointer should always >= currentMax");
      }
    }
    currentDocId = currentMax;
    long end = System.currentTimeMillis();
    timeMeasure.addAndGet(end - start);
    // Remove this after tracing is added
    if (currentDocId == Constants.EOF) {
      if(LOGGER.isDebugEnabled()){
        LOGGER.debug("AND operator took:{}. Break down by child iterators:{} times:{}", timeMeasure.get(), Arrays.toString(docIdIterators), Arrays.toString(timeMeasures));
      }
    }
    return currentDocId;
  }

  @Override
  public int currentDocId() {
    return currentDocId;
  }
  
  public String toString(){
    return Arrays.toString(docIdIterators);
  }
}
