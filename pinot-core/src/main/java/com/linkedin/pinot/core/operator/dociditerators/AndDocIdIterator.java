/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
  public ScanBasedDocIdIterator[] scanBasedDocIdIterators;
  public final int[] docIdPointers;
  public boolean reachedEnd = false;
  public int currentDocId = -1;
  int currentMax = -1;
  private boolean hasScanBasedIterators;

  public AndDocIdIterator(BlockDocIdIterator[] blockDocIdIterators) {
    int numIndexBasedIterators = 0;
    int numScanBasedIterators = 0;
    for (int i = 0; i < blockDocIdIterators.length; i++) {
      if (blockDocIdIterators[i] instanceof IndexBasedDocIdIterator) {
        numIndexBasedIterators = numIndexBasedIterators + 1;
      } else if (blockDocIdIterators[i] instanceof ScanBasedDocIdIterator) {
        numScanBasedIterators = numScanBasedIterators + 1;
      }
    }
    // if we have at least one index based then do intersection based on index based only, and then
    // check if matching docs apply on scan based iterator
    if (numIndexBasedIterators > 0 && numScanBasedIterators > 0) {
      hasScanBasedIterators = true;
      int nonScanIteratorsSize = blockDocIdIterators.length - numScanBasedIterators;
      this.docIdIterators = new BlockDocIdIterator[nonScanIteratorsSize];
      this.scanBasedDocIdIterators = new ScanBasedDocIdIterator[numScanBasedIterators];
      int nonScanBasedIndex = 0;
      int scanBasedIndex = 0;
      for (int i = 0; i < blockDocIdIterators.length; i++) {
        if (blockDocIdIterators[i] instanceof ScanBasedDocIdIterator) {
          this.scanBasedDocIdIterators[scanBasedIndex++] =
              (ScanBasedDocIdIterator) blockDocIdIterators[i];
        } else {
          this.docIdIterators[nonScanBasedIndex++] = blockDocIdIterators[i];
        }
      }
    } else {
      hasScanBasedIterators = false;
      this.docIdIterators = blockDocIdIterators;
    }
    this.docIdPointers = new int[docIdIterators.length];
    Arrays.fill(docIdPointers, -1);
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
    if (currentDocId == Constants.EOF) {
      return currentDocId;
    }
    currentMax = currentMax + 1;
    // always increment the pointer to current max, when this is called first time, every one will
    // be set to start of posting list.
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
          // we need to advance all pointer since we found a new max
          i = -1;
        }
      }
      if (hasScanBasedIterators  && i == docIdIterators.length - 1  ) {
        // this means we found the docId common to all nonScanBased iterators, now we need to ensure
        // that its also found in scanBasedIterator, if not matched, we restart the intersection
        for (ScanBasedDocIdIterator iterator : scanBasedDocIdIterators) {
          if (!iterator.isMatch(currentMax)) {
            i = -1;
            currentMax = currentMax + 1;
            break;
          }
        }
      }
    }
    currentDocId = currentMax;
    return currentDocId;
  }

  @Override
  public int currentDocId() {
    return currentDocId;
  }

  public String toString() {
    return Arrays.toString(docIdIterators);
  }
}
