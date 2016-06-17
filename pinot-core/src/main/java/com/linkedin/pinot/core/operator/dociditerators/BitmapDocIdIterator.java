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

import org.roaringbitmap.IntIterator;

import com.linkedin.pinot.core.common.Constants;

public final class BitmapDocIdIterator implements IndexBasedDocIdIterator {
  final private IntIterator iterator;
  private int endDocId = Integer.MAX_VALUE;
  private int startDocId;
  private int currentDocId = -1;

  public BitmapDocIdIterator(IntIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public int currentDocId() {
    return currentDocId;
  }

  public void setStartDocId(int startDocId) {
    this.startDocId = startDocId;
  }

  public void setEndDocId(int endDocId) {
    this.endDocId = endDocId;
  }

  @Override
  public int next() {
    // Empty?
    if (currentDocId == Constants.EOF || !iterator.hasNext()) {
      currentDocId = Constants.EOF;
      return Constants.EOF;
    }

    currentDocId = iterator.next();

    // Advance to startDocId if necessary
    while(currentDocId < startDocId && iterator.hasNext()) {
      currentDocId = iterator.next();
    }

    // Current docId outside of the valid range?
    if (currentDocId < startDocId || endDocId < currentDocId) {
      currentDocId = Constants.EOF;
    }

    return currentDocId;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId < currentDocId) {
      throw new IllegalArgumentException("Trying to move backwards to docId " + targetDocId +
          ", current position " + currentDocId);
    }

    if (currentDocId == targetDocId) {
      return currentDocId;
    } else {
      int curr = next();
      while(curr < targetDocId && curr != Constants.EOF) {
        curr = next();
      }
      return curr;
    }
  }

  public String toString() {
    return BitmapDocIdIterator.class.getSimpleName();
  }

}
