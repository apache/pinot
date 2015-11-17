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

import java.util.concurrent.atomic.AtomicLong;

import org.roaringbitmap.IntIterator;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;

public final class BitmapDocIdIterator implements BlockDocIdIterator {
  final private IntIterator iterator;
  private int endDocId;
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
    int next = Constants.EOF;
    boolean found = false;
    while (iterator.hasNext()) {
      next = iterator.next();
      if (next > endDocId) {
        break;
      }
      if (next >= startDocId) {
        found = true;
        break;
      }
    }
    if (found) {
      currentDocId = next;
    } else {
      currentDocId = Constants.EOF;
    }
    return currentDocId;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId < startDocId) {
      targetDocId = startDocId;
    } else if (targetDocId > endDocId) {
      currentDocId = Constants.EOF;
    }
    if (currentDocId == Constants.EOF) {
      return currentDocId;
    }
    if (currentDocId == targetDocId) {
      return currentDocId;
    }
    boolean found = false;
    int next = Constants.EOF;
    while (iterator.hasNext()) {
      next = iterator.next();
      if (next > endDocId) {
        break;
      }
      if (next >= targetDocId) {
        found = true;
        break;
      }
    }
    if (found) {
      currentDocId = next;
    } else {
      currentDocId = Constants.EOF;
    }
    long end = System.nanoTime();
    return currentDocId;
  }
}