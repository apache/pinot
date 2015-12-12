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

import java.util.List;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;

public final class CompositeDocIdIterator implements BlockDocIdIterator {
  List<BlockDocIdIterator> docIdIterators;
  int blockIdx = 0;
  BlockDocIdIterator itr = null;

  /**
   * @param compositeFilterBlockDocIdSet
   */
  public CompositeDocIdIterator(List<BlockDocIdIterator> docIdIterators) {
    this.docIdIterators = docIdIterators;
  }


  @Override
  public int currentDocId() {
    checkIterator();
    return itr.currentDocId();
  }

  @Override
  public int next() {
    checkIterator();
    int next = itr.next();
    while (next == Constants.EOF && blockIdx < docIdIterators.size()) {
      checkIterator();
      next = itr.next();
    }
    return next;
  }

  @Override
  public int advance(int targetDocId) {
    do {
      // We loop here because we may be trying to advance to a target doc ID pas the current
      // sub-segment
      checkIterator();
      itr.advance(targetDocId);
    } while (itr.currentDocId() == Constants.EOF && blockIdx < docIdIterators.size());
    return next();
  }

  void checkIterator() {
    if (itr == null) {
      itr = docIdIterators.get(blockIdx++);
    }

    while (itr.currentDocId() == Constants.EOF && blockIdx < docIdIterators.size()) {
      itr = docIdIterators.get(blockIdx++);
    }
  }
}