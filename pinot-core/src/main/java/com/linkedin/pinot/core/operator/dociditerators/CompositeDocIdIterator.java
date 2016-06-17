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

import java.util.List;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;

public final class CompositeDocIdIterator implements BlockDocIdIterator {
  List<BlockDocIdIterator> docIdIterators;
  BlockDocIdIterator currentItr = null;
  int blockIdx = 0;
  int currentDocId = -1;

  /**
   * @param docIdIterators
   */
  public CompositeDocIdIterator(List<BlockDocIdIterator> docIdIterators) {
    this.docIdIterators = docIdIterators;
    if (docIdIterators.isEmpty()) {
      currentDocId = Constants.EOF;
    }
  }


  @Override
  public int currentDocId() {
    return currentDocId;
  }

  @Override
  public int next() {
    // Advance until we find a valid docId, or exhaust all iterators.
    while (((currentItr == null) || ((currentDocId = currentItr.next()) == Constants.EOF)) &&
        blockIdx < docIdIterators.size()) {
      currentItr = docIdIterators.get(blockIdx++);
    }

    return currentDocId;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId < currentDocId) {
      throw new IllegalArgumentException("Trying to move backwards to docId " + targetDocId +
          ", current position " + currentDocId);
    }

    // Advance until we hit the targetDocId, or exhaust.
    while (currentDocId < targetDocId && (next() != Constants.EOF));
    return currentDocId;
  }
}