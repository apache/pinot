/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.dociditerators;

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;


/**
 * The {@code AndDocIdIterator} is the iterator for AndDocIdSet to perform AND on all child BlockDocIdIterators.
 * <p>It keeps calling {@link BlockDocIdIterator#advance(int)} to gather the common matching document ids from all child
 * BlockDocIdIterators until one of them hits the end.
 */
public final class AndDocIdIterator implements BlockDocIdIterator {
  public final BlockDocIdIterator[] _docIdIterators;

  private int _nextDocId = 0;

  public AndDocIdIterator(BlockDocIdIterator[] docIdIterators) {
    _docIdIterators = docIdIterators;
  }

  @Override
  public int next() {
    int maxDocId = _nextDocId;
    int maxDocIdIndex = -1;
    int numDocIdIterators = _docIdIterators.length;
    int index = 0;
    while (index < numDocIdIterators) {
      if (index == maxDocIdIndex) {
        // Skip the index with the max document id
        index++;
        continue;
      }
      int docId = _docIdIterators[index].advance(maxDocId);
      if (docId != Constants.EOF) {
        if (docId == maxDocId) {
          index++;
        } else {
          // The current iterator does not contain the maxDocId, update maxDocId and advance all other iterators
          maxDocId = docId;
          maxDocIdIndex = index;
          index = 0;
        }
      } else {
        closeIterators();
        return Constants.EOF;
      }
    }
    _nextDocId = maxDocId;
    return _nextDocId++;
  }

  @Override
  public int advance(int targetDocId) {
    _nextDocId = targetDocId;
    return next();
  }

  private void closeIterators() {
    for (BlockDocIdIterator it : _docIdIterators) {
      if (it instanceof ScanBasedDocIdIterator) {
        ((ScanBasedDocIdIterator) it).close();
      }
    }
  }
}
