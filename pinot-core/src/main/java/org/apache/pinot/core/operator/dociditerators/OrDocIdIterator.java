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

import java.util.Arrays;
import org.apache.pinot.query.spi.data.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;


/**
 * The {@code OrDocIdIterator} is the iterator for OrDocIdSet to perform OR on all child BlockDocIdIterators.
 */
public final class OrDocIdIterator implements BlockDocIdIterator {
  private final BlockDocIdIterator[] _docIdIterators;
  private final int[] _nextDocIds;

  private int _numNotExhaustedIterators;
  private int _previousDocId = -1;

  public OrDocIdIterator(BlockDocIdIterator[] docIdIterators) {
    _docIdIterators = docIdIterators;
    int numDocIdIterators = docIdIterators.length;
    _nextDocIds = new int[numDocIdIterators];
    Arrays.fill(_nextDocIds, -1);
    _numNotExhaustedIterators = numDocIdIterators;
  }

  /**
   * Loop over the document id array and pick the smallest one so that the document ids returned are in ascending order.
   * <p>For each child iterator, fetch the next document id when its current document id is the same as document id
   * previous returned so that each document id is only returned once.
   * <p>{@inheritDoc}
   */
  @Override
  public int next() {
    int nextDocId = Integer.MAX_VALUE;
    boolean hasExhaustedIterator = false;
    for (int i = 0; i < _numNotExhaustedIterators; i++) {
      int docId = _nextDocIds[i];
      if (docId == _previousDocId) {
        docId = _docIdIterators[i].next();
        _nextDocIds[i] = docId;
        if (docId == Constants.EOF) {
          hasExhaustedIterator = true;
          continue;
        }
      }
      nextDocId = Math.min(nextDocId, docId);
    }
    if (hasExhaustedIterator) {
      removeExhaustedIterators();
    }
    if (nextDocId != Integer.MAX_VALUE) {
      _previousDocId = nextDocId;
      return nextDocId;
    } else {
      return Constants.EOF;
    }
  }

  /**
   * Loop over the document id array and pick the smallest one that is equal or grater than the given target document id
   * so that the document ids returned are in ascending order.
   * <p>For each child iterator, advance to the target document id when its current document id is smaller than the
   * target document id so that each document id is only returned once.
   * <p>{@inheritDoc}
   */
  @Override
  public int advance(int targetDocId) {
    int nextDocId = Integer.MAX_VALUE;
    boolean hasExhaustedIterator = false;
    for (int i = 0; i < _numNotExhaustedIterators; i++) {
      int docId = _nextDocIds[i];
      if (docId < targetDocId) {
        docId = _docIdIterators[i].advance(targetDocId);
        _nextDocIds[i] = docId;
        if (docId == Constants.EOF) {
          hasExhaustedIterator = true;
          continue;
        }
      }
      nextDocId = Math.min(nextDocId, docId);
    }
    if (hasExhaustedIterator) {
      removeExhaustedIterators();
    }
    if (nextDocId != Integer.MAX_VALUE) {
      _previousDocId = nextDocId;
      return nextDocId;
    } else {
      return Constants.EOF;
    }
  }

  /**
   * Helper method to remove exhausted iterators.
   * <p>Whenever we find an exhausted iterator, we replace it with the last one, and update number of not exhausted
   * iterators until the first number of not exhausted iterators in the array are not exhausted.
   */
  private void removeExhaustedIterators() {
    int i = 0;
    while (i < _numNotExhaustedIterators) {
      if (_nextDocIds[i] == Constants.EOF) {
        // Need to check the new iterator moved to this index, so do not increase the index
        _numNotExhaustedIterators--;
        _docIdIterators[i] = _docIdIterators[_numNotExhaustedIterators];
        _nextDocIds[i] = _nextDocIds[_numNotExhaustedIterators];
      } else {
        i++;
      }
    }
  }
}
