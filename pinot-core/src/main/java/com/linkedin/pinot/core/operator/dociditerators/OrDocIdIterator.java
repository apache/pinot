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

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;


public final class OrDocIdIterator implements BlockDocIdIterator {
  private final BlockDocIdIterator[] _docIdIterators;
  private final int[] _nextDocIds;
  private final int _minDocId;
  private final int _maxDocId;

  private int _numNotExhaustedIterators;
  private int _currentDocId = -1;

  public OrDocIdIterator(BlockDocIdIterator[] docIdIterators, int minDocId, int maxDocId) {
    _docIdIterators = docIdIterators;
    _numNotExhaustedIterators = docIdIterators.length;
    _nextDocIds = new int[_numNotExhaustedIterators];
    for (int i = 0; i < _numNotExhaustedIterators; i++) {
      _nextDocIds[i] = docIdIterators[i].advance(minDocId);
    }
    _minDocId = minDocId;
    _maxDocId = maxDocId;
    removeExhaustedIterators();
  }

  /**
   * Loop over the document id array and pick the smallest one so that the document ids returned are in ascending order.
   * <p>For each child iterator, fetch the next document id when its current document id is the same as the current
   * document id of parent iterator so that each document id is only returned once.
   * <p>{@inheritDoc}
   */
  @Override
  public int next() {
    if (_currentDocId == Constants.EOF) {
      return Constants.EOF;
    }

    int nextDocId = Integer.MAX_VALUE;
    boolean hasExhaustedIterator = false;
    for (int i = 0; i < _numNotExhaustedIterators; i++) {
      int docId = _nextDocIds[i];
      if (docId == _currentDocId) {
        docId = _docIdIterators[i].next();
        _nextDocIds[i] = docId;
      }
      if (docId != Constants.EOF) {
        nextDocId = Math.min(nextDocId, docId);
      } else {
        hasExhaustedIterator = true;
      }
    }
    if (nextDocId > _maxDocId) {
      _currentDocId = Constants.EOF;
    } else {
      _currentDocId = nextDocId;
      if (hasExhaustedIterator) {
        removeExhaustedIterators();
      }
    }
    return _currentDocId;
  }

  /**
   * Loop over the document id array and pick the smallest one that is equal or grater than the given target document id
   * so that the document ids returned are in ascending order.
   * <p>For each child iterator, advance to the target document id when its current document id is smaller than the
   * target document id  so that each document id is only returned once.
   * <p>{@inheritDoc}
   */
  @Override
  public int advance(int targetDocId) {
    if (_currentDocId == Constants.EOF) {
      return Constants.EOF;
    }
    if (targetDocId > _maxDocId) {
      _currentDocId = Constants.EOF;
      return Constants.EOF;
    }
    if (targetDocId <= _currentDocId) {
      return _currentDocId;
    }

    if (targetDocId < _minDocId) {
      targetDocId = _minDocId;
    }

    int nextDocId = Integer.MAX_VALUE;
    boolean hasExhaustedIterator = false;
    for (int i = 0; i < _numNotExhaustedIterators; i++) {
      int docId = _nextDocIds[i];
      if (docId < targetDocId) {
        docId = _docIdIterators[i].advance(targetDocId);
        _nextDocIds[i] = docId;
      }
      if (docId != Constants.EOF) {
        nextDocId = Math.min(nextDocId, docId);
      } else {
        hasExhaustedIterator = true;
      }
    }
    if (nextDocId > _maxDocId) {
      _currentDocId = Constants.EOF;
    } else {
      _currentDocId = nextDocId;
      if (hasExhaustedIterator) {
        removeExhaustedIterators();
      }
    }
    return _currentDocId;
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

  @Override
  public int currentDocId() {
    return _currentDocId;
  }
}
