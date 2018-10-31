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


public final class ArrayBasedDocIdIterator implements BlockDocIdIterator {
  private final int[] _docIds;
  private final int _searchableLength;

  private int _currentIndex = -1;
  private int _currentDocId = -1;

  public ArrayBasedDocIdIterator(int[] docIds, int searchableLength) {
    _docIds = docIds;
    _searchableLength = searchableLength;
  }

  @Override
  public int next() {
    if (_currentDocId == Constants.EOF) {
      return Constants.EOF;
    }
    if (++_currentIndex == _searchableLength) {
      _currentDocId = Constants.EOF;
    } else {
      _currentDocId = _docIds[_currentIndex];
    }
    return _currentDocId;
  }

  @Override
  public int advance(int targetDocId) {
    if (_currentDocId == Constants.EOF) {
      return Constants.EOF;
    }
    if (targetDocId <= _currentDocId) {
      return _currentDocId;
    }
    while (++_currentIndex < _searchableLength) {
      if (_docIds[_currentIndex] >= targetDocId) {
        _currentDocId = _docIds[_currentIndex];
        return _currentDocId;
      }
    }
    _currentDocId = Constants.EOF;
    return Constants.EOF;
  }

  @Override
  public int currentDocId() {
    return _currentDocId;
  }
}
