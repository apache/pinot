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
package com.linkedin.pinot.core.operator.filter;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.Constants;


public class AndBlockDocIdIterator implements BlockDocIdIterator {

  private final BlockDocIdIterator[] _blockDocIdIterators;
  private int _currentDocId = -1;

  public AndBlockDocIdIterator(BlockDocIdSet[] blockDocIdSets) {
    _blockDocIdIterators = new BlockDocIdIterator[blockDocIdSets.length];
    for (int i = 0; i < blockDocIdSets.length; ++i) {
      BlockDocIdSet blockDocIdSet = blockDocIdSets[i];
      if (blockDocIdSet != null) {
        _blockDocIdIterators[i] = blockDocIdSet.iterator();
      }
    }
  }

  @Override
  public int skipTo(int targetDocId) {
    if (targetDocId < _currentDocId) {
      throw new UnsupportedOperationException("Cannot set back the docId backward");
    }
    _currentDocId = targetDocId - 1;
    return next();
  }

  @Override
  public int next() {
    _currentDocId = _blockDocIdIterators[0].skipTo(_currentDocId + 1);
    while (_currentDocId != Constants.EOF) {
      boolean passed = true;
      for (int i = 1; i < _blockDocIdIterators.length; ++i) {
        int nextDoc = _blockDocIdIterators[i].skipTo(_currentDocId);
        if (nextDoc == Constants.EOF) {
          _currentDocId = Constants.EOF;
          return _currentDocId;
        }
        if (nextDoc > _currentDocId) {
          passed = false;
          _currentDocId = _blockDocIdIterators[0].skipTo(nextDoc);
          break;
        }
      }
      if (passed) {
        break;
      }
    }
    return _currentDocId;
  }

  @Override
  public int currentDocId() {
    return _currentDocId;
  }
}
