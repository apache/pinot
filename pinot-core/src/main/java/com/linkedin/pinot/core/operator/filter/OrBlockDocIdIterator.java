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


public class OrBlockDocIdIterator implements BlockDocIdIterator {

  private final BlockDocIdIterator[] _blockDocIdIterators;
  private int _currentDocId = -1;
  private int _size = 0;

  public OrBlockDocIdIterator(BlockDocIdSet[] blockDocIdSets) {
    _blockDocIdIterators = new BlockDocIdIterator[blockDocIdSets.length];
    for (int i = 0; i < blockDocIdSets.length; ++i) {
      BlockDocIdSet blockDocIdSet = blockDocIdSets[i];
      if (blockDocIdSet != null) {
        _blockDocIdIterators[i] = blockDocIdSet.iterator();
        _size++;
      }
    }
    if (_size == 0) {
      _currentDocId = Constants.EOF;
    }
  }

  @Override
  public int skipTo(int target) {
    if (_currentDocId == Constants.EOF) {
      return Constants.EOF;
    }

    if (target <= _currentDocId) {
      target = _currentDocId + 1;
    }

    while (true) {
      BlockDocIdIterator topIter = _blockDocIdIterators[0];
      if ((topIter.skipTo(target)) != Constants.EOF) {
        adjustIterators();
      }
      else {
        removeFirst();
        if (_size == 0) {
          return (_currentDocId = Constants.EOF);
        }
      }
      int topDoc = _blockDocIdIterators[0].currentDocId();
      if (topDoc >= target) {
        return (_currentDocId = topDoc);
      }
    }
  }

  @Override
  public int next() {
    if (_currentDocId == Constants.EOF) {
      return Constants.EOF;
    }
    while (true) {
      BlockDocIdIterator topIter = _blockDocIdIterators[0];
      if ((topIter.next()) != Constants.EOF) {
        adjustIterators();
      } else {
        removeFirst();
        if (_size == 0)
          return (_currentDocId = Constants.EOF);
      }
      int nextDocId = _blockDocIdIterators[0].currentDocId();
      if (nextDocId > _currentDocId) {
        return (_currentDocId = nextDocId);
      }
    }
  }

  @Override
  public int currentDocId() {
    return _currentDocId;
  }

  private void adjustIterators() {
    final BlockDocIdIterator topIter = _blockDocIdIterators[0];
    final int doc = topIter.currentDocId();
    int i = 0;

    while (true) {
      int lchild = (i << 1) + 1;
      if (lchild >= _size) {
        break;
      }

      BlockDocIdIterator left = _blockDocIdIterators[lchild];
      int ldoc = left.currentDocId();

      int rchild = lchild + 1;
      if (rchild < _size) {
        BlockDocIdIterator right = _blockDocIdIterators[rchild];
        int rdoc = right.currentDocId();

        if (rdoc <= ldoc) {
          if (doc <= rdoc) {
            break;
          }
          _blockDocIdIterators[i] = right;
          i = rchild;
          continue;
        }
      }
      if (doc <= ldoc) {
        break;
      }
      _blockDocIdIterators[i] = left;
      i = lchild;
    }
    _blockDocIdIterators[i] = topIter;
  }

  private void removeFirst() {
    _size--;
    if (_size > 0) {
      BlockDocIdIterator tmp = _blockDocIdIterators[0];
      _blockDocIdIterators[0] = _blockDocIdIterators[_size];
      _blockDocIdIterators[_size] = tmp; // keep the finished iterator at the end for debugging 
      adjustIterators();
    }
  }
}
