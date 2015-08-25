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
package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;


/**
 * BReusableFilteredDocIdSetOperator will take a filter Operator and get the matched docId set.
 * Internally, cached a given size of docIds, so this Operator could be replicated
 * for many ColumnarReaderDataSource.
 *
 *
 */
public class BReusableFilteredDocIdSetOperator extends BaseOperator {

  private final Operator _filterOperators;
  private final int _docSize;
  private BlockDocIdIterator _currentBlockDocIdIterator;
  private Block _currentBlock;
  private DocIdSetBlock _currentDocIdSetBlock;
  private int _currentDoc = 0;
  private final int _maxSizeOfdocIdSet;
  private final int[] _docIdArray;
  private int _pos = 0;
  private int _searchableDocIdSize = 0;
  boolean inited = false;

  public BReusableFilteredDocIdSetOperator(Operator filterOperators, int docSize, int maxSizeOfdocIdSet) {
    _maxSizeOfdocIdSet = maxSizeOfdocIdSet;
    _docIdArray = new int[_maxSizeOfdocIdSet];
    _filterOperators = filterOperators;
    _docSize = docSize;

  }

  @Override
  public boolean open() {
    _filterOperators.open();
    return true;
  }

  @Override
  public Block getNextBlock() {
    if (_currentDoc == Constants.EOF) {
      return null;
    }
    if (!inited) {
      inited = true;
      _currentDoc = 0;
      if (_filterOperators == null) {
        _currentBlock = new MatchEntireSegmentDocIdSetBlock(_docSize);
        _currentBlockDocIdIterator = _currentBlock.getBlockDocIdSet().iterator();
        return _currentBlock;
      } else {
        _currentBlock = _filterOperators.nextBlock();
      }
      _currentBlockDocIdIterator = _currentBlock.getBlockDocIdSet().iterator();
    }
    if (_filterOperators == null) {
      _currentBlock = null;
      return _currentBlock;
    }
    _pos = 0;
    getNextDoc();
    while (_currentDoc != Constants.EOF) {
      _docIdArray[_pos++] = _currentDoc;
      if (_pos == _maxSizeOfdocIdSet) {
        _searchableDocIdSize = _pos;
        _currentDocIdSetBlock = new DocIdSetBlock(_docIdArray, _pos);
        return _currentDocIdSetBlock;
      }
      getNextDoc();
    }
    if (_pos > 0) {
      _searchableDocIdSize = _pos;
      _currentDocIdSetBlock = new DocIdSetBlock(_docIdArray, _pos);
      return _currentDocIdSetBlock;
    }
    _currentDocIdSetBlock = null;
    return _currentDocIdSetBlock;
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getOperatorName() {
    return "BReusableFilteredDocIdSetOperator";
  }

  public Block getCurrentBlock() {
    return _currentBlock;
  }

  public DocIdSetBlock getCurrentDocIdSetBlock() {
    return _currentDocIdSetBlock;
  }

  private int getNextDoc() {
    if (_currentDoc == Constants.EOF) {
      return _currentDoc;
    }
    while ((_currentBlockDocIdIterator == null) || ((_currentDoc = _currentBlockDocIdIterator.next()) == Constants.EOF)) {
      if (_filterOperators != null) {
        _currentBlock = _filterOperators.nextBlock();
      } else {
        if (_currentDoc == Constants.EOF) {
          _currentBlock = null;
        }
      }
      if (_currentBlock == null) {
        return Constants.EOF;
      }
      _currentBlockDocIdIterator = _currentBlock.getBlockDocIdSet().iterator();
    }
    return _currentDoc;
  }

  @Override
  public boolean close() {
    _filterOperators.close();
    return true;
  }

  public long getCurrentBlockSize() {
    return _searchableDocIdSize;
  }

}
