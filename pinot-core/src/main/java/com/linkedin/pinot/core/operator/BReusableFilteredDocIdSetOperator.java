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
import com.linkedin.pinot.core.operator.docidsets.DocIdSetBlock;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;

/**
 * BReusableFilteredDocIdSetOperator will take a filter Operator and get the matched docId set.
 * Internally, cached a given size of docIds, so this Operator could be replicated
 * for many ColumnarReaderDataSource.
 */
public class BReusableFilteredDocIdSetOperator extends BaseOperator {

  private final Operator _filterOperator;
  private BlockDocIdIterator _currentBlockDocIdIterator;
  private Block _currentBlock;
  private int _currentDoc = 0;
  private final int _maxSizeOfdocIdSet;
  boolean inited = false;
  private static final ThreadLocal<int[]> DOC_ID_ARRAY = new ThreadLocal<int[]>() {
    @Override
    protected int[] initialValue() {
      return new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
  };

  /**
   * @param filterOperator
   * @param docSize
   * @param maxSizeOfdocIdSet must be less than {@link DocIdSetPlanNode}. MAX_DOC_PER_CALL which is
   *          10000
   */
  public BReusableFilteredDocIdSetOperator(Operator filterOperator, int docSize,
      int maxSizeOfdocIdSet) {
    _maxSizeOfdocIdSet = Math.min(maxSizeOfdocIdSet, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _filterOperator = filterOperator;
  }

  @Override
  public boolean open() {
    _filterOperator.open();
    return true;
  }

  @Override
  public Block getNextBlock() {
    // Handle limit 0 clause safely.
    // For limit 0, _docIdArray will be zero sized
    if (_currentDoc == Constants.EOF) {
      return null;
    }
    int[] docIdArray = DOC_ID_ARRAY.get();
    if (!inited) {
      inited = true;
      _currentDoc = 0;
      _currentBlock = _filterOperator.nextBlock();
      _currentBlockDocIdIterator = _currentBlock.getBlockDocIdSet().iterator();
    }
    int pos = 0;
    for (int i = 0; i < _maxSizeOfdocIdSet; i++) {
      _currentDoc = _currentBlockDocIdIterator.next();
      if (_currentDoc == Constants.EOF) {
        break;
      }
      docIdArray[pos++] = _currentDoc;
    }
    DocIdSetBlock docIdSetBlock = null;
    if (pos > 0) {
      docIdSetBlock = new DocIdSetBlock(docIdArray, pos);
    }
    return docIdSetBlock;
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getOperatorName() {
    return "BReusableFilteredDocIdSetOperator";
  }

  @Override
  public boolean close() {
    _filterOperator.close();
    return true;
  }

}
