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
package com.linkedin.pinot.core.operator;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.blocks.DocIdSetBlock;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;

/**
 * BReusableFilteredDocIdSetOperator will take a filter Operator and get the matched docId set.
 * Internally, cached a given size of docIds, so this Operator could be replicated
 * for many ColumnarReaderDataSource.
 */
public class BReusableFilteredDocIdSetOperator extends BaseOperator<DocIdSetBlock> {
  private static final String OPERATOR_NAME = "BReusableFilteredDocIdSetOperator";

  private static final ThreadLocal<int[]> DOC_ID_ARRAY = new ThreadLocal<int[]>() {
    @Override
    protected int[] initialValue() {
      return new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
  };

  private final BaseFilterOperator _filterOperator;
  private final int _maxSizeOfDocIdSet;
  private FilterBlockDocIdSet _filterBlockDocIdSet;
  private BlockDocIdIterator _blockDocIdIterator;
  private int _currentDocId = 0;

  /**
   * @param filterOperator
   * @param docSize
   * @param maxSizeOfDocIdSet must be less than {@link DocIdSetPlanNode}. MAX_DOC_PER_CALL which is
   *          10000
   */
  public BReusableFilteredDocIdSetOperator(Operator filterOperator, int docSize,
      int maxSizeOfDocIdSet) {
    Preconditions.checkArgument(maxSizeOfDocIdSet <= DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _maxSizeOfDocIdSet = maxSizeOfDocIdSet;
    _filterOperator = (BaseFilterOperator) filterOperator;
  }

  @Override
  protected DocIdSetBlock getNextBlock() {
    // Handle limit 0 clause safely.
    // For limit 0, _docIdArray will be zero sized
    if (_currentDocId == Constants.EOF) {
      return null;
    }
    int[] docIdArray = DOC_ID_ARRAY.get();
    // Initialize filter block doc id set.
    if (_filterBlockDocIdSet == null) {
      _filterBlockDocIdSet = (FilterBlockDocIdSet) _filterOperator.nextBlock().getBlockDocIdSet();
      _blockDocIdIterator = _filterBlockDocIdSet.iterator();
    }
    int pos = 0;
    for (int i = 0; i < _maxSizeOfDocIdSet; i++) {
      _currentDocId = _blockDocIdIterator.next();
      if (_currentDocId == Constants.EOF) {
        break;
      }
      docIdArray[pos++] = _currentDocId;
    }
    if (pos > 0) {
      return new DocIdSetBlock(docIdArray, pos);
    } else {
      return null;
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return new ExecutionStatistics(0L, _filterBlockDocIdSet.getNumEntriesScannedInFilter(), 0L, 0L);
  }
}
