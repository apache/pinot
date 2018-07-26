/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.operator.blocks.DocIdSetBlock;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import javax.annotation.Nonnull;


/**
 * The <code>DocIdSetOperator</code> takes a filter operator and returns blocks with set of the matched document Ids.
 * <p>Should call {@link #nextBlock()} multiple times until it returns <code>null</code> (already exhausts all the
 * matched documents) or already gathered enough documents (for selection queries).
 */
public class DocIdSetOperator extends BaseOperator<DocIdSetBlock> {
  private static final String OPERATOR_NAME = "DocIdSetOperator";

  private static final ThreadLocal<int[]> THREAD_LOCAL_DOC_IDS = new ThreadLocal<int[]>() {
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

  public DocIdSetOperator(@Nonnull BaseFilterOperator filterOperator, int maxSizeOfDocIdSet) {
    Preconditions.checkArgument(maxSizeOfDocIdSet > 0 && maxSizeOfDocIdSet <= DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _filterOperator = filterOperator;
    _maxSizeOfDocIdSet = maxSizeOfDocIdSet;
  }

  @Override
  protected DocIdSetBlock getNextBlock() {
    if (_currentDocId == Constants.EOF) {
      return null;
    }

    // Initialize filter block document Id set
    if (_filterBlockDocIdSet == null) {
      _filterBlockDocIdSet = (FilterBlockDocIdSet) _filterOperator.nextBlock().getBlockDocIdSet();
      _blockDocIdIterator = _filterBlockDocIdSet.iterator();
    }

    int pos = 0;
    int[] docIds = THREAD_LOCAL_DOC_IDS.get();
    for (int i = 0; i < _maxSizeOfDocIdSet; i++) {
      _currentDocId = _blockDocIdIterator.next();
      if (_currentDocId == Constants.EOF) {
        break;
      }
      docIds[pos++] = _currentDocId;
    }
    if (pos > 0) {
      return new DocIdSetBlock(docIds, pos);
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
