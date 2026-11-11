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
package org.apache.pinot.core.operator;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.dociditerators.RestrictedScanDocIdIterator;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.RowLevelSecurityFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.apache.pinot.spi.query.QueryThreadContext;


/// The `AscendingDocIdSetOperator` takes a filter operator and returns blocks with set of the matched
/// document Ids.
///
/// Should call [#nextBlock()] multiple times until it returns `null` (already exhausts all the
/// matched documents) or already gathered enough documents (for selection queries).
///
/// The returned [org.apache.pinot.core.operator.blocks.DocIdSetBlock] wraps a thread-local scratch buffer shared by
/// all `DocIdSetOperator` instances on the same thread: its contents are invalidated by the next [#nextBlock()] call
/// on any instance on this thread. Consumers that hold on to a block across pulls must copy the array.
public class DocIdSetOperator extends BaseDocIdSetOperator implements AutoCloseable {
  private static final String EXPLAIN_NAME = "DOC_ID_SET";

  private static final ThreadLocal<int[]> THREAD_LOCAL_DOC_IDS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);

  private final BaseFilterOperator _filterOperator;
  private final int _maxSizeOfDocIdSet;

  private BlockDocIdSet _blockDocIdSet;
  private BlockDocIdIterator _blockDocIdIterator;
  private int _currentDocId = 0;
  private long _lastReportedEntriesScanned = 0;

  public DocIdSetOperator(BaseFilterOperator filterOperator, int maxSizeOfDocIdSet) {
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
    if (_blockDocIdSet == null) {
      _blockDocIdSet = _filterOperator.nextBlock().getBlockDocIdSet();
      BlockDocIdIterator earlyTerminationIterator = null;
      if (_filterOperator instanceof RowLevelSecurityFilterOperator rls && rls.isStreamingRestrictionEnabled()
          && _blockDocIdSet instanceof AndDocIdSet andDocIdSet) {
        earlyTerminationIterator = andDocIdSet.iteratorForEarlyTermination(
            BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE, this::afterRestrictedBatch);
      }
      _blockDocIdIterator = earlyTerminationIterator != null ? earlyTerminationIterator : _blockDocIdSet.iterator();
    }

    QueryThreadContext.sampleUsage();

    int pos = 0;
    int[] docIds = THREAD_LOCAL_DOC_IDS.get();
    for (int i = 0; i < _maxSizeOfDocIdSet; i++) {
      _currentDocId = _blockDocIdIterator.next();
      if (_currentDocId == Constants.EOF) {
        break;
      }
      docIds[pos++] = _currentDocId;
    }
    if (pos > 0 || _blockDocIdIterator instanceof RestrictedScanDocIdIterator) {
      // A restricted scan can reach EOF without producing a block.
      reportEntriesScannedInFilter();
    }
    return pos > 0 ? new DocIdSetBlock(docIds, pos) : null;
  }

  private void afterRestrictedBatch() {
    // A batch can scan candidates without producing any matching documents.
    reportEntriesScannedInFilter();
    checkTerminationAndSampleUsage();
  }

  private void reportEntriesScannedInFilter() {
    QueryScanCostContext scanCost = getScanCostContext();
    if (scanCost != null) {
      long currentTotal = _blockDocIdSet.getNumEntriesScannedInFilter();
      long delta = currentTotal - _lastReportedEntriesScanned;
      if (delta > 0) {
        scanCost.addEntriesScannedInFilter(delta);
        _lastReportedEntriesScanned = currentTotal;
      }
    }
  }

  /// Releases the restricted iterator when a selection finishes, including when it stops before EOF.
  @Override
  public void close() {
    if (_blockDocIdIterator instanceof RestrictedScanDocIdIterator restrictedIterator) {
      _currentDocId = Constants.EOF;
      restrictedIterator.close();
    }
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return List.of(_filterOperator);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putLong("maxDocs", _maxSizeOfDocIdSet);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    long numEntriesScannedInFilter = _blockDocIdSet != null ? _blockDocIdSet.getNumEntriesScannedInFilter() : 0;
    return new ExecutionStatistics(0, numEntriesScannedInFilter, 0, 0);
  }

  @Override
  public boolean isCompatibleWith(DocIdOrder order) {
    return order == DocIdOrder.ASC;
  }

  @Override
  public BaseDocIdSetOperator withOrder(DocIdOrder order) {
    if (isCompatibleWith(order)) {
      return this;
    }
    return new ReverseDocIdSetOperator(_filterOperator, _maxSizeOfDocIdSet);
  }
}
