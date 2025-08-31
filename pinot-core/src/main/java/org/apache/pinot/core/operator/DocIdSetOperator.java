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
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.cache.SegmentQueryCache;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * The <code>DocIdSetOperator</code> takes a filter operator and returns blocks with set of the matched document Ids.
 * <p>Should call {@link #nextBlock()} multiple times until it returns <code>null</code> (already exhausts all the
 * matched documents) or already gathered enough documents (for selection queries).
 */
public class DocIdSetOperator extends BaseOperator<DocIdSetBlock> {
  private static final String EXPLAIN_NAME = "DOC_ID_SET";

  protected static final ThreadLocal<int[]> THREAD_LOCAL_DOC_IDS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);

  private final BaseFilterOperator _filterOperator;
  protected final int _maxSizeOfDocIdSet;

  private BlockDocIdSet _blockDocIdSet;
  private BlockDocIdIterator _blockDocIdIterator;
  protected int _currentDocId = 0;
  private final MutableRoaringBitmap _queryCacheToFill;
  private final SegmentQueryCache.QueryCacheUpdater _queryCacheUpdater;

  public DocIdSetOperator(BaseFilterOperator filterOperator, int maxSizeOfDocIdSet) {
    this(filterOperator, maxSizeOfDocIdSet, null, null);
  }

  public DocIdSetOperator(BaseFilterOperator filterOperator, int maxSizeOfDocIdSet,
      MutableRoaringBitmap queryCacheToFill,
      SegmentQueryCache.QueryCacheUpdater queryCacheUpdater) {
    Preconditions.checkArgument(maxSizeOfDocIdSet > 0 && maxSizeOfDocIdSet <= DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _filterOperator = filterOperator;
    _maxSizeOfDocIdSet = maxSizeOfDocIdSet;
    _queryCacheToFill = queryCacheToFill;
    _queryCacheUpdater = queryCacheUpdater;
  }

  @Override
  protected DocIdSetBlock getNextBlock() {
    if (_currentDocId == Constants.EOF) {
      return null;
    }

    // Initialize filter block document Id set
    if (_blockDocIdSet == null) {
      _blockDocIdSet = _filterOperator.nextBlock().getBlockDocIdSet();
      _blockDocIdIterator = _blockDocIdSet.iterator();
    }

    Tracing.ThreadAccountantOps.sample();

    int pos = 0;
    int[] docIds = THREAD_LOCAL_DOC_IDS.get();
    for (int i = 0; i < _maxSizeOfDocIdSet; i++) {
      _currentDocId = _blockDocIdIterator.next();
      if (_currentDocId == Constants.EOF) {
        break;
      }
      docIds[pos++] = _currentDocId;
    }
    if (_queryCacheToFill != null && _queryCacheUpdater != null) {
      _queryCacheToFill.addN(docIds, 0, pos);
      if (_currentDocId == Constants.EOF) {
        _queryCacheUpdater.update(_queryCacheToFill.toImmutableRoaringBitmap());
      }
    }
    if (pos > 0) {
      return new DocIdSetBlock(docIds, pos);
    } else {
      return null;
    }
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_filterOperator);
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
}
