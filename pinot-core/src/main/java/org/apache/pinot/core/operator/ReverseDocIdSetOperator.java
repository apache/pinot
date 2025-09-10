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
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.dociditerators.BitmapBasedDocIdIterator;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


public class DescDocIdSetOperator extends BaseDocIdSetOperator {
  private static final String EXPLAIN_NAME = "DOC_ID_SET";

  private static final ThreadLocal<int[]> THREAD_LOCAL_DOC_IDS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);

  private final BaseFilterOperator _filterOperator;
  private final int _maxSizeOfDocIdSet;

  private BlockDocIdSet _blockDocIdSet;
  private int _currentDocId = 0;
  private IntIterator _reverseIterator;

  public DescDocIdSetOperator(BaseFilterOperator filterOperator, int maxSizeOfDocIdSet) {
    Preconditions.checkArgument(maxSizeOfDocIdSet > 0 && maxSizeOfDocIdSet <= DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _filterOperator = filterOperator;
    _maxSizeOfDocIdSet = maxSizeOfDocIdSet;
  }

  @Override
  protected DocIdSetBlock getNextBlock() {
    if (_reverseIterator == null) {
      initializeBitmap();
    }

    if (_currentDocId == Constants.EOF) {
      return null;
    }

    Tracing.ThreadAccountantOps.sample();

    int pos = 0;
    int[] docIds = THREAD_LOCAL_DOC_IDS.get();
    for (int i = 0; i < _maxSizeOfDocIdSet && _reverseIterator.hasNext(); i++) {
      _currentDocId = _reverseIterator.next();
      docIds[pos++] = _currentDocId;
    }
    if (pos > 0) {
      return new DocIdSetBlock(docIds, pos);
    } else {
      return null;
    }
  }

  private void initializeBitmap() {
    _blockDocIdSet = _filterOperator.nextBlock().getBlockDocIdSet();
    BlockDocIdIterator iterator = _blockDocIdSet.iterator();
    if (iterator instanceof BitmapBasedDocIdIterator) {
      _reverseIterator = ((BitmapBasedDocIdIterator) iterator).getDocIds().getReverseIntIterator();
    } else {
      RoaringBitmapWriter<RoaringBitmap> writer = RoaringBitmapWriter.writer().get();
      int docId = iterator.next();
      while (docId != Constants.EOF) {
        writer.add(docId);
        docId = iterator.next();
      }
      _reverseIterator = writer.get().getReverseIntIterator();
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

  @Override
  public boolean isAscending() {
    return false;
  }

  @Override
  public boolean isDescending() {
    return true;
  }

  @Override
  public BaseDocIdSetOperator withOrder(boolean ascending)
      throws UnsupportedOperationException {
    if (isAscending() == ascending) {
      return this;
    }
    return new DocIdSetOperator(_filterOperator, _maxSizeOfDocIdSet);
  }
}
