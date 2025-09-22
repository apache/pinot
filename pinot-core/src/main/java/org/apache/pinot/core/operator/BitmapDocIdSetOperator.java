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

import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.roaringbitmap.ImmutableBitmapDataProvider;
import org.roaringbitmap.IntIterator;


/**
 * The <code>BitmapDocIdSetOperator</code> takes a bitmap of document ids and returns blocks of document ids.
 * <p>Should call {@link #nextBlock()} multiple times until it returns <code>null</code> (already exhausts all the
 * documents) or already gathered enough documents (for selection queries).
 */
public class BitmapDocIdSetOperator extends BaseDocIdSetOperator {
  private static final String EXPLAIN_NAME = "DOC_ID_SET_BITMAP";

  private final ImmutableBitmapDataProvider _docIds;
  private final int[] _docIdBuffer;
  private final DidOrder _didOrder;

  // TODO: Consider using BatchIterator to fill the document ids. Currently BatchIterator only reads bits for one
  //       container instead of trying to fill up the buffer with bits from multiple containers. If in the future
  //       BatchIterator provides an API to fill up the buffer, switch to BatchIterator.
  private IntIterator _docIdIterator;

  public BitmapDocIdSetOperator(ImmutableBitmapDataProvider docIds, int[] docIdBuffer, DidOrder didOrder) {
    _docIds = docIds;
    _docIdBuffer = docIdBuffer;
    _didOrder = didOrder;
  }

  public BitmapDocIdSetOperator(IntIterator docIdIterator, int[] docIdBuffer, DidOrder didOrder) {
    _docIds = null;
    _docIdIterator = docIdIterator;
    _docIdBuffer = docIdBuffer;
    _didOrder = didOrder;
  }

  public static BitmapDocIdSetOperator ascending(ImmutableBitmapDataProvider docIds) {
    return ascending(docIds, new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);
  }

  public static BitmapDocIdSetOperator ascending(ImmutableBitmapDataProvider docIds, int numDocs) {
    return ascending(docIds, new int[Math.min(numDocs, DocIdSetPlanNode.MAX_DOC_PER_CALL)]);
  }

  public static BitmapDocIdSetOperator ascending(ImmutableBitmapDataProvider docIds, int[] docIdBuffer) {
    return new BitmapDocIdSetOperator(docIds, docIdBuffer, DidOrder.ASC);
  }

  public static BitmapDocIdSetOperator descending(ImmutableBitmapDataProvider docIds, int numDocs) {
    return descending(docIds, new int[Math.min(numDocs, DocIdSetPlanNode.MAX_DOC_PER_CALL)]);
  }

  public static BitmapDocIdSetOperator descending(ImmutableBitmapDataProvider bitmap, int[] docIdBuffer) {
    return new BitmapDocIdSetOperator(bitmap, docIdBuffer, DidOrder.DESC);
  }

  @Override
  protected DocIdSetBlock getNextBlock() {
    if (_docIdIterator == null) {
      assert _docIds != null;
      _docIdIterator = _didOrder == DidOrder.ASC ? _docIds.getIntIterator() : _docIds.getReverseIntIterator();
    }
    int bufferSize = _docIdBuffer.length;
    int index = 0;
    while (index < bufferSize && _docIdIterator.hasNext()) {
      _docIdBuffer[index++] = _docIdIterator.next();
    }
    if (index > 0) {
      return new DocIdSetBlock(_docIdBuffer, index);
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
    return Collections.emptyList();
  }

  @Override
  public boolean isCompatibleWith(DidOrder order) {
    return _didOrder == order;
  }

  @Override
  public BaseDocIdSetOperator withOrder(DidOrder order)
      throws UnsupportedOperationException {
    if (isCompatibleWith(order)) {
      return this;
    }
    if (_docIds == null) {
      throw new UnsupportedOperationException(EXPLAIN_NAME + " doesn't support changing its order");
    }
    return new BitmapDocIdSetOperator(_docIds, _docIdBuffer, order);
  }
}
