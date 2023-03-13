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
public class BitmapDocIdSetOperator extends BaseOperator<DocIdSetBlock> {

  private static final String EXPLAIN_NAME = "DOC_ID_SET_BITMAP";

  // TODO: Consider using BatchIterator to fill the document ids. Currently BatchIterator only reads bits for one
  //       container instead of trying to fill up the buffer with bits from multiple containers. If in the future
  //       BatchIterator provides an API to fill up the buffer, switch to BatchIterator.
  private final IntIterator _intIterator;
  private final int[] _docIdBuffer;

  public BitmapDocIdSetOperator(ImmutableBitmapDataProvider bitmap) {
    _intIterator = bitmap.getIntIterator();
    _docIdBuffer = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
  }

  public BitmapDocIdSetOperator(ImmutableBitmapDataProvider bitmap, int numDocs) {
    _intIterator = bitmap.getIntIterator();
    _docIdBuffer = new int[Math.min(numDocs, DocIdSetPlanNode.MAX_DOC_PER_CALL)];
  }

  public BitmapDocIdSetOperator(IntIterator intIterator, int[] docIdBuffer) {
    _intIterator = intIterator;
    _docIdBuffer = docIdBuffer;
  }

  public BitmapDocIdSetOperator(ImmutableBitmapDataProvider bitmap, int[] docIdBuffer) {
    _intIterator = bitmap.getIntIterator();
    _docIdBuffer = docIdBuffer;
  }

  @Override
  protected DocIdSetBlock getNextBlock() {
    int bufferSize = _docIdBuffer.length;
    int index = 0;
    while (index < bufferSize && _intIterator.hasNext()) {
      _docIdBuffer[index++] = _intIterator.next();
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
}
