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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.roaringbitmap.IntIterator;


public class IntIteratorDocIdSetOperator extends BaseDocIdSetOperator {

  private static final String EXPLAIN_NAME = "DOC_ID_SET_ITERATOR";

  // TODO: Consider using BatchIterator to fill the document ids. Currently BatchIterator only reads bits for one
  //       container instead of trying to fill up the buffer with bits from multiple containers. If in the future
  //       BatchIterator provides an API to fill up the buffer, switch to BatchIterator.
  private final IntIterator _intIterator;
  private final int[] _docIdBuffer;
  private final DidOrder _didOrder;

  public IntIteratorDocIdSetOperator(IntIterator intIterator, int[] docIdBuffer, DidOrder didOrder) {
    _intIterator = intIterator;
    _docIdBuffer = docIdBuffer;
    _didOrder = didOrder;
  }

  @Override
  public boolean isCompatibleWith(DidOrder order) {
    return _didOrder == order;
  }

  @Override
  public BaseDocIdSetOperator withOrder(DidOrder order)
      throws UnsupportedOperationException {
    if (_didOrder != order) {
      throw new UnsupportedOperationException(EXPLAIN_NAME + " doesn't support changing its order");
    }
    return this;
  }

  @Override
  @Nullable
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
  public List<? extends Operator> getChildOperators() {
    return List.of();
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
