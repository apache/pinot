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
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class CachedBitmapDocIdSetOperator extends DocIdSetOperator {
  private static final String EXPLAIN_NAME = "CACHED_BITMAP_DOC_ID_SET";
  private final ImmutableRoaringBitmap _queryCache;
  private final BatchIterator _batchIterator;

  public CachedBitmapDocIdSetOperator(ImmutableRoaringBitmap queryCache, int maxDocPerCall) {
    super(null, maxDocPerCall);
    _queryCache = queryCache;
    _batchIterator = _queryCache.getBatchIterator();
  }

  @Override
  protected DocIdSetBlock getNextBlock() {
    if (_currentDocId == Constants.EOF) {
      return null;
    }
    Tracing.ThreadAccountantOps.sample();
    int[] docIds = THREAD_LOCAL_DOC_IDS.get();
    int pos = _batchIterator.nextBatch(docIds);
    if (pos > 0) {
      if (pos < _maxSizeOfDocIdSet) {
        _currentDocId = Constants.EOF;
      } else {
        _currentDocId = docIds[pos - 1];
      }
      return new DocIdSetBlock(docIds, pos);
    } else {
      _currentDocId = Constants.EOF;
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
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putLong("maxDocs", _maxSizeOfDocIdSet);
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return new ExecutionStatistics(0, 0, 0, 0);
  }
}
