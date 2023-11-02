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
package org.apache.pinot.core.query.distinct.dictionary;

import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntPriorityQueue;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.segment.spi.datasource.NullMode;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * {@link DistinctExecutor} for distinct order-by queries with single dictionary-encoded column.
 */
public class DictionaryBasedSingleColumnDistinctOrderByExecutor
    extends BaseDictionaryBasedSingleColumnDistinctExecutor {
  private final IntPriorityQueue _priorityQueue;

  public DictionaryBasedSingleColumnDistinctOrderByExecutor(ExpressionContext expression, Dictionary dictionary,
      DataType dataType, OrderByExpressionContext orderByExpressionContext, int limit) {
    super(expression, dictionary, dataType, limit, NullMode.NONE_NULLABLE);

    assert orderByExpressionContext.getExpression().equals(expression);
    int comparisonFactor = orderByExpressionContext.isAsc() ? -1 : 1;
    _priorityQueue =
        new IntHeapPriorityQueue(Math.min(limit, MAX_INITIAL_CAPACITY), (i1, i2) -> (i1 - i2) * comparisonFactor);
  }

  @Override
  public boolean process(ValueBlock valueBlock) {
    BlockValSet blockValueSet = valueBlock.getBlockValueSet(_expression);
    int numDocs = valueBlock.getNumDocs();
    if (blockValueSet.isSingleValue()) {
      int[] dictIds = blockValueSet.getDictionaryIdsSV();
      for (int i = 0; i < numDocs; i++) {
        add(dictIds[i]);
      }
    } else {
      int[][] dictIds = blockValueSet.getDictionaryIdsMV();
      for (int i = 0; i < numDocs; i++) {
        for (int dictId : dictIds[i]) {
          add(dictId);
        }
      }
    }
    return false;
  }

  private void add(int dictId) {
    if (!_dictIdSet.contains(dictId)) {
      if (_dictIdSet.size() < _limit) {
        _dictIdSet.add(dictId);
        _priorityQueue.enqueue(dictId);
      } else {
        int firstDictId = _priorityQueue.firstInt();
        if (_priorityQueue.comparator().compare(dictId, firstDictId) > 0) {
          _dictIdSet.remove(firstDictId);
          _dictIdSet.add(dictId);
          _priorityQueue.dequeueInt();
          _priorityQueue.enqueue(dictId);
        }
      }
    }
  }
}
