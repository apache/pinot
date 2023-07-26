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

import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutorUtils;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * {@link DistinctExecutor} for distinct order-by queries with multiple dictionary-encoded columns.
 */
public class DictionaryBasedMultiColumnDistinctOrderByExecutor extends BaseDictionaryBasedMultiColumnDistinctExecutor {
  private final boolean _hasMVExpression;
  private final PriorityQueue<DictIds> _priorityQueue;

  public DictionaryBasedMultiColumnDistinctOrderByExecutor(List<ExpressionContext> expressions, boolean hasMVExpression,
      List<Dictionary> dictionaries, List<DataType> dataTypes, List<OrderByExpressionContext> orderByExpressions,
      int limit) {
    super(expressions, dictionaries, dataTypes, limit);
    _hasMVExpression = hasMVExpression;

    int numOrderByExpressions = orderByExpressions.size();
    int[] orderByExpressionIndices = new int[numOrderByExpressions];
    int[] comparisonFactors = new int[numOrderByExpressions];
    for (int i = 0; i < numOrderByExpressions; i++) {
      OrderByExpressionContext orderByExpression = orderByExpressions.get(i);
      orderByExpressionIndices[i] = expressions.indexOf(orderByExpression.getExpression());
      comparisonFactors[i] = orderByExpression.isAsc() ? -1 : 1;
    }
    _priorityQueue = new ObjectHeapPriorityQueue<>(Math.min(limit, MAX_INITIAL_CAPACITY), (o1, o2) -> {
      int[] dictIds1 = o1._dictIds;
      int[] dictIds2 = o2._dictIds;
      for (int i = 0; i < numOrderByExpressions; i++) {
        int index = orderByExpressionIndices[i];
        int result = dictIds1[index] - dictIds2[index];
        if (result != 0) {
          return result * comparisonFactors[i];
        }
      }
      return 0;
    });
  }

  @Override
  public boolean process(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int numExpressions = _expressions.size();
    if (!_hasMVExpression) {
      int[][] dictIdsArray = new int[numDocs][numExpressions];
      for (int i = 0; i < numExpressions; i++) {
        BlockValSet blockValueSet = valueBlock.getBlockValueSet(_expressions.get(i));
        int[] dictIdsForExpression = blockValueSet.getDictionaryIdsSV();
        for (int j = 0; j < numDocs; j++) {
          dictIdsArray[j][i] = dictIdsForExpression[j];
        }
      }
      for (int i = 0; i < numDocs; i++) {
        add(new DictIds(dictIdsArray[i]));
      }
    } else {
      int[][] svDictIds = new int[numExpressions][];
      int[][][] mvDictIds = new int[numExpressions][][];
      for (int i = 0; i < numExpressions; i++) {
        BlockValSet blockValueSet = valueBlock.getBlockValueSet(_expressions.get(i));
        if (blockValueSet.isSingleValue()) {
          svDictIds[i] = blockValueSet.getDictionaryIdsSV();
        } else {
          mvDictIds[i] = blockValueSet.getDictionaryIdsMV();
        }
      }
      for (int i = 0; i < numDocs; i++) {
        int[][] dictIdsArray = DistinctExecutorUtils.getDictIds(svDictIds, mvDictIds, i);
        for (int[] dictIds : dictIdsArray) {
          add(new DictIds(dictIds));
        }
      }
    }
    return false;
  }

  private void add(DictIds dictIds) {
    if (!_dictIdsSet.contains(dictIds)) {
      if (_dictIdsSet.size() < _limit) {
        _dictIdsSet.add(dictIds);
        _priorityQueue.enqueue(dictIds);
      } else {
        DictIds firstDictIds = _priorityQueue.first();
        if (_priorityQueue.comparator().compare(dictIds, firstDictIds) > 0) {
          _dictIdsSet.remove(firstDictIds);
          _dictIdsSet.add(dictIds);
          _priorityQueue.dequeue();
          _priorityQueue.enqueue(dictIds);
        }
      }
    }
  }
}
