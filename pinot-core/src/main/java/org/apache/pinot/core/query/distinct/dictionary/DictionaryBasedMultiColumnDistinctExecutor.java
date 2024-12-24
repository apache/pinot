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

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutorUtils;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.distinct.table.MultiColumnDistinctTable;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;


/**
 * {@link DistinctExecutor} for multiple dictionary-encoded columns.
 */
public class DictionaryBasedMultiColumnDistinctExecutor implements DistinctExecutor {
  private final List<ExpressionContext> _expressions;
  private final boolean _hasMVExpression;
  private final DataSchema _dataSchema;
  private final List<Dictionary> _dictionaries;
  private final int _limit;
  private final boolean _nullHandlingEnabled;
  private final int[] _nullDictIds;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final int[] _orderByExpressionIndices;
  private final int[] _comparisonFactors;
  private final HashSet<DictIds> _dictIdsSet;

  private ObjectHeapPriorityQueue<DictIds> _priorityQueue;

  public DictionaryBasedMultiColumnDistinctExecutor(List<ExpressionContext> expressions, boolean hasMVExpression,
      DataSchema dataSchema, List<Dictionary> dictionaries, int limit, boolean nullHandlingEnabled,
      @Nullable List<OrderByExpressionContext> orderByExpressions) {
    _expressions = expressions;
    _hasMVExpression = hasMVExpression;
    _dataSchema = dataSchema;
    _dictionaries = dictionaries;
    _limit = limit;
    _nullHandlingEnabled = nullHandlingEnabled;
    if (nullHandlingEnabled) {
      _nullDictIds = new int[_expressions.size()];
      Arrays.fill(_nullDictIds, -1);
    } else {
      _nullDictIds = null;
    }
    _orderByExpressions = orderByExpressions;
    if (orderByExpressions != null) {
      int numOrderByExpressions = orderByExpressions.size();
      _orderByExpressionIndices = new int[numOrderByExpressions];
      _comparisonFactors = new int[numOrderByExpressions];
      for (int i = 0; i < numOrderByExpressions; i++) {
        OrderByExpressionContext orderByExpression = orderByExpressions.get(i);
        int index = expressions.indexOf(orderByExpression.getExpression());
        _orderByExpressionIndices[i] = index;
        _comparisonFactors[i] = orderByExpression.isAsc() ? -1 : 1;
        // When there are null values:
        // - ASC & nulls last: set null dictId to Integer.MAX_VALUE
        // - DESC & nulls first: set null dictId to Integer.MIN_VALUE
        if (nullHandlingEnabled && orderByExpression.isAsc() == orderByExpression.isNullsLast()) {
          _nullDictIds[index] = Integer.MAX_VALUE;
        }
      }
    } else {
      _orderByExpressionIndices = null;
      _comparisonFactors = null;
    }

    _dictIdsSet = Sets.newHashSetWithExpectedSize(Math.min(limit, MAX_INITIAL_CAPACITY));
  }

  @Override
  public boolean process(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int numExpressions = _expressions.size();
    if (!_hasMVExpression) {
      int[][] dictIdsArray = new int[numDocs][numExpressions];
      for (int i = 0; i < numExpressions; i++) {
        BlockValSet blockValueSet = valueBlock.getBlockValueSet(_expressions.get(i));
        int[] dictIdsForExpression = getDictIdsSV(blockValueSet, i);
        for (int j = 0; j < numDocs; j++) {
          dictIdsArray[j][i] = dictIdsForExpression[j];
        }
      }
      if (_limit == Integer.MAX_VALUE) {
        for (int i = 0; i < numDocs; i++) {
          addUnbounded(new DictIds(dictIdsArray[i]));
        }
      } else if (_orderByExpressions == null) {
        for (int i = 0; i < numDocs; i++) {
          if (addWithoutOrderBy(new DictIds(dictIdsArray[i]))) {
            return true;
          }
        }
      } else {
        for (int i = 0; i < numDocs; i++) {
          addWithOrderBy(new DictIds(dictIdsArray[i]));
        }
      }
    } else {
      int[][] svDictIds = new int[numExpressions][];
      int[][][] mvDictIds = new int[numExpressions][][];
      for (int i = 0; i < numExpressions; i++) {
        BlockValSet blockValueSet = valueBlock.getBlockValueSet(_expressions.get(i));
        if (blockValueSet.isSingleValue()) {
          svDictIds[i] = getDictIdsSV(blockValueSet, i);
        } else {
          mvDictIds[i] = blockValueSet.getDictionaryIdsMV();
        }
      }
      if (_limit == Integer.MAX_VALUE) {
        for (int i = 0; i < numDocs; i++) {
          int[][] dictIdsArray = DistinctExecutorUtils.getDictIds(svDictIds, mvDictIds, i);
          for (int[] dictIds : dictIdsArray) {
            addUnbounded(new DictIds(dictIds));
          }
        }
      } else if (_orderByExpressions == null) {
        for (int i = 0; i < numDocs; i++) {
          int[][] dictIdsArray = DistinctExecutorUtils.getDictIds(svDictIds, mvDictIds, i);
          for (int[] dictIds : dictIdsArray) {
            if (addWithoutOrderBy(new DictIds(dictIds))) {
              return true;
            }
          }
        }
      } else {
        for (int i = 0; i < numDocs; i++) {
          int[][] dictIdsArray = DistinctExecutorUtils.getDictIds(svDictIds, mvDictIds, i);
          for (int[] dictIds : dictIdsArray) {
            addWithOrderBy(new DictIds(dictIds));
          }
        }
      }
    }
    return false;
  }

  private int[] getDictIdsSV(BlockValSet blockValueSet, int expressionIndex) {
    int[] dictIds = blockValueSet.getDictionaryIdsSV();
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValueSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        int nullDictId = _nullDictIds[expressionIndex];
        nullBitmap.forEach((IntConsumer) docId -> dictIds[docId] = nullDictId);
      }
    }
    return dictIds;
  }

  private void addUnbounded(DictIds dictIds) {
    _dictIdsSet.add(dictIds);
  }

  private boolean addWithoutOrderBy(DictIds dictIds) {
    assert _dictIdsSet.size() < _limit;
    _dictIdsSet.add(dictIds);
    return _dictIdsSet.size() == _limit;
  }

  private void addWithOrderBy(DictIds dictIds) {
    assert _dictIdsSet.size() <= _limit;
    if (_dictIdsSet.size() < _limit) {
      _dictIdsSet.add(dictIds);
      return;
    }
    if (_dictIdsSet.contains(dictIds)) {
      return;
    }
    if (_priorityQueue == null) {
      _priorityQueue = new ObjectHeapPriorityQueue<>(_dictIdsSet, getComparator());
    }
    DictIds firstDictIds = _priorityQueue.first();
    if (_priorityQueue.comparator().compare(dictIds, firstDictIds) > 0) {
      _dictIdsSet.remove(firstDictIds);
      _dictIdsSet.add(dictIds);
      _priorityQueue.dequeue();
      _priorityQueue.enqueue(dictIds);
    }
  }

  private Comparator<DictIds> getComparator() {
    assert _orderByExpressionIndices != null && _comparisonFactors != null;
    int numOrderByExpressions = _orderByExpressionIndices.length;
    return (d1, d2) -> {
      int[] dictIds1 = d1._dictIds;
      int[] dictIds2 = d2._dictIds;
      for (int i = 0; i < numOrderByExpressions; i++) {
        int index = _orderByExpressionIndices[i];
        int result = dictIds1[index] - dictIds2[index];
        if (result != 0) {
          return result * _comparisonFactors[i];
        }
      }
      return 0;
    };
  }

  @Override
  public DistinctTable getResult() {
    MultiColumnDistinctTable distinctTable =
        new MultiColumnDistinctTable(_dataSchema, _limit, _nullHandlingEnabled, _orderByExpressions,
            _dictIdsSet.size());
    int numExpressions = _expressions.size();
    if (_nullHandlingEnabled) {
      for (DictIds dictIds : _dictIdsSet) {
        Object[] values = new Object[numExpressions];
        for (int i = 0; i < numExpressions; i++) {
          int dictId = dictIds._dictIds[i];
          if (dictId != -1 && dictId != Integer.MAX_VALUE) {
            values[i] = _dictionaries.get(i).getInternal(dictId);
          }
        }
        distinctTable.addUnbounded(new Record(values));
      }
    } else {
      for (DictIds dictIds : _dictIdsSet) {
        Object[] values = new Object[numExpressions];
        for (int i = 0; i < numExpressions; i++) {
          values[i] = _dictionaries.get(i).getInternal(dictIds._dictIds[i]);
        }
        distinctTable.addUnbounded(new Record(values));
      }
    }
    return distinctTable;
  }

  private static class DictIds {
    final int[] _dictIds;

    DictIds(int[] dictIds) {
      _dictIds = dictIds;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
      return Arrays.equals(_dictIds, ((DictIds) o)._dictIds);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(_dictIds);
    }
  }
}
