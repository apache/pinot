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
package org.apache.pinot.core.operator.query;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class AggregationGroupByOptUtil {
  private final IndexSegment _indexSegment;
  private final TransformOperator _transformOperator;
  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _groupByExpressions;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final TransformResultMetadata[] _orderByExpressionMetadata;
  private final int _limit;
  private final int _numOrderByExpressions;
  private final PriorityQueue<Object[]> _rows;
  private final HashMap<Key, List<Integer>> _groupByKeyMap;

  public AggregationGroupByOptUtil(IndexSegment indexSegment, QueryContext queryContext,
      AggregationFunction[] aggregationFunctions, ExpressionContext[] groupByExpressions,
      TransformOperator transformOperator, List<OrderByExpressionContext> orderByExpressions) {
    _indexSegment = indexSegment;
    _transformOperator = transformOperator;
    _groupByExpressions = groupByExpressions;
    _aggregationFunctions = aggregationFunctions;
    // TODO: can we use shallow copy here?
    _orderByExpressions = new ArrayList<>(orderByExpressions);

    Set<ExpressionContext> orderByExpressionsSet = new HashSet<>();
    int numOrderByExpressions = _orderByExpressions.size();
    int numGroupByExpressions = _groupByExpressions.length;
    // pre-allocate space for groupBy keys as well
    _orderByExpressionMetadata = new TransformResultMetadata[numGroupByExpressions];
    for (int i = 0; i < numOrderByExpressions; i++) {
      ExpressionContext expression = _orderByExpressions.get(i).getExpression();
      _orderByExpressionMetadata[i] = _transformOperator.getResultMetadata(expression);
      orderByExpressionsSet.add(expression);
    }
    for (ExpressionContext expression : _groupByExpressions) {
      if (!orderByExpressionsSet.contains(expression)) {
        OrderByExpressionContext orderByExpressionContext = new OrderByExpressionContext(expression, true);
        _orderByExpressions.add(orderByExpressionContext);
        _orderByExpressionMetadata[numOrderByExpressions++] = _transformOperator.getResultMetadata(expression);
      }
    }
    _numOrderByExpressions = numOrderByExpressions;
    _limit = queryContext.getLimit();

    _rows = new PriorityQueue<>(_limit, getComparator());
    _groupByKeyMap = new HashMap<>();
  }

  private Comparator<Object[]> getComparator() {
    // Compare all single-value columns
    List<Integer> valueIndexList = new ArrayList<>(_numOrderByExpressions);
    for (int i = 0; i < _numOrderByExpressions; i++) {
      if (_orderByExpressionMetadata[i].isSingleValue()) {
        valueIndexList.add(i);
      }
    }

    int numValuesToCompare = valueIndexList.size();
    int[] valueIndices = new int[numValuesToCompare];
    FieldSpec.DataType[] storedTypes = new FieldSpec.DataType[numValuesToCompare];
    // Use multiplier -1 or 1 to control ascending/descending order
    int[] multipliers = new int[numValuesToCompare];
    for (int i = 0; i < numValuesToCompare; i++) {
      int valueIndex = valueIndexList.get(i);
      valueIndices[i] = valueIndex;
      storedTypes[i] = _orderByExpressionMetadata[valueIndex].getDataType().getStoredType();
      multipliers[i] = _orderByExpressions.get(valueIndex).isAsc() ? -1 : 1;
    }

    return (o1, o2) -> {
      for (int i = 0; i < numValuesToCompare; i++) {
        int index = valueIndices[i];

        // TODO: Evaluate the performance of casting to Comparable and avoid the switch
        Object v1 = o1[index];
        Object v2 = o2[index];
        int result;
        switch (storedTypes[i]) {
          case INT:
            result = ((Integer) v1).compareTo((Integer) v2);
            break;
          case LONG:
            result = ((Long) v1).compareTo((Long) v2);
            break;
          case FLOAT:
            result = ((Float) v1).compareTo((Float) v2);
            break;
          case DOUBLE:
            result = ((Double) v1).compareTo((Double) v2);
            break;
          case STRING:
            result = ((String) v1).compareTo((String) v2);
            break;
          case BYTES:
            result = ((ByteArray) v1).compareTo((ByteArray) v2);
            break;
          // NOTE: Multi-value columns are not comparable, so we should not reach here
          default:
            throw new IllegalStateException();
        }
        if (result != 0) {
          return result * multipliers[i];
        }
      }
      return 0;
    };
  }

  protected TransformOperator constructTransformOperator() {
    //Do it in two passes
    // Fetch the order-by expressions and docIds and insert them into the priority queue
    BlockValSet[] blockValSets = new BlockValSet[_numOrderByExpressions];
    BlockValSet[] docIdValSets = new BlockValSet[1];
    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      for (int i = 0; i < _numOrderByExpressions; i++) {
        ExpressionContext expression = _orderByExpressions.get(i).getExpression();
        blockValSets[i] = transformBlock.getBlockValueSet(expression);
      }
      docIdValSets[0] = transformBlock.getBlockValueSet(CommonConstants.Segment.BuiltInVirtualColumn.DOCID);
      RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);
      RowBasedBlockValueFetcher docIdValueFetcher = new RowBasedBlockValueFetcher(docIdValSets);
      int numDocsFetched = transformBlock.getNumDocs();
      for (int i = 0; i < numDocsFetched; i++) {
        Object[] row = new Object[_numOrderByExpressions];
        Object[] docId = new Object[1];
        blockValueFetcher.getRow(i, row, 0);
        docIdValueFetcher.getRow(i, docId, 0);
        AddToPriorityQueue(row, docId);
      }
    }

    int numRows = _rows.size();
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    for (Map.Entry<Key, List<Integer>> entry : _groupByKeyMap.entrySet()) {
      List<Integer> filteredDocIds = entry.getValue();
      for (Integer docId : filteredDocIds) {
        docIds.add(docId);
      }
    }

    // Make a new transform operator
    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(_aggregationFunctions, _groupByExpressions);
    Set<String> columns = new HashSet<>();
    for (ExpressionContext expression : expressionsToTransform) {
      expression.getColumns(columns);
    }
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    for (String column : columns) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
    }
    // TODO: Create own BitmapDocIdSetOperator
    ProjectionOperator projectionOperator =
        new ProjectionOperator(dataSourceMap, new SelectionOrderByOperator.BitmapDocIdSetOperator(docIds, numRows));

    return new TransformOperator(projectionOperator, expressionsToTransform);
  }

  private void AddToPriorityQueue(Object[] row, Object[] docIds) {
    if (_rows.size() < _limit) {
      Key groupByKeys = new Key(row);
      int docId = (int) docIds[0];
      if (AddToMap(groupByKeys, docId)) {
        _rows.add(row);
      }
    } else {
      int compareResult = _rows.comparator().compare(_rows.peek(), row);
      if (compareResult < 0) {
        Key groupByKeys = new Key(row);
        int docId = (int) docIds[0];
        if (AddToMap(groupByKeys, docId)) {
          Object[] removedRow = _rows.poll();
          Key removedGroupByKey = new Key(removedRow);
          _groupByKeyMap.remove(removedGroupByKey);
          _rows.offer(row);
        }
      } else if (compareResult == 0) {
        Key groupByKeys = new Key(row);
        int docId = (int) docIds[0];
        List<Integer> docList = _groupByKeyMap.get(groupByKeys);
        if (docList != null) {
          docList.add(docId);
          _groupByKeyMap.put(groupByKeys, docList);
        }
      }
    }
  }

  private boolean AddToMap(Key groupByKeys, int docID) {
    // TODO: Use bitmap instead
    List<Integer> docList = _groupByKeyMap.get(groupByKeys);
    if (docList == null) {
      _groupByKeyMap.put(groupByKeys, new ArrayList<>(docID));
      return true;
    } else {
      docList.add(docID);
      _groupByKeyMap.put(groupByKeys, docList);
      return false;
    }
  }
}
