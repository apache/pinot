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
import java.util.Arrays;
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
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
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


public class AggregationGroupByNoOrderByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "AggregationGroupByNoOrderByOperator";

  private final IndexSegment _indexSegment;
  private final List<ExpressionContext> _expressions;
  private final List<ExpressionContext> _groupByExpressions;
  private final TransformOperator _transformOperator;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final int _numOrderByExpressions;
  private final int _numGroupByExpressions;
  private final TransformResultMetadata[] _orderByExpressionMetadata;
  private final AggregationFunction[] _aggregationFunctions;
  // TODO: Optimize this limit
  private final int ROW_LIMIT = 5000;
  private final PriorityQueue<Object[]> _rows;
  //TODO: or use gorupKey generator
  private final HashMap<Object[], List<Integer>> _groupByKeyMap;
  private FieldSpec.DataType[] _storedTypes;

  public AggregationGroupByNoOrderByOperator(IndexSegment indexSegment, QueryContext queryContext,
      List<ExpressionContext> expressions, TransformOperator transformOperator,
      List<OrderByExpressionContext> orderByExpressions) {
    _indexSegment = indexSegment;
    _expressions = expressions;
    _transformOperator = transformOperator;

    _groupByExpressions = queryContext.getGroupByExpressions();
    assert _groupByExpressions != null;
    _numGroupByExpressions = _groupByExpressions.size();
    _aggregationFunctions = queryContext.getAggregationFunctions();
    //TODO: assert orderBy is not null on the caller side
    _orderByExpressions = orderByExpressions;
    _numOrderByExpressions = _orderByExpressions.size();
    _orderByExpressionMetadata = new TransformResultMetadata[_numOrderByExpressions];
    for (int i = 0; i < _numOrderByExpressions; i++) {
      ExpressionContext expression = _orderByExpressions.get(i).getExpression();
      _orderByExpressionMetadata[i] = _transformOperator.getResultMetadata(expression);
    }

    _rows = new PriorityQueue<>(ROW_LIMIT, getComparator());
    // TODO: Instead of object to int map, use primitive type map
    _groupByKeyMap = new HashMap<>();
  }

  private void createKeyMap() {
//    for (int i = 0; i < _numGroupByExpressions; i++) {
//      ExpressionContext groupByExpression = _groupByExpressions.get(i);
//      TransformResultMetadata transformResultMetadata = _transformOperator.getResultMetadata(groupByExpression);
//      _storedTypes[i] = transformResultMetadata.getDataType().getStoredType();
//      if (transformResultMetadata.hasDictionary()) {
//        _dictionaries[i] = transformOperator.getDictionary(groupByExpression);
//      } else {
//        _onTheFlyDictionaries[i] = ValueToIdMapFactory.get(_storedTypes[i]);
//      }
//      _isSingleValueExpressions[i] = transformResultMetadata.isSingleValue();
//    }
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

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    //Do it in two passes
    // Fetch the order-by expressions and docIds and insert them into the priority queue
    // // Schema: orderBy expressions | groupBy expressions | docID
    BlockValSet[] blockValSets = new BlockValSet[_numOrderByExpressions + _numGroupByExpressions + 1];
    TransformBlock transformBlock;
    while ((transformBlock = _transformOperator.nextBlock()) != null) {
      for (int i = 0; i < _numOrderByExpressions; i++) {
        ExpressionContext expression = _orderByExpressions.get(i).getExpression();
        blockValSets[i] = transformBlock.getBlockValueSet(expression);
      }
      for (int i = 0; i < _numGroupByExpressions; i++) {
        ExpressionContext expression = _groupByExpressions.get(i);
        blockValSets[_numOrderByExpressions + i] = transformBlock.getBlockValueSet(expression);
      }
      blockValSets[_numOrderByExpressions + _numGroupByExpressions] =
          transformBlock.getBlockValueSet(CommonConstants.Segment.BuiltInVirtualColumn.DOCID);
      RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);
      int numDocsFetched = transformBlock.getNumDocs();
      for (int i = 0; i < numDocsFetched; i++) {
        // TODO: Decide if we pre-allocate everything or do it separately
        Object[] row = new Object[_numOrderByExpressions + _numGroupByExpressions + 1];
        blockValueFetcher.getRow(i, row, 0);
        AddToPriorityQueue(row);
      }
    }

    int numRows = _rows.size();
    List<Object[]> rowList = new ArrayList<>(numRows);
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    for (Map.Entry<Object[], List<Integer>> entry : _groupByKeyMap.entrySet()) {
      List<Integer> filteredDocIds = entry.getValue();
      for (Integer docId : filteredDocIds) {
        docIds.add(docId);
      }
    }

    // Make a new transform operator
    Set<ExpressionContext> expressionsToTransform =
        AggregationFunctionUtils.collectExpressionsToTransform(_aggregationFunctions, _groupByExpressions.toArray(new ExpressionContext[0]));
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
    TransformOperator transformOperator = new TransformOperator(projectionOperator, expressionsToTransform);

    // TODO: handle star tree
    AggregationGroupByOrderByOperator operator = new AggregationGroupByOrderByOperator(_aggregationFunctions, _groupByExpressions,
       _maxInitialResultHolderCapacity, _numGroupsLimit, _minSegmentTrimSize, transformOperator, numTotalDocs,
       _queryContext, false);

    return operator.getNextBlock();
  }

  private void AddToPriorityQueue(Object[] row) {
    // PQ: row: orderBy experssion | groupBy expression | docID

    // groupBy column: 000 001 000
    if (_rows.size() < ROW_LIMIT) {
      Object[] groupByKeys =
          Arrays.copyOfRange(row, _numOrderByExpressions, _numOrderByExpressions + _numGroupByExpressions);
      int docID = (int) row[_numOrderByExpressions + _numGroupByExpressions];
      if (AddToMap(groupByKeys, docID)) {
        _rows.add(row);
      }
    } else if (_rows.comparator().compare(_rows.peek(), row) <= 0) {
      Object[] groupByKeys =
          Arrays.copyOfRange(row, _numOrderByExpressions, _numOrderByExpressions + _numGroupByExpressions);
      int docID = (int) row[_numOrderByExpressions + _numGroupByExpressions];
      if (AddToMap(groupByKeys, docID)) {
        Object[] removedRow = _rows.poll();
        Object[] removedGroupByKey =
            Arrays.copyOfRange(removedRow, _numOrderByExpressions, _numOrderByExpressions + _numGroupByExpressions);
        _groupByKeyMap.remove(removedGroupByKey);
        _rows.offer(row);
      }
    }
  }

  private boolean AddToMap(Object[] groupByKeys, int docID) {
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

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
