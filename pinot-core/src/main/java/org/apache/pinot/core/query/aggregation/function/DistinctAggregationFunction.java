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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DistinctTable;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.util.GroupByUtils;


/**
 * The DISTINCT clause in SQL is executed as the DISTINCT aggregation function.
 * // TODO: Support group-by
 */
public class DistinctAggregationFunction implements AggregationFunction<DistinctTable, Comparable> {
  private final String[] _columns;
  private final List<SelectionSort> _orderBy;
  private final int _capacity;
  private final List<TransformExpressionTree> _inputExpressions;

  /**
   * Constructor for the class.
   *
   * @param columns Distinct columns to return
   * @param orderBy Order By clause
   * @param limit Limit clause
   */
  public DistinctAggregationFunction(List<String> columns, List<SelectionSort> orderBy, int limit) {
    int numColumns = columns.size();
    _columns = columns.toArray(new String[numColumns]);
    _orderBy = orderBy;
    // NOTE: DISTINCT with order-by is similar to group-by with order-by, where we limit the maximum number of unique
    //       records (groups) for each query to reduce the memory footprint. The result might not be 100% accurate in
    //       certain scenarios, but should give a good enough approximation.
    _capacity = CollectionUtils.isNotEmpty(_orderBy) ? GroupByUtils.getTableCapacity(limit) : limit;

    _inputExpressions = new ArrayList<>(numColumns);
    for (String column : columns) {
      _inputExpressions.add(TransformExpressionTree.compileToExpressionTree(column));
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCT;
  }

  @Override
  public String getColumnName() {
    return AggregationFunctionType.DISTINCT.getName() + "_" + AggregationFunctionUtils.concatArgs(_columns);
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.DISTINCT.getName().toLowerCase() + "(" + AggregationFunctionUtils
        .concatArgs(_columns) + ")";
  }

  @Override
  public List<TransformExpressionTree> getInputExpressions() {
    return _inputExpressions;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    int numBlockValSets = blockValSetMap.size();
    int numExpressions = _inputExpressions.size();
    Preconditions
        .checkState(numBlockValSets == numExpressions, "Size mismatch: numBlockValSets = %s, numExpressions = %s",
            numBlockValSets, numExpressions);

    BlockValSet[] blockValSets = new BlockValSet[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      blockValSets[i] = blockValSetMap.get(_inputExpressions.get(i));
    }

    DistinctTable distinctTable = aggregationResultHolder.getResult();
    if (distinctTable == null) {
      ColumnDataType[] columnDataTypes = new ColumnDataType[numExpressions];
      for (int i = 0; i < numExpressions; i++) {
        columnDataTypes[i] = ColumnDataType.fromDataTypeSV(blockValSetMap.get(_inputExpressions.get(i)).getValueType());
      }
      DataSchema dataSchema = new DataSchema(_columns, columnDataTypes);
      distinctTable = new DistinctTable(dataSchema, _orderBy, _capacity);
      aggregationResultHolder.setValue(distinctTable);
    }

    // TODO: Follow up PR will make few changes to start using DictionaryBasedAggregationOperator
    // for DISTINCT queries without filter.
    RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);

    // TODO: Do early termination in the operator itself which should
    // not call aggregate function at all if the limit has reached
    // that will require the interface change since this function
    // has to communicate back that required number of records have
    // been collected
    for (int i = 0; i < length; i++) {
      distinctTable.upsert(new Record(blockValueFetcher.getRow(i)));
    }
  }

  @Override
  public DistinctTable extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    DistinctTable distinctTable = aggregationResultHolder.getResult();
    if (distinctTable != null) {
      return distinctTable;
    } else {
      ColumnDataType[] columnDataTypes = new ColumnDataType[_columns.length];
      // NOTE: Use STRING for unknown type
      Arrays.fill(columnDataTypes, ColumnDataType.STRING);
      return new DistinctTable(new DataSchema(_columns, columnDataTypes), _orderBy, _capacity);
    }
  }

  @Override
  public DistinctTable merge(DistinctTable intermediateResult1, DistinctTable intermediateResult2) {
    if (intermediateResult1.size() == 0) {
      return intermediateResult2;
    }
    if (intermediateResult2.size() != 0) {
      intermediateResult1.merge(intermediateResult2);
    }
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public DistinctTable extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public Comparable extractFinalResult(DistinctTable intermediateResult) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }
}
