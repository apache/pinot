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
import java.util.Iterator;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DistinctTable;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;


/**
 * The DISTINCT clause in SQL is executed as the DISTINCT aggregation function.
 * // TODO: Support group-by
 */
public class DistinctAggregationFunction implements AggregationFunction<DistinctTable, Comparable> {
  private final DistinctTable _distinctTable;
  private final String[] _columnNames;
  private final int _limit;

  private FieldSpec.DataType[] _dataTypes;

  DistinctAggregationFunction(String multiColumnExpression, int limit) {
    _distinctTable = new DistinctTable(limit);
    _columnNames = multiColumnExpression.split(FunctionCallAstNode.DISTINCT_MULTI_COLUMN_SEPARATOR);
    _limit = limit;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCT;
  }

  @Override
  public String getColumnName(String column) {
    return AggregationFunctionType.DISTINCT.getName() + "_" + column;
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
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, BlockValSet... blockValSets) {
    int numColumns = _columnNames.length;
    Preconditions.checkState(blockValSets.length == numColumns, "Size mismatch: numBlockValSets = %s, numColumns = %s",
        blockValSets.length, numColumns);

    if (_dataTypes == null) {
      _dataTypes = new FieldSpec.DataType[numColumns];
      for (int i = 0; i < numColumns; i++) {
        _dataTypes[i] = blockValSets[i].getValueType();
      }
      _distinctTable.setColumnNames(_columnNames);
      _distinctTable.setColumnTypes(_dataTypes);
    }

    // TODO: Follow up PR will make few changes to start using DictionaryBasedAggregationOperator
    // for DISTINCT queries without filter.
    RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);

    int rowIndex = 0;
    // TODO: Do early termination in the operator itself which should
    // not call aggregate function at all if the limit has reached
    // that will require the interface change since this function
    // has to communicate back that required number of records have
    // been collected
    while (rowIndex < length && _distinctTable.size() < _limit) {
      Object[] columnData = blockValueFetcher.getRow(rowIndex);
      _distinctTable.addKey(new Key(columnData));
      rowIndex++;
    }
  }

  @Override
  public DistinctTable extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _distinctTable;
  }

  @Override
  public DistinctTable merge(DistinctTable inProgressMergedResult, DistinctTable newResultToMerge) {
    // do the union
    Iterator<Key> iterator = newResultToMerge.getIterator();
    while (iterator.hasNext() && inProgressMergedResult.size() < _limit) {
      Key key = iterator.next();
      inProgressMergedResult.addKey(key);
    }
    return inProgressMergedResult;
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
    return ColumnDataType.OBJECT;
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
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
