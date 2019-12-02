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
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
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
  private DistinctTable _distinctTable;
  private final String[] _columnNames;
  private final int _limit;
  private final List<SelectionSort> _orderBy;

  private FieldSpec.DataType[] _dataTypes;

  DistinctAggregationFunction(String multiColumnExpression, int limit, List<SelectionSort> orderBy) {
    _columnNames = multiColumnExpression.split(FunctionCallAstNode.DISTINCT_MULTI_COLUMN_SEPARATOR);
    _orderBy = orderBy;
    // use a multiplier for trim size when DISTINCT queries have ORDER BY. This logic
    // is similar to what we have in GROUP BY with ORDER BY
    // this does not guarantee 100% accuracy but still takes closer to it
    _limit = CollectionUtils.isNotEmpty(_orderBy) ? limit * 5 : limit;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCT;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  private ColumnDataType[] fieldSpecTypeToColumnTypes() {
    int numColumns = _dataTypes.length;
    ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
    for (int i = 0; i < numColumns; i++) {
      switch (_dataTypes[i]) {
        case INT:
          columnDataTypes[i] = ColumnDataType.INT;
          break;
        case LONG:
          columnDataTypes[i] = ColumnDataType.LONG;
          break;
        case FLOAT:
          columnDataTypes[i] = ColumnDataType.FLOAT;
          break;
        case DOUBLE:
          columnDataTypes[i] = ColumnDataType.DOUBLE;
          break;
        case STRING:
          columnDataTypes[i] = ColumnDataType.STRING;
          break;
        case BYTES:
          columnDataTypes[i] = ColumnDataType.BYTES;
          break;
        default:
          throw new UnsupportedOperationException("DISTINCT currently does not support type: " + _dataTypes[i]);
      }
    }
    return columnDataTypes;
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
      ColumnDataType[] columnDataTypes =  fieldSpecTypeToColumnTypes();
      DataSchema dataSchema = new DataSchema(_columnNames, columnDataTypes);
      _distinctTable = new DistinctTable(dataSchema, _orderBy, _limit);
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
    while (rowIndex < length) {
      Object[] columnData = blockValueFetcher.getRow(rowIndex);
      Record record = new Record(columnData);
      _distinctTable.upsert(record);
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
    Iterator<Record> iterator = newResultToMerge.iterator();
    while (iterator.hasNext()) {
      Record record = iterator.next();
      inProgressMergedResult.upsert(record);
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
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
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
