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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.customobject.DistinctTable;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;


/**
 * The DISTINCT clause in SQL is executed as the DISTINCT aggregation function.
 * TODO: Support group-by
 */
@SuppressWarnings("rawtypes")
public class DistinctAggregationFunction implements AggregationFunction<DistinctTable, Comparable> {
  private final List<ExpressionContext> _expressions;
  private final String[] _columns;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final int _limit;

  /**
   * Constructor for the class.
   *
   * @param expressions Distinct columns to return
   * @param orderByExpressions Order By clause
   * @param limit Limit clause
   */
  public DistinctAggregationFunction(List<ExpressionContext> expressions,
      @Nullable List<OrderByExpressionContext> orderByExpressions, int limit) {
    _expressions = expressions;
    int numExpressions = expressions.size();
    _columns = new String[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      _columns[i] = expressions.get(i).toString();
    }
    _orderByExpressions = orderByExpressions;
    _limit = limit;
  }

  public String[] getColumns() {
    return _columns;
  }

  public List<OrderByExpressionContext> getOrderByExpressions() {
    return _orderByExpressions;
  }

  public int getLimit() {
    return _limit;
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
  public List<ExpressionContext> getInputExpressions() {
    return _expressions;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    int numBlockValSets = blockValSetMap.size();
    int numExpressions = _expressions.size();
    Preconditions
        .checkState(numBlockValSets == numExpressions, "Size mismatch: numBlockValSets = %s, numExpressions = %s",
            numBlockValSets, numExpressions);

    BlockValSet[] blockValSets = new BlockValSet[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      blockValSets[i] = blockValSetMap.get(_expressions.get(i));
    }

    DistinctTable distinctTable = aggregationResultHolder.getResult();
    if (distinctTable == null) {
      ColumnDataType[] columnDataTypes = new ColumnDataType[numExpressions];
      for (int i = 0; i < numExpressions; i++) {
        columnDataTypes[i] = ColumnDataType.fromDataTypeSV(blockValSetMap.get(_expressions.get(i)).getValueType());
      }
      DataSchema dataSchema = new DataSchema(_columns, columnDataTypes);
      distinctTable = new DistinctTable(dataSchema, _orderByExpressions, _limit);
      aggregationResultHolder.setValue(distinctTable);
    }

    // TODO: Follow up PR will make few changes to start using DictionaryBasedAggregationOperator for DISTINCT queries
    //       without filter.

    if (distinctTable.hasOrderBy()) {
      // With order-by, no need to check whether the DistinctTable is already satisfied
      RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);
      for (int i = 0; i < length; i++) {
        distinctTable.addWithOrderBy(new Record(blockValueFetcher.getRow(i)));
      }
    } else {
      // Without order-by, early-terminate when the DistinctTable is already satisfied
      if (distinctTable.isSatisfied()) {
        return;
      }
      RowBasedBlockValueFetcher blockValueFetcher = new RowBasedBlockValueFetcher(blockValSets);
      for (int i = 0; i < length; i++) {
        if (distinctTable.addWithoutOrderBy(new Record(blockValueFetcher.getRow(i)))) {
          return;
        }
      }
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
      return new DistinctTable(new DataSchema(_columns, columnDataTypes), _orderByExpressions, _limit);
    }
  }

  /**
   * NOTE: This method only handles merging of 2 main DistinctTables. It should not be used on Broker-side because it
   *       does not support merging deserialized DistinctTables.
   * <p>{@inheritDoc}
   */
  @Override
  public DistinctTable merge(DistinctTable intermediateResult1, DistinctTable intermediateResult2) {
    if (intermediateResult1.size() == 0) {
      return intermediateResult2;
    }
    if (intermediateResult2.size() != 0) {
      intermediateResult1.mergeMainDistinctTable(intermediateResult2);
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
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
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
