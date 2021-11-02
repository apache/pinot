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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * The DISTINCT clause in SQL is represented as the DISTINCT aggregation function. Currently it is only used to wrap the
 * information for the distinct queries.
 * TODO: Use a separate way to represent DISTINCT instead of aggregation.
 */
@SuppressWarnings("rawtypes")
public class DistinctAggregationFunction implements AggregationFunction<Object, Comparable> {
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
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
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
  public Object extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public Object extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public Object merge(Object intermediateResult1, Object intermediateResult2) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public Comparable extractFinalResult(Object intermediateResult) {
    throw new UnsupportedOperationException("Operation not supported for DISTINCT aggregation function");
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(getType().getName()).append('(');
    int numArguments = getInputExpressions().size();
    if (numArguments > 0) {
      stringBuilder.append(getInputExpressions().get(0).toString());
      for (int i = 1; i < numArguments; i++) {
        stringBuilder.append(", ").append(getInputExpressions().get(i).toString());
      }
    }
    return stringBuilder.append(')').toString();
  }
}
