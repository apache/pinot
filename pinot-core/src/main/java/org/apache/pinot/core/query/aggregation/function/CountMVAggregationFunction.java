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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


public class CountMVAggregationFunction extends CountAggregationFunction {
  private final String _column;
  private final TransformExpressionTree _expression;

  /**
   * Constructor for the class.
   *
   * @param column Column to aggregate on (could be column name or transform function).
   */
  public CountMVAggregationFunction(String column) {
    _column = column;
    _expression = TransformExpressionTree.compileToExpressionTree(column);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.COUNTMV;
  }

  @Override
  public String getColumnName() {
    return AggregationFunctionType.COUNTMV.getName() + "_" + _column;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.COUNTMV.getName().toLowerCase() + "(" + _column + ")";
  }

  @Override
  public List<TransformExpressionTree> getInputExpressions() {
    return Collections.singletonList(_expression);
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    int[] valueArray = blockValSetMap.get(_expression).getNumMVEntries();
    long count = 0L;
    for (int i = 0; i < length; i++) {
      count += valueArray[i];
    }
    aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + count);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    int[] valueArray = blockValSetMap.get(_expression).getNumMVEntries();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<TransformExpressionTree, BlockValSet> blockValSetMap) {
    int[] valueArray = blockValSetMap.get(_expression).getNumMVEntries();
    for (int i = 0; i < length; i++) {
      int value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + value);
      }
    }
  }
}
