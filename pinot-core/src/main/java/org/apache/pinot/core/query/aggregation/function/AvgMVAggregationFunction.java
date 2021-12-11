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

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class AvgMVAggregationFunction extends AvgAggregationFunction {

  public AvgMVAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.AVGMV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[][] valuesArray = blockValSetMap.get(_expression).getDoubleValuesMV();
    double sum = 0.0;
    long count = 0L;
    for (int i = 0; i < length; i++) {
      double[] values = valuesArray[i];
      for (double value : values) {
        sum += value;
      }
      count += values.length;
    }
    setAggregationResult(aggregationResultHolder, sum, count);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[][] valuesArray = blockValSetMap.get(_expression).getDoubleValuesMV();
    for (int i = 0; i < length; i++) {
      aggregateOnGroupKey(groupKeyArray[i], groupByResultHolder, valuesArray[i]);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[][] valuesArray = blockValSetMap.get(_expression).getDoubleValuesMV();
    for (int i = 0; i < length; i++) {
      double[] values = valuesArray[i];
      for (int groupKey : groupKeysArray[i]) {
        aggregateOnGroupKey(groupKey, groupByResultHolder, values);
      }
    }
  }

  private void aggregateOnGroupKey(int groupKey, GroupByResultHolder groupByResultHolder, double[] values) {
    double sum = 0.0;
    for (double value : values) {
      sum += value;
    }
    long count = values.length;
    setGroupByResult(groupKey, groupByResultHolder, sum, count);
  }
}
