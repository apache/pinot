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
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.query.spi.data.BlockValSet;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.DataSchema.ColumnDataType;


public class MaxAggregationFunction extends BaseSingleInputAggregationFunction<Double, Double> {
  private static final double DEFAULT_INITIAL_VALUE = Double.NEGATIVE_INFINITY;

  public MaxAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MAX;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new DoubleAggregationResultHolder(DEFAULT_INITIAL_VALUE);
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new DoubleGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_INITIAL_VALUE);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] valueArray = blockValSetMap.get(_expression).getDoubleValuesSV();
    double max = aggregationResultHolder.getDoubleResult();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      if (value > max) {
        max = value;
      }
    }
    aggregationResultHolder.setValue(max);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] valueArray = blockValSetMap.get(_expression).getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      int groupKey = groupKeyArray[i];
      if (value > groupByResultHolder.getDoubleResult(groupKey)) {
        groupByResultHolder.setValueForKey(groupKey, value);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[] valueArray = blockValSetMap.get(_expression).getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        if (value > groupByResultHolder.getDoubleResult(groupKey)) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    }
  }

  @Override
  public Double extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getDoubleResult();
  }

  @Override
  public Double extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getDoubleResult(groupKey);
  }

  @Override
  public Double merge(Double intermediateResult1, Double intermediateResult2) {
    if (intermediateResult1 > intermediateResult2) {
      return intermediateResult1;
    } else {
      return intermediateResult2;
    }
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(Double intermediateResult) {
    return intermediateResult;
  }
}
