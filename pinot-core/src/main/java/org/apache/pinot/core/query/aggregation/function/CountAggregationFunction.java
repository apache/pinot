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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;


public class CountAggregationFunction implements AggregationFunction<Long, Long> {
  private static final String COLUMN_NAME = "count_star";
  private static final String RESULT_COLUMN_NAME = "count(*)";
  private static final double DEFAULT_INITIAL_VALUE = 0.0;
  // Special expression used by star-tree to pass in BlockValSet
  private static final ExpressionContext STAR_TREE_COUNT_STAR_EXPRESSION =
      ExpressionContext.forIdentifier(AggregationFunctionColumnPair.STAR);

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.COUNT;
  }

  @Override
  public String getColumnName() {
    return COLUMN_NAME;
  }

  @Override
  public String getResultColumnName() {
    return RESULT_COLUMN_NAME;
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return Collections.emptyList();
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
    if (blockValSetMap.isEmpty()) {
      aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + length);
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSetMap.get(STAR_TREE_COUNT_STAR_EXPRESSION).getLongValuesSV();
      long count = 0;
      for (int i = 0; i < length; i++) {
        count += valueArray[i];
      }
      aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + count);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (blockValSetMap.isEmpty()) {
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
      }
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSetMap.get(STAR_TREE_COUNT_STAR_EXPRESSION).getLongValuesSV();
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (blockValSetMap.isEmpty()) {
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
        }
      }
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSetMap.get(STAR_TREE_COUNT_STAR_EXPRESSION).getLongValuesSV();
      for (int i = 0; i < length; i++) {
        long value = valueArray[i];
        for (int groupKey : groupKeysArray[i]) {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + value);
        }
      }
    }
  }

  @Override
  public Long extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return (long) aggregationResultHolder.getDoubleResult();
  }

  @Override
  public Long extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return (long) groupByResultHolder.getDoubleResult(groupKey);
  }

  @Override
  public Long merge(Long intermediateResult1, Long intermediateResult2) {
    return intermediateResult1 + intermediateResult2;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Override
  public Long extractFinalResult(Long intermediateResult) {
    return intermediateResult;
  }
}
