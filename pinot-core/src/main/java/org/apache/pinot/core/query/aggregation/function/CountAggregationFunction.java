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
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


public class CountAggregationFunction implements AggregationFunction<Long, Long> {
  private static final String COLUMN_NAME = AggregationFunctionType.COUNT.getName() + "_star";
  private static final double DEFAULT_INITIAL_VALUE = 0.0;

  protected final String _column;

  /**
   * Constructor for the class.
   * @param column Column name to aggregate on.
   */
  public CountAggregationFunction(String column) {
    _column = column;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.COUNT;
  }

  @Override
  public String getColumnName(String column) {
    return COLUMN_NAME;
  }

  @Override
  public String getResultColumnName(String column) {
    return AggregationFunctionType.COUNT.getName().toLowerCase() + "(*)";
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
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
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, Map<String, BlockValSet> blockValSetMap) {
    if (blockValSetMap.size() == 0) {
      aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + length);
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSetMap.get(_column).getLongValuesSV();
      long count = 0;
      for (int i = 0; i < length; i++) {
        count += valueArray[i];
      }
      aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + count);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<String, BlockValSet> blockValSetMap) {
    if (blockValSetMap.size() == 0) {
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
      }
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSetMap.get(_column).getLongValuesSV();
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<String, BlockValSet> blockValSetMap) {
    if (blockValSetMap.size() == 0) {
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
        }
      }
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSetMap.get(_column).getLongValuesSV();
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
  public boolean isIntermediateResultComparable() {
    return true;
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
