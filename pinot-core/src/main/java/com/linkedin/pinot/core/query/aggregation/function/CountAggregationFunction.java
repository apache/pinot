/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import javax.annotation.Nonnull;


public class CountAggregationFunction implements AggregationFunction<Long, Long> {
  private static final String COLUMN_NAME = AggregationFunctionType.COUNT.getName() + "_star";
  private static final double DEFAULT_INITIAL_VALUE = 0.0;

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.COUNT;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return COLUMN_NAME;
  }

  @Override
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new DoubleAggregationResultHolder(DEFAULT_INITIAL_VALUE);
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new DoubleGroupByResultHolder(initialCapacity, maxCapacity, DEFAULT_INITIAL_VALUE);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    if (blockValSets.length == 0) {
      aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + length);
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSets[0].getLongValuesSV();
      long count = 0;
      for (int i = 0; i < length; i++) {
        count += valueArray[i];
      }
      aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + count);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    if (blockValSets.length == 0) {
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
      }
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSets[0].getLongValuesSV();
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    if (blockValSets.length == 0) {
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
        }
      }
    } else {
      // Star-tree pre-aggregated values
      long[] valueArray = blockValSets[0].getLongValuesSV();
      for (int i = 0; i < length; i++) {
        long value = valueArray[i];
        for (int groupKey : groupKeysArray[i]) {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + value);
        }
      }
    }
  }

  @Nonnull
  @Override
  public Long extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    return (long) aggregationResultHolder.getDoubleResult();
  }

  @Nonnull
  @Override
  public Long extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    return (long) groupByResultHolder.getDoubleResult(groupKey);
  }

  @Nonnull
  @Override
  public Long merge(@Nonnull Long intermediateResult1, @Nonnull Long intermediateResult2) {
    return intermediateResult1 + intermediateResult2;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return true;
  }

  @Nonnull
  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.LONG;
  }

  @Nonnull
  @Override
  public Long extractFinalResult(@Nonnull Long intermediateResult) {
    return intermediateResult;
  }
}
