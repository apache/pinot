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


public class MaxAggregationFunction implements AggregationFunction<Double, Double> {
  private static final double DEFAULT_INITIAL_VALUE = Double.NEGATIVE_INFINITY;

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MAX;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.MAX.getName() + "_" + column;
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
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
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
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      int groupKey = groupKeyArray[i];
      if (value > groupByResultHolder.getDoubleResult(groupKey)) {
        groupByResultHolder.setValueForKey(groupKey, value);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        if (value > groupByResultHolder.getDoubleResult(groupKey)) {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
    }
  }

  @Nonnull
  @Override
  public Double extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getDoubleResult();
  }

  @Nonnull
  @Override
  public Double extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getDoubleResult(groupKey);
  }

  @Nonnull
  @Override
  public Double merge(@Nonnull Double intermediateResult1, @Nonnull Double intermediateResult2) {
    if (intermediateResult1 > intermediateResult2) {
      return intermediateResult1;
    } else {
      return intermediateResult2;
    }
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return true;
  }

  @Nonnull
  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE;
  }

  @Nonnull
  @Override
  public Double extractFinalResult(@Nonnull Double intermediateResult) {
    return intermediateResult;
  }
}
