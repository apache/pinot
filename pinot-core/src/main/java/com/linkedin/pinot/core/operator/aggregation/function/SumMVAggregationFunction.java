/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.aggregation.function;

import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import javax.annotation.Nonnull;


public class SumMVAggregationFunction extends SumAggregationFunction {

  @Nonnull
  @Override
  public String getName() {
    return AggregationFunctionFactory.SUM_MV_AGGREGATION_FUNCTION;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String[] columns) {
    return AggregationFunctionFactory.SUM_MV_AGGREGATION_FUNCTION + "_" + columns[0];
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    double[][] valuesArray = projectionBlockValSets[0].getMultiValues();
    double sum = aggregationResultHolder.getDoubleResult();
    for (int i = 0; i < length; i++) {
      for (double value : valuesArray[i]) {
        sum += value;
      }
    }
    aggregationResultHolder.setValue(sum);
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    double[][] valuesArray = projectionBlockValSets[0].getMultiValues();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      double sum = groupByResultHolder.getDoubleResult(groupKey);
      for (double value : valuesArray[i]) {
        sum += value;
      }
      groupByResultHolder.setValueForKey(groupKey, sum);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    double[][] valuesArray = projectionBlockValSets[0].getMultiValues();
    for (int i = 0; i < length; i++) {
      double[] values = valuesArray[i];
      for (int groupKey : groupKeysArray[i]) {
        double sum = groupByResultHolder.getDoubleResult(groupKey);
        for (double value : values) {
          sum += value;
        }
        groupByResultHolder.setValueForKey(groupKey, sum);
      }
    }
  }
}
