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
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import javax.annotation.Nonnull;


public class DistinctCountMVAggregationFunction extends DistinctCountAggregationFunction {

  @Nonnull
  @Override
  public String getName() {
    return AggregationFunctionFactory.DISTINCTCOUNT_MV_AGGREGATION_FUNCTION;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String[] columns) {
    return AggregationFunctionFactory.DISTINCTCOUNT_MV_AGGREGATION_FUNCTION + "_" + columns[0];
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    int[][] valuesArray = projectionBlockValSets[0].getMVHashCodeArray();
    IntOpenHashSet valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = new IntOpenHashSet();
      aggregationResultHolder.setValue(valueSet);
    }
    for (int i = 0; i < length; i++) {
      for (int value : valuesArray[i]) {
        valueSet.add(value);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    int[][] valuesArray = projectionBlockValSets[0].getMVHashCodeArray();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
      if (valueSet == null) {
        valueSet = new IntOpenHashSet();
        groupByResultHolder.setValueForKey(groupKey, valueSet);
      }
      for (int value : valuesArray[i]) {
        valueSet.add(value);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    int[][] valuesArray = projectionBlockValSets[0].getMVHashCodeArray();
    for (int i = 0; i < length; i++) {
      int[] values = valuesArray[i];
      for (int groupKey : groupKeysArray[i]) {
        IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
        if (valueSet == null) {
          valueSet = new IntOpenHashSet();
          groupByResultHolder.setValueForKey(groupKey, valueSet);
        }
        for (int value : values) {
          valueSet.add(value);
        }
      }
    }
  }
}
