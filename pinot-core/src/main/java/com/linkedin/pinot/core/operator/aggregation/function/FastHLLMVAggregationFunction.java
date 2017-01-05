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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import javax.annotation.Nonnull;


public class FastHLLMVAggregationFunction extends FastHLLAggregationFunction {

  @Nonnull
  @Override
  public String getName() {
    return AggregationFunctionFactory.FASTHLL_MV_AGGREGATION_FUNCTION;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String[] columns) {
    return AggregationFunctionFactory.FASTHLL_MV_AGGREGATION_FUNCTION + "_" + columns[0];
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    String[][] valuesArray = projectionBlockValSets[0].getMultiValues();
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      hyperLogLog = new HyperLogLog(_log2m);
      aggregationResultHolder.setValue(hyperLogLog);
    }
    for (int i = 0; i < length; i++) {
      try {
        for (String value : valuesArray[i]) {
          hyperLogLog.addAll(HllUtil.convertStringToHll(value));
        }
      } catch (CardinalityMergeException e) {
        throw new RuntimeException("Caught exception while aggregating HyperLogLog.", e);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    String[][] valuesArray = projectionBlockValSets[0].getMultiValues();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
      if (hyperLogLog == null) {
        hyperLogLog = new HyperLogLog(_log2m);
        groupByResultHolder.setValueForKey(groupKey, hyperLogLog);
      }
      try {
        for (String value : valuesArray[i]) {
          hyperLogLog.addAll(HllUtil.convertStringToHll(value));
        }
      } catch (CardinalityMergeException e) {
        throw new RuntimeException("Caught exception while aggregating HyperLogLog.", e);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    String[][] valuesArray = projectionBlockValSets[0].getMultiValues();
    for (int i = 0; i < length; i++) {
      String[] values = valuesArray[i];
      for (int groupKey : groupKeysArray[i]) {
        HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
        if (hyperLogLog == null) {
          hyperLogLog = new HyperLogLog(_log2m);
          groupByResultHolder.setValueForKey(groupKey, hyperLogLog);
        }
        try {
          for (String value : values) {
            hyperLogLog.addAll(HllUtil.convertStringToHll(value));
          }
        } catch (CardinalityMergeException e) {
          throw new RuntimeException("Caught exception while aggregating HyperLogLog.", e);
        }
      }
    }
  }
}
