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
package com.linkedin.pinot.core.query.aggregation.function;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import javax.annotation.Nonnull;


public class FastHLLMVAggregationFunction extends FastHLLAggregationFunction {
  private static final String NAME = AggregationFunctionFactory.AggregationFunctionType.FASTHLLMV.getName();

  @Nonnull
  @Override
  public String getName() {
    return NAME;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String[] columns) {
    return NAME + "_" + columns[0];
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    String[][] valuesArray = blockValSets[0].getStringValuesMV();
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
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    String[][] valuesArray = blockValSets[0].getStringValuesMV();
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
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    String[][] valuesArray = blockValSets[0].getStringValuesMV();
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
