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
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.ObjectGroupByResultHolder;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import com.linkedin.pinot.core.startree.hll.HllConstants;
import javax.annotation.Nonnull;


public class DistinctCountHLLAggregationFunction implements AggregationFunction<HyperLogLog, Long> {
  private static final String NAME = AggregationFunctionFactory.AggregationFunctionType.DISTINCTCOUNTHLL.getName();

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
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity, trimSize);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    int[] valueArray = projectionBlockValSets[0].getSVHashCodeArray();
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      hyperLogLog = new HyperLogLog(HllConstants.DEFAULT_LOG2M);
      aggregationResultHolder.setValue(hyperLogLog);
    }
    for (int i = 0; i < length; i++) {
      hyperLogLog.offer(valueArray[i]);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    int[] valueArray = projectionBlockValSets[0].getSVHashCodeArray();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
      if (hyperLogLog == null) {
        hyperLogLog = new HyperLogLog(HllConstants.DEFAULT_LOG2M);
        groupByResultHolder.setValueForKey(groupKey, hyperLogLog);
      }
      hyperLogLog.offer(valueArray[i]);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    int[] valueArray = projectionBlockValSets[0].getSVHashCodeArray();
    for (int i = 0; i < length; i++) {
      int value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
        if (hyperLogLog == null) {
          hyperLogLog = new HyperLogLog(HllConstants.DEFAULT_LOG2M);
          groupByResultHolder.setValueForKey(groupKey, hyperLogLog);
        }
        hyperLogLog.offer(value);
      }
    }
  }

  @Nonnull
  @Override
  public HyperLogLog extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    HyperLogLog hyperLogLog = aggregationResultHolder.getResult();
    if (hyperLogLog == null) {
      return new HyperLogLog(HllConstants.DEFAULT_LOG2M);
    } else {
      return hyperLogLog;
    }
  }

  @Nonnull
  @Override
  public HyperLogLog extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    HyperLogLog hyperLogLog = groupByResultHolder.getResult(groupKey);
    if (hyperLogLog == null) {
      return new HyperLogLog(HllConstants.DEFAULT_LOG2M);
    } else {
      return hyperLogLog;
    }
  }

  @Nonnull
  @Override
  public HyperLogLog merge(@Nonnull HyperLogLog intermediateResult1, @Nonnull HyperLogLog intermediateResult2) {
    try {
      intermediateResult1.addAll(intermediateResult2);
    } catch (CardinalityMergeException e) {
      throw new RuntimeException("Caught exception while merging HyperLogLog.", e);
    }
    return intermediateResult1;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getIntermediateResultDataType() {
    return FieldSpec.DataType.OBJECT;
  }

  @Nonnull
  @Override
  public Long extractFinalResult(@Nonnull HyperLogLog intermediateResult) {
    return intermediateResult.cardinality();
  }
}
