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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.ObjectGroupByResultHolder;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import javax.annotation.Nonnull;


public class DistinctCountAggregationFunction implements AggregationFunction<IntOpenHashSet, Integer> {
  private static final String NAME = AggregationFunctionFactory.AggregationFunctionType.DISTINCTCOUNT.getName();

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
    IntOpenHashSet valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = new IntOpenHashSet();
      aggregationResultHolder.setValue(valueSet);
    }
    for (int i = 0; i < length; i++) {
      valueSet.add(valueArray[i]);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    int[] valueArray = projectionBlockValSets[0].getSVHashCodeArray();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
      if (valueSet == null) {
        valueSet = new IntOpenHashSet();
        groupByResultHolder.setValueForKey(groupKey, valueSet);
      }
      valueSet.add(valueArray[i]);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    int[] valueArray = projectionBlockValSets[0].getSVHashCodeArray();
    for (int i = 0; i < length; i++) {
      int value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
        if (valueSet == null) {
          valueSet = new IntOpenHashSet();
          groupByResultHolder.setValueForKey(groupKey, valueSet);
        }
        valueSet.add(value);
      }
    }
  }

  @Nonnull
  @Override
  public IntOpenHashSet extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    IntOpenHashSet valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      return new IntOpenHashSet();
    } else {
      return valueSet;
    }
  }

  @Nonnull
  @Override
  public IntOpenHashSet extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      return new IntOpenHashSet();
    } else {
      return valueSet;
    }
  }

  @Nonnull
  @Override
  public IntOpenHashSet merge(@Nonnull IntOpenHashSet intermediateResult1,
      @Nonnull IntOpenHashSet intermediateResult2) {
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getIntermediateResultDataType() {
    return FieldSpec.DataType.OBJECT;
  }

  @Nonnull
  @Override
  public Integer extractFinalResult(@Nonnull IntOpenHashSet intermediateResult) {
    return intermediateResult.size();
  }
}
