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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class MinMaxRangeMVAggregationFunction extends MinMaxRangeAggregationFunction {

  public MinMaxRangeMVAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "MIN_MAX_RANGE_MV"), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MINMAXRANGEMV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[][] valuesArray = blockValSet.getDoubleValuesMV();

    MinMaxRangePair minMax = new MinMaxRangePair();
    AtomicBoolean empty = new AtomicBoolean(true);
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (double value : valuesArray[i]) {
          minMax.apply(value);
          empty.set(false);
        }
      }
    });

    if (!empty.get()) {
      setAggregationResult(aggregationResultHolder, minMax.getMin(), minMax.getMax());
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[][] valuesArray = blockValSet.getDoubleValuesMV();

    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        aggregateOnGroupKey(groupKeyArray[i], groupByResultHolder, valuesArray[i]);
      }
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[][] valuesArray = blockValSet.getDoubleValuesMV();

    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        double[] values = valuesArray[i];
        for (int groupKey : groupKeysArray[i]) {
          aggregateOnGroupKey(groupKey, groupByResultHolder, values);
        }
      }
    });
  }

  private void aggregateOnGroupKey(int groupKey, GroupByResultHolder groupByResultHolder, double[] values) {
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    for (double value : values) {
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
    }
    setGroupByResult(groupKey, groupByResultHolder, min, max);
  }
}
