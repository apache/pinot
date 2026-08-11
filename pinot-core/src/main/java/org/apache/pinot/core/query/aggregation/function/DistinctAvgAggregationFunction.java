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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Aggregation function to compute the average of distinct values for an SV column.
public class DistinctAvgAggregationFunction extends BaseDistinctAggregateAggregationFunction<Double> {

  public DistinctAvgAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    this(verifySingleArgument(arguments, "DISTINCT_AVG"), AggregationFunctionType.DISTINCTAVG, nullHandlingEnabled);
  }

  protected DistinctAvgAggregationFunction(ExpressionContext expression,
      AggregationFunctionType aggregationFunctionType, boolean nullHandlingEnabled) {
    super(expression, aggregationFunctionType, nullHandlingEnabled);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.isSingleValue()) {
      svAggregate(blockValSet, length, aggregationResultHolder);
    } else {
      mvAggregate(blockValSet, length, aggregationResultHolder);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.isSingleValue()) {
      svAggregateGroupBySV(blockValSet, length, groupKeyArray, groupByResultHolder);
    } else {
      mvAggregateGroupBySV(blockValSet, length, groupKeyArray, groupByResultHolder);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    if (blockValSet.isSingleValue()) {
      svAggregateGroupByMV(blockValSet, length, groupKeysArray, groupByResultHolder);
    } else {
      mvAggregateGroupByMV(blockValSet, length, groupKeysArray, groupByResultHolder);
    }
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE;
  }

  @Nullable
  @Override
  public Double extractFinalResult(@Nullable Set intermediateResult) {
    // A null intermediate result means nothing was aggregated, and so does an empty set, which is what a
    // deserialized peer can still carry. With null handling enabled the distinct average of nothing is NULL; with it
    // disabled it is the zero sum divided by a zero count, which is the NaN below.
    if (intermediateResult == null || intermediateResult.isEmpty()) {
      return _nullHandlingEnabled ? null : Double.NaN;
    }

    Double distinctSum = 0.0;

    for (Object obj : intermediateResult) {
      distinctSum += ((Number) obj).doubleValue();
    }
    Double distinctAvg = distinctSum / intermediateResult.size();
    return distinctAvg;
  }
}
