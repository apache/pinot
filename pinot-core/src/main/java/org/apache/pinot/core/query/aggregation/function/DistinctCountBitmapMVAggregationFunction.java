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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * The {@code DistinctCountBitmapMVAggregationFunction} calculates the number of distinct values for a given multi-value
 * expression using RoaringBitmap. The bitmap stores the actual values for {@code INT} expression, or hash code of the
 * values for other data types (values with the same hash code will only be counted once).
 */
public class DistinctCountBitmapMVAggregationFunction extends DistinctCountBitmapAggregationFunction {

  public DistinctCountBitmapMVAggregationFunction(List<ExpressionContext> arguments) {
    super(verifySingleArgument(arguments, "DISTINCT_COUNT_BITMAP_MV"));
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTBITMAPMV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    aggregateMV(length, aggregationResultHolder, blockValSet, blockValSet.getValueType().getStoredType());
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    aggregateMVGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet,
        blockValSet.getValueType().getStoredType());
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    aggregateMVGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet,
        blockValSet.getValueType().getStoredType());
  }
}
