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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class CountMVAggregationFunction extends CountAggregationFunction {

  public CountMVAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "COUNT_MV"), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.COUNTMV;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.COUNTMV.getName().toLowerCase() + "(" + _expression + ")";
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return Collections.singletonList(_expression);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    int[] valueArray = blockValSet.getNumMVEntries();
    // Hack to make count effectively final for use in the lambda (we know that there aren't concurrent access issues
    // with forEachNotNull)
    final long[] count = {0L};
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        count[0] += valueArray[i];
      }
    });
    aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + count[0]);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    int[] valueArray = blockValSet.getNumMVEntries();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        int groupKey = groupKeyArray[i];
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
      }
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    int[] valueArray = blockValSet.getNumMVEntries();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        int[] groupKeys = groupKeysArray[i];
        for (int groupKey : groupKeys) {
          groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + valueArray[i]);
        }
      }
    });
  }
}
