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


public class SumMVAggregationFunction extends SumAggregationFunction {

  public SumMVAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "SUM_MV"), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.SUMMV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[][] valuesArray = blockValSet.getDoubleValuesMV();

    Double sum;
    sum = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
      double innerSum = 0;
      for (int i = from; i < to; i++) {
        for (double value : valuesArray[i]) {
          innerSum += value;
        }
      }
      return acum == null ? innerSum : acum + innerSum;
    });

    updateAggregationResultHolder(aggregationResultHolder, sum);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[][] valuesArray = blockValSet.getDoubleValuesMV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          int groupKey = groupKeyArray[i];
          if (valuesArray[i].length > 0) {
            // "i" has to be non-null here so we can use the default value as the initial value instead of null
            double sum = DEFAULT_VALUE;
            for (double value : valuesArray[i]) {
              sum += value;
            }
            Double result = groupByResultHolder.getResult(groupKey);
            groupByResultHolder.setValueForKey(groupKey, result == null ? sum : result + sum);
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        double sum = groupByResultHolder.getDoubleResult(groupKey);
        for (double value : valuesArray[i]) {
          sum += value;
        }
        groupByResultHolder.setValueForKey(groupKey, sum);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[][] valuesArray = blockValSet.getDoubleValuesMV();

    if (_nullHandlingEnabled) {
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          double[] values = valuesArray[i];
          if (values.length > 0) {
            // "i" has to be non-null here so we can use the default value as the initial value instead of null
            double sum = DEFAULT_VALUE;
            for (double value : values) {
              sum += value;
            }
            for (int groupKey : groupKeysArray[i]) {
              Double result = groupByResultHolder.getResult(groupKey);
              groupByResultHolder.setValueForKey(groupKey, result == null ? sum : result + sum);
            }
          }
        }
      });
    } else {
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
}
