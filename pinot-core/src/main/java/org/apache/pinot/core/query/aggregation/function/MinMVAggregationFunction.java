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


public class MinMVAggregationFunction extends MinAggregationFunction {

  public MinMVAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "MIN_MV"), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MINMV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[][] valuesArray = blockValSet.getDoubleValuesMV();
    Double min = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
      double innerMin = DEFAULT_VALUE;
      for (int i = from; i < to; i++) {
        double[] values = valuesArray[i];
        for (double value : values) {
          if (value < innerMin) {
            innerMin = value;
          }
        }
      }
      return acum == null ? innerMin : Math.min(acum, innerMin);
    });

    updateAggregationResultHolder(aggregationResultHolder, min);
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
          Double min = groupByResultHolder.getResult(groupKey);
          for (double value : valuesArray[i]) {
            if (min == null || value < min) {
              min = value;
            }
          }
          groupByResultHolder.setValueForKey(groupKey, min);
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        double min = groupByResultHolder.getDoubleResult(groupKey);
        for (double value : valuesArray[i]) {
          if (value < min) {
            min = value;
          }
        }
        groupByResultHolder.setValueForKey(groupKey, min);
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
          Double min = null;
          for (double value : valuesArray[i]) {
            if (min == null || value < min) {
              min = value;
            }
          }

          for (int groupKey : groupKeysArray[i]) {
            Double currentMin = groupByResultHolder.getResult(groupKey);
            if (currentMin == null || (min != null && min < currentMin)) {
              groupByResultHolder.setValueForKey(groupKey, min);
            }
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        double[] values = valuesArray[i];
        for (int groupKey : groupKeysArray[i]) {
          double min = groupByResultHolder.getDoubleResult(groupKey);
          for (double value : values) {
            if (value < min) {
              min = value;
            }
          }
          groupByResultHolder.setValueForKey(groupKey, min);
        }
      }
    }
  }
}
