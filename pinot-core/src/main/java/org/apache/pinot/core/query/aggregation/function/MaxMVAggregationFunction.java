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


public class MaxMVAggregationFunction extends MaxAggregationFunction {

  public MaxMVAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifySingleArgument(arguments, "MAX_MV"), nullHandlingEnabled);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MAXMV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    double[][] valuesArray = blockValSet.getDoubleValuesMV();
    Double max = foldNotNull(length, blockValSet, null, (acum, from, to) -> {
      double innerMax = DEFAULT_INITIAL_VALUE;
      for (int i = from; i < to; i++) {
        double[] values = valuesArray[i];
        for (double value : values) {
          if (value > innerMax) {
            innerMax = value;
          }
        }
      }
      return acum == null ? innerMax : Math.max(acum, innerMax);
    });

    updateAggregationResultHolder(aggregationResultHolder, max);
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
          Double max = groupByResultHolder.getResult(groupKey);
          for (double value : valuesArray[i]) {
            if (max == null || value > max) {
              max = value;
            }
          }
          groupByResultHolder.setValueForKey(groupKey, max);
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        int groupKey = groupKeyArray[i];
        double max = groupByResultHolder.getDoubleResult(groupKey);
        for (double value : valuesArray[i]) {
          if (value > max) {
            max = value;
          }
        }
        groupByResultHolder.setValueForKey(groupKey, max);
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
          Double max = null;
          for (double value : valuesArray[i]) {
            if (max == null || value > max) {
              max = value;
            }
          }

          for (int groupKey : groupKeysArray[i]) {
            Double currentMax = groupByResultHolder.getResult(groupKey);
            if (currentMax == null || (max != null && max > currentMax)) {
              groupByResultHolder.setValueForKey(groupKey, max);
            }
          }
        }
      });
    } else {
      for (int i = 0; i < length; i++) {
        double[] values = valuesArray[i];
        for (int groupKey : groupKeysArray[i]) {
          double max = groupByResultHolder.getDoubleResult(groupKey);
          for (double value : values) {
            if (value > max) {
              max = value;
            }
          }
          groupByResultHolder.setValueForKey(groupKey, max);
        }
      }
    }
  }
}
