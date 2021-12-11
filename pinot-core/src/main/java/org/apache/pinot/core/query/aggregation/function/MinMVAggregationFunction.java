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

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class MinMVAggregationFunction extends MinAggregationFunction {

  public MinMVAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MINMV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[][] valuesArray = blockValSetMap.get(_expression).getDoubleValuesMV();
    double min = aggregationResultHolder.getDoubleResult();
    for (int i = 0; i < length; i++) {
      for (double value : valuesArray[i]) {
        if (value < min) {
          min = value;
        }
      }
    }
    aggregationResultHolder.setValue(min);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[][] valuesArray = blockValSetMap.get(_expression).getDoubleValuesMV();
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

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    double[][] valuesArray = blockValSetMap.get(_expression).getDoubleValuesMV();
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
