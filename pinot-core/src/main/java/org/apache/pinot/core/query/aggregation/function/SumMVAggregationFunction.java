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

import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


public class SumMVAggregationFunction extends SumAggregationFunction {

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.SUMMV;
  }

  @Override
  public String getColumnName(String column) {
    return AggregationFunctionType.SUMMV.getName() + "_" + column;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, BlockValSet... blockValSets) {
    double[][] valuesArray = blockValSets[0].getDoubleValuesMV();
    double sum = aggregationResultHolder.getDoubleResult();
    for (int i = 0; i < length; i++) {
      for (double value : valuesArray[i]) {
        sum += value;
      }
    }
    aggregationResultHolder.setValue(sum);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    double[][] valuesArray = blockValSets[0].getDoubleValuesMV();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      double sum = groupByResultHolder.getDoubleResult(groupKey);
      for (double value : valuesArray[i]) {
        sum += value;
      }
      groupByResultHolder.setValueForKey(groupKey, sum);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets) {
    double[][] valuesArray = blockValSets[0].getDoubleValuesMV();
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
