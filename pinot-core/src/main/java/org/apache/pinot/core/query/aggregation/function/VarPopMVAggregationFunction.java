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

import javax.annotation.Nonnull;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;

/**
 * Mutli-value aggregation of the population standard variance.
 *
 */
public class VarPopMVAggregationFunction extends VarPopAggregationFunction {

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.VARPOPMV;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String column) {
    return AggregationFunctionType.VARPOPMV.getName() + "_" + column;
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    double[][] valuesArray = blockValSets[0].getDoubleValuesMV();
    double sum2 = 0.0;
    double sum = 0.0;
    long count = 0L;
    for (int i = 0; i < length; i++) {
      double[] values = valuesArray[i];
      for (double value : values) {
        sum2 += Math.pow(value, 2);
        sum += value;
      }
      count += values.length;
    }
    setAggregationResult(aggregationResultHolder, sum2, sum, count);
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[][] valuesArray = blockValSets[0].getDoubleValuesMV();
    for (int i = 0; i < length; i++) {
      aggregateOnGroupKey(groupKeyArray[i], groupByResultHolder, valuesArray[i]);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[][] valuesArray = blockValSets[0].getDoubleValuesMV();
    for (int i = 0; i < length; i++) {
      double[] values = valuesArray[i];
      for (int groupKey : groupKeysArray[i]) {
        aggregateOnGroupKey(groupKey, groupByResultHolder, values);
      }
    }
  }

  private void aggregateOnGroupKey(int groupKey, @Nonnull GroupByResultHolder groupByResultHolder, double[] values) {
    double sum2 = 0.0;
    double sum = 0.0;
    for (double value : values) {
      sum2 += Math.pow(value, 2);
      sum += value;
    }
    long count = values.length;
    setGroupByResult(groupKey, groupByResultHolder, sum2, sum, count);
  }
}
