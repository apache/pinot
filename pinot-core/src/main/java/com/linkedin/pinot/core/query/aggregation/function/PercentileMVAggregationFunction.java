/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import javax.annotation.Nonnull;


public class PercentileMVAggregationFunction extends PercentileAggregationFunction {
  private final String _name;

  public PercentileMVAggregationFunction(int percentile) {
    super(percentile);
    switch (percentile) {
      case 50:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILE50MV.getName();
        break;
      case 90:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILE90MV.getName();
        break;
      case 95:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILE95MV.getName();
        break;
      case 99:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILE99MV.getName();
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported percentile for PercentileAggregationFunction: " + percentile);
    }
  }

  @Nonnull
  @Override
  public String getName() {
    return _name;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String[] columns) {
    return _name + "_" + columns[0];
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    double[][] valuesArray = blockValSets[0].getDoubleValuesMV();
    DoubleArrayList doubleArrayList = aggregationResultHolder.getResult();
    if (doubleArrayList == null) {
      doubleArrayList = new DoubleArrayList();
      aggregationResultHolder.setValue(doubleArrayList);
    }
    for (int i = 0; i < length; i++) {
      for (double value : valuesArray[i]) {
        doubleArrayList.add(value);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[][] valuesArray = blockValSets[0].getDoubleValuesMV();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      DoubleArrayList doubleArrayList = groupByResultHolder.getResult(groupKey);
      if (doubleArrayList == null) {
        doubleArrayList = new DoubleArrayList();
        groupByResultHolder.setValueForKey(groupKey, doubleArrayList);
      }
      for (double value : valuesArray[i]) {
        doubleArrayList.add(value);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[][] valuesArray = blockValSets[0].getDoubleValuesMV();
    for (int i = 0; i < length; i++) {
      double[] values = valuesArray[i];
      for (int groupKey : groupKeysArray[i]) {
        DoubleArrayList doubleArrayList = groupByResultHolder.getResult(groupKey);
        if (doubleArrayList == null) {
          doubleArrayList = new DoubleArrayList();
          groupByResultHolder.setValueForKey(groupKey, doubleArrayList);
        }
        for (double value : values) {
          doubleArrayList.add(value);
        }
      }
    }
  }
}
