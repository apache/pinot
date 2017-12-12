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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.Collections;
import javax.annotation.Nonnull;


public class PercentileAggregationFunction implements AggregationFunction<DoubleArrayList, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  private final String _name;
  private final int _percentile;

  public PercentileAggregationFunction(int percentile) {
    switch (percentile) {
      case 50:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILE50.getName();
        break;
      case 90:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILE90.getName();
        break;
      case 95:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILE95.getName();
        break;
      case 99:
        _name = AggregationFunctionFactory.AggregationFunctionType.PERCENTILE99.getName();
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported percentile for PercentileAggregationFunction: " + percentile);
    }
    _percentile = percentile;
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
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity, trimSize);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    DoubleArrayList doubleArrayList = aggregationResultHolder.getResult();
    if (doubleArrayList == null) {
      doubleArrayList = new DoubleArrayList();
      aggregationResultHolder.setValue(doubleArrayList);
    }
    for (int i = 0; i < length; i++) {
      doubleArrayList.add(valueArray[i]);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      DoubleArrayList doubleArrayList = groupByResultHolder.getResult(groupKey);
      if (doubleArrayList == null) {
        doubleArrayList = new DoubleArrayList();
        groupByResultHolder.setValueForKey(groupKey, doubleArrayList);
      }
      doubleArrayList.add(valueArray[i]);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        DoubleArrayList doubleArrayList = groupByResultHolder.getResult(groupKey);
        if (doubleArrayList == null) {
          doubleArrayList = new DoubleArrayList();
          groupByResultHolder.setValueForKey(groupKey, doubleArrayList);
        }
        doubleArrayList.add(value);
      }
    }
  }

  @Nonnull
  @Override
  public DoubleArrayList extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    DoubleArrayList doubleArrayList = aggregationResultHolder.getResult();
    if (doubleArrayList == null) {
      return new DoubleArrayList();
    } else {
      return doubleArrayList;
    }
  }

  @Nonnull
  @Override
  public DoubleArrayList extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    DoubleArrayList doubleArrayList = groupByResultHolder.getResult(groupKey);
    if (doubleArrayList == null) {
      return new DoubleArrayList();
    } else {
      return doubleArrayList;
    }
  }

  @Nonnull
  @Override
  public DoubleArrayList merge(@Nonnull DoubleArrayList intermediateResult1,
      @Nonnull DoubleArrayList intermediateResult2) {
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getIntermediateResultDataType() {
    return FieldSpec.DataType.OBJECT;
  }

  @Nonnull
  @Override
  public Double extractFinalResult(@Nonnull DoubleArrayList intermediateResult) {
    int size = intermediateResult.size();
    if (size == 0) {
      return DEFAULT_FINAL_RESULT;
    } else {
      Collections.sort(intermediateResult);
      return intermediateResult.get((int) ((long) intermediateResult.size() * _percentile / 100));
    }
  }
}
