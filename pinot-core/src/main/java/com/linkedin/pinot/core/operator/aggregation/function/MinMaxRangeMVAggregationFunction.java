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
package com.linkedin.pinot.core.operator.aggregation.function;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.utils.Pair;
import java.util.List;


/**
 * Class to implement the 'minmaxrange' aggregation function.
 */
public class MinMaxRangeMVAggregationFunction implements AggregationFunction {
  private static final String FUNCTION_NAME = AggregationFunctionFactory.MINMAXRANGE_MV_AGGREGATION_FUNCTION;
  private static final ResultDataType RESULT_DATA_TYPE = ResultDataType.MINMAXRANGE_PAIR;

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  /**
   * Performs 'minmaxrange' aggregation on the input array.
   *
   * {@inheritDoc}
   *
   * @param length
   * @param resultHolder
   * @param valueArrayArray
   */
  @Override
  public void aggregate(int length, AggregationResultHolder resultHolder, Object... valueArrayArray) {
    Preconditions.checkArgument(valueArrayArray.length == 1);
    Preconditions.checkArgument(valueArrayArray[0] instanceof double[][]);
    final double[][] values = (double[][]) valueArrayArray[0];
    Preconditions.checkState(length <= values.length);

    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;

    for (int i = 0; i < length; i++) {
      for (int j = 0; j < values[i].length; ++j) {
        double value = values[i][j];
        if (value < min) {
          min = value;
        }
        if (value > max) {
          max = value;
        }
      }
    }

    Pair<Double, Double> rangeValue = resultHolder.getResult();
    if (rangeValue == null) {
      rangeValue = new Pair<>(min, max);
      resultHolder.setValue(rangeValue);
    } else {
      if (min < rangeValue.getFirst()) {
        rangeValue.setFirst(min);
      }
      if (max > rangeValue.getSecond()) {
        rangeValue.setSecond(max);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * While the interface allows for variable number of valueArrays, we do not support
   * multiple columns within one aggregation function right now.
   *
   * @param length
   * @param groupKeys
   * @param resultHolder
   * @param valueArray
   */
  @Override
  public void aggregateGroupBySV(int length, int[] groupKeys, GroupByResultHolder resultHolder, Object... valueArray) {
    Preconditions.checkArgument(valueArray.length == 1);
    Preconditions.checkArgument(valueArray[0] instanceof double[][]);
    final double[][] values = (double[][]) valueArray[0];
    Preconditions.checkState(length <= values.length);

    for (int i = 0; i < length; i++) {
      int groupKey = groupKeys[i];
      for (double value : values[i]) {
        Pair<Double, Double> rangeValue = resultHolder.getResult(groupKey);
        if (rangeValue == null) {
          rangeValue = new Pair<>(value, value);
          resultHolder.setValueForKey(groupKey, rangeValue);
        } else {
          if (value < rangeValue.getFirst()) {
            rangeValue.setFirst(value);
          }
          if (value > rangeValue.getSecond()) {
            rangeValue.setSecond(value);
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param length
   * @param docIdToGroupKeys
   * @param resultHolder
   * @param valueArray
   */
  @Override
  public void aggregateGroupByMV(int length, int[][] docIdToGroupKeys, GroupByResultHolder resultHolder,
      Object... valueArray) {
    Preconditions.checkArgument(valueArray.length == 1);
    Preconditions.checkArgument(valueArray[0] instanceof double[][]);
    final double[][] values = (double[][]) valueArray[0];
    Preconditions.checkState(length <= values.length);
    for (int i = 0; i < length; ++i) {
      for (double value : values[i]) {
        for (int groupKey : docIdToGroupKeys[i]) {
          Pair<Double, Double> rangeValue = resultHolder.getResult(groupKey);
          if (rangeValue == null) {
            rangeValue = new Pair<>(value, value);
            resultHolder.setValueForKey(groupKey, rangeValue);
          } else {
            if (value < rangeValue.getFirst()) {
              rangeValue.setFirst(value);
            }
            if (value > rangeValue.getSecond()) {
              rangeValue.setSecond(value);
            }
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public double getDefaultValue() {
    throw new RuntimeException("Unsupported method getDefaultValue() for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public ResultDataType getResultDataType() {
    return RESULT_DATA_TYPE;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  /**
   * {@inheritDoc}
   *
   * @param combinedResult
   * @return
   */
  @Override
  public Double reduce(List<Object> combinedResult) {
    throw new RuntimeException(
        "Unsupported method reduce(List<Object> combinedResult) for class " + getClass().getName());
  }
}
