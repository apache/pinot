/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.operator.groupby.ResultHolder;
import com.linkedin.pinot.core.query.utils.Pair;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.List;


/**
 * Class to implement the 'range' aggregation function.
 */
public class RangeAggregationFunction implements AggregationFunction {
  private static final String FUNCTION_NAME = "minmaxrange";
  private static final ResultDataType RESULT_DATA_TYPE = ResultDataType.RANGE_PAIR;

  /**
   * Performs 'range' aggregation on the input array.
   *
   * {@inheritDoc}
   *
   * @param values
   * @return
   */
  @Override
  public double aggregate(double[] values) {
    throw new RuntimeException("Unsupported method aggregate(double[] values) for class " + getClass().getName());
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
  public void apply(int length, int[] groupKeys, ResultHolder resultHolder, double[]... valueArray) {
    Preconditions.checkState(length <= valueArray[0].length);

    for (int i = 0; i < length; i++) {
      int groupKey = groupKeys[i];
      Pair<Double, Double> rangeValue = (Pair<Double, Double>) resultHolder.getResult(groupKey);
      double value = valueArray[0][i];

      if (rangeValue == null) {
        rangeValue =
            new com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction.MinMaxRangePair(value,
                value);
      } else {
        if (value < rangeValue.getFirst()) {
          rangeValue.setFirst(value);
        }
        if (value > rangeValue.getSecond()) {
          rangeValue.setSecond(value);
        }
      }
      resultHolder.putValueForKey(groupKey, rangeValue);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param length
   * @param valueArrayIndexToGroupKeys
   * @param resultHolder
   * @param valueArray
   */
  @Override
  public void apply(int length, Int2ObjectOpenHashMap valueArrayIndexToGroupKeys, ResultHolder resultHolder,
      double[] valueArray) {
    Preconditions.checkState(length <= valueArray.length);

    for (int i = 0; i < length; ++i) {
      long[] groupKeys = (long[]) valueArrayIndexToGroupKeys.get(i);
      double value = valueArray[i];

      for (long groupKey : groupKeys) {
        Pair<Double, Double> rangeValue = (Pair<Double, Double>) resultHolder.getResult(groupKey);

        if (rangeValue == null) {
          rangeValue =
              new com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction.MinMaxRangePair(
                  value, value);
        } else {
          if (value < rangeValue.getFirst()) {
            rangeValue.setFirst(value);
          }
          if (value > rangeValue.getSecond()) {
            rangeValue.setSecond(value);
          }
        }
        resultHolder.putValueForKey(groupKey, rangeValue);
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
        "Unsupported method reduce(List<Object> combinedResult for class " + getClass().getName());
  }
}
