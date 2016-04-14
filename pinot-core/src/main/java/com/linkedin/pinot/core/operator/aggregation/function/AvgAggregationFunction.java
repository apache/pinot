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
 * Class to implement the 'avg' aggregation function.
 */
public class AvgAggregationFunction implements AggregationFunction {
  private static final String FUNCTION_NAME = "avg";
  private static final double DEFAULT_VALUE = 0.0;
  private static final AggregationFunction.ResultDataType RESULT_DATA_TYPE = ResultDataType.AVERAGE_PAIR;

  /**
   * Performs 'sum' aggregation on the input array.
   * Returns {@value #DEFAULT_VALUE} if the input array is empty.
   *
   * {@inheritDoc}
   *
   * @param values
   * @return
   */
  @Override
  public double aggregate(double[] values) {
    double sum = 0.0;

    for (double value : values) {
      sum += value;
    }
    return (sum / values.length);
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
      Pair<Double, Long> avgValue = (Pair<Double, Long>) resultHolder.getResult(groupKey);

      if (avgValue == null) {
        avgValue = new com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction.AvgPair(0.0, 0L);
      }

      avgValue.setFirst(avgValue.getFirst() + valueArray[0][i]);
      avgValue.setSecond(avgValue.getSecond() + 1);
      resultHolder.putValueForKey(groupKey, avgValue);
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

      for (long groupKey : groupKeys) {
        Pair<Double, Long> avgValue = (Pair<Double, Long>) resultHolder.getResult(groupKey);

        if (avgValue == null) {
          avgValue = new com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction.AvgPair(0.0, 0L);
        }
        avgValue.setFirst(avgValue.getFirst() + valueArray[i]);
        avgValue.setSecond(avgValue.getSecond() + 1);
        resultHolder.putValueForKey(groupKey, avgValue);
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
    return DEFAULT_VALUE;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public AggregationFunction.ResultDataType getResultDataType() {
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
    double reducedSumResult = 0;
    long reducedCntResult = 0;

    for (Object object : combinedResult) {
      Pair resultPair = (Pair) object;
      reducedSumResult += (double) resultPair.getFirst();
      reducedCntResult += (long) resultPair.getSecond();
    }

    if (reducedCntResult > 0) {
      double avgResult = reducedSumResult / reducedCntResult;
      return avgResult;
    } else {
      return DEFAULT_VALUE;
    }
  }
}
