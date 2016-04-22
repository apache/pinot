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
import java.util.List;


/**
 * Class to implement the 'min' aggregation function.
 */
public class MinAggregationFunction implements AggregationFunction {
  private static final String FUNCTION_NAME = "min";
  private static final double DEFAULT_VALUE = Double.POSITIVE_INFINITY;
  private static final ResultDataType _resultDataType = ResultDataType.DOUBLE;

  /**
   * Performs the 'sum' aggregation function on the input array.
   * Returns {@value #DEFAULT_VALUE} if input array is empty.
   *
   * {@inheritDoc}
   * @param values
   * @return
   */
  @Override
  public double aggregate(double[] values) {
    double ret = DEFAULT_VALUE;

    for (int i = 0; i < values.length; i++) {
      ret = Math.min(ret, values[i]);
    }
    return ret;
  }

  /**
   * {@inheritDoc}
   * While the interface allows for variable number of valueArrays, we do not support
   * multiple columns within one aggregation function right now.
   *
   * @param length
   * @param groupKeys
   * @param resultHolder
   * @param valueArray
   */
  @Override
  public void applySV(int length, int[] groupKeys, ResultHolder resultHolder, double[]... valueArray) {
    Preconditions.checkArgument(valueArray.length == 1);
    Preconditions.checkState(length <= valueArray[0].length);

    for (int i = 0; i < length; i++) {
      int groupKey = groupKeys[i];
      double oldValue = resultHolder.getDoubleResult(groupKey);
      resultHolder.putValueForKey(groupKey, Math.min(oldValue, valueArray[0][i]));
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
  public void applyMV(int length, int[][] docIdToGroupKeys, ResultHolder resultHolder, double[]... valueArray) {
    Preconditions.checkArgument(valueArray.length == 1);
    Preconditions.checkState(length <= valueArray[0].length);

    for (int i = 0; i < length; ++i) {
      for (int groupKey : docIdToGroupKeys[i]) {
        double oldValue = resultHolder.getDoubleResult(groupKey);
        double newValue = Math.min(oldValue, valueArray[0][i]);
        resultHolder.putValueForKey(groupKey, newValue);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param combinedResult
   * @return
   */
  @Override
  public Double reduce(List<Object> combinedResult) {
    double reducedResult = DEFAULT_VALUE;

    for (Object object : combinedResult) {
      double result = (Double) object;
      reducedResult = Math.min(result, reducedResult);
    }
    return reducedResult;
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
  public ResultDataType getResultDataType() {
    return _resultDataType;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public String getName() {
    return FUNCTION_NAME;
  }
}
