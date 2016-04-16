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

import com.linkedin.pinot.core.operator.groupby.ResultHolder;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.List;


/**
 * Class to perform the 'count' aggregation function.
 */
public class CountAggregationFunction implements AggregationFunction {
  private static final String FUNCTION_NAME = "count";
  private static final double DEFAULT_VALUE = 0.0;
  private static final ResultDataType _resultDataType = ResultDataType.LONG;

  /**
   * Performs 'count' aggregation on the input array.
   * Returns {@value #DEFAULT_VALUE} if the input array is empty.
   *
   * {@inheritDoc}
   *
   * @param values
   * @return
   */
  @Override
  public double aggregate(double[] values) {
    return values.length;
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
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeys[i];
      double oldValue = resultHolder.getDoubleResult(groupKey);
      resultHolder.putValueForKey(groupKey, (oldValue + 1));
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
    for (int i = 0; i < length; ++i) {
      long[] groupKeys = (long[]) valueArrayIndexToGroupKeys.get(i);

      for (long groupKey : groupKeys) {
        double oldValue = resultHolder.getDoubleResult(groupKey);
        resultHolder.putValueForKey(groupKey, oldValue + 1);
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
      reducedResult += result;
    }
    return reducedResult;
  }
}
