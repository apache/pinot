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
import java.util.List;


/**
 * Class to perform the 'count' aggregation function.
 */
public class CountAggregationFunction implements AggregationFunction {
  private static final String FUNCTION_NAME = AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION;
  private static final double DEFAULT_VALUE = 0.0;
  private static final ResultDataType RESULT_DATA_TYPE = ResultDataType.LONG;

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  /**
   * Performs 'count' aggregation on the input array.
   * Returns {@value #DEFAULT_VALUE} if the input array is empty.
   *
   * {@inheritDoc}
   *
   * @param length
   * @param resultHolder
   * @param valueArray
   */
  @Override
  public void aggregate(int length, AggregationResultHolder resultHolder, Object... valueArray) {
    // Should never call count with a valueArray.
    Preconditions.checkArgument(valueArray.length == 0);

    resultHolder.setValue(resultHolder.getDoubleResult() + length);
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
    Preconditions.checkArgument(valueArray.length == 0);

    for (int i = 0; i < length; i++) {
      int groupKey = groupKeys[i];
      double oldValue = resultHolder.getDoubleResult(groupKey);
      resultHolder.setValueForKey(groupKey, (oldValue + 1));
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param length
   * @param docIdToGroupKey
   * @param resultHolder
   * @param valueArray
   */
  @Override
  public void aggregateGroupByMV(int length, int[][] docIdToGroupKey, GroupByResultHolder resultHolder,
      Object... valueArray) {
    Preconditions.checkArgument(valueArray.length == 0);
    for (int i = 0; i < length; ++i) {
      for (int groupKey : docIdToGroupKey[i]) {
        double oldValue = resultHolder.getDoubleResult(groupKey);
        resultHolder.setValueForKey(groupKey, oldValue + 1);
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
    return RESULT_DATA_TYPE;
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
