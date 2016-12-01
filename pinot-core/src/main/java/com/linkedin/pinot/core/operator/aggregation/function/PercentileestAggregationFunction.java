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
import com.linkedin.pinot.core.query.aggregation.function.quantile.digest.QuantileDigest;
import java.util.List;


/**
 * Class to implement the 'percentileestXX' aggregation function.
 */
public class PercentileestAggregationFunction implements AggregationFunction {
  private final String FUNCTION_NAME;
  private static final ResultDataType RESULT_DATA_TYPE = ResultDataType.PERCENTILEEST_QUANTILEDIGEST;
  public static final double DEFAULT_MAX_ERROR = 0.05;
  private final int _percentile;

  public PercentileestAggregationFunction(int percentile) {
    switch (percentile) {
      case 50:
        FUNCTION_NAME = AggregationFunctionFactory.PERCENTILEEST50_AGGREGATION_FUNCTION;
        break;
      case 90:
        FUNCTION_NAME = AggregationFunctionFactory.PERCENTILEEST90_AGGREGATION_FUNCTION;
        break;
      case 95:
        FUNCTION_NAME = AggregationFunctionFactory.PERCENTILEEST95_AGGREGATION_FUNCTION;
        break;
      case 99:
        FUNCTION_NAME = AggregationFunctionFactory.PERCENTILEEST99_AGGREGATION_FUNCTION;
        break;
      default:
        throw new RuntimeException("Invalid percentile for PercentileAggregationFunction: " + percentile);
    }
    _percentile = percentile;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  /**
   * Performs 'percentileest' aggregation on the input array.
   *
   * {@inheritDoc}
   *
   * @param length
   * @param resultHolder
   * @param valueArray
   */
  @Override
  public void aggregate(int length, AggregationResultHolder resultHolder, Object... valueArray) {
    Preconditions.checkArgument(valueArray.length == 1);
    Preconditions.checkArgument(valueArray[0] instanceof double[]);
    final double[] values = (double[]) valueArray[0];
    Preconditions.checkState(length <= values.length);

    QuantileDigest digest = resultHolder.getResult();
    if (digest == null) {
      digest = new QuantileDigest(DEFAULT_MAX_ERROR);
      resultHolder.setValue(digest);
    }

    for (int i = 0; i < length; i++) {
      digest.add((long) values[i]);
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
    Preconditions.checkArgument(valueArray[0] instanceof double[]);
    final double[] values = (double[]) valueArray[0];
    Preconditions.checkState(length <= values.length);

    for (int i = 0; i < length; i++) {
      int groupKey = groupKeys[i];
      QuantileDigest digest = resultHolder.getResult(groupKey);
      if (digest == null) {
        digest = new QuantileDigest(DEFAULT_MAX_ERROR);
        resultHolder.setValueForKey(groupKey, digest);
      }
      digest.add((long) values[i]);
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
    Preconditions.checkArgument(valueArray[0] instanceof double[]);
    final double[] values = (double[]) valueArray[0];
    Preconditions.checkState(length <= values.length);

    for (int i = 0; i < length; i++) {
      long value = (long) values[i];
      for (int groupKey : docIdToGroupKeys[i]) {
        QuantileDigest digest = resultHolder.getResult(groupKey);
        if (digest == null) {
          digest = new QuantileDigest(DEFAULT_MAX_ERROR);
          resultHolder.setValueForKey(groupKey, digest);
        }
        digest.add(value);
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
