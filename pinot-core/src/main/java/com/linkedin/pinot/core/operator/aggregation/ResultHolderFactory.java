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
package com.linkedin.pinot.core.operator.aggregation;

import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.operator.aggregation.groupby.DoubleGroupByResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.ObjectGroupByResultHolder;


/**
 * The <code>ResultHolderFactory</code> class is the factory to create {@link AggregationResultHolder} and
 * {@link GroupByResultHolder} that appropriate for a given function.
 */
public class ResultHolderFactory {
  private ResultHolderFactory() {
  }

  public static final int MAX_INITIAL_RESULT_HOLDER_CAPACITY = 10_000;

  /**
   * Get the appropriate implementation of {@link AggregationResultHolder} based on the aggregation function.
   *
   * @param function aggregation function.
   * @return appropriate aggregation result holder.
   */
  public static AggregationResultHolder getAggregationResultHolder(AggregationFunction function) {
    String functionName = function.getName();

    switch (functionName.toLowerCase()) {
      case AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MAX_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MIN_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.SUM_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.COUNT_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MAX_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MIN_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.SUM_MV_AGGREGATION_FUNCTION:
        return new DoubleAggregationResultHolder(function.getDefaultValue());

      default:
        return new ObjectAggregationResultHolder();
    }
  }

  /**
   * Get the appropriate implementation of {@link GroupByResultHolder} based on the aggregation function.
   *
   * @param function aggregation function.
   * @param capacityCap maximum result holder capacity needed.
   * @param trimSize maximum number of groups returned after trimming.
   * @return appropriate group-by result holder.
   */
  public static GroupByResultHolder getGroupByResultHolder(AggregationFunction function, int capacityCap,
      int trimSize) {
    String functionName = function.getName();

    int initialCapacity = Math.min(capacityCap, MAX_INITIAL_RESULT_HOLDER_CAPACITY);

    switch (functionName.toLowerCase()) {
      case AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MAX_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.SUM_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.COUNT_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MAX_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.SUM_MV_AGGREGATION_FUNCTION:
        return new DoubleGroupByResultHolder(initialCapacity, capacityCap, trimSize, function.getDefaultValue(),
            false /* minOrder */);

      case AggregationFunctionFactory.MIN_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MIN_MV_AGGREGATION_FUNCTION:
        return new DoubleGroupByResultHolder(initialCapacity, capacityCap, trimSize, function.getDefaultValue(),
            true /* minOrder */);

      default:
        return new ObjectGroupByResultHolder(initialCapacity, capacityCap, trimSize);
    }
  }
}
