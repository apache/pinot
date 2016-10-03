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
 * Factory class to create ResultHolder appropriate for a given function.
 * Supports both, AggregationResultHolder as well as GroupByResultHolder.
 */
public class ResultHolderFactory {
  private ResultHolderFactory() {
  }

  public static final int MAX_INITIAL_RESULT_HOLDER_CAPACITY = 10000;

  /**
   * Creates and returns the appropriate implementation of AggregationResultHolder,
   * based on aggregation function.
   *
   * @param function Aggregation function
   * @return Appropriate aggregation result holder
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
   * Creates and returns the appropriate implementation of GroupByResultHolder,
   * based on aggregation function.
   *
   * @param function Aggregation function
   * @param maxNumResults Max number of results
   * @return Appropriate group by result holder
   */
  public static GroupByResultHolder getGroupByResultHolder(AggregationFunction function, long maxNumResults) {
    String functionName = function.getName();

    int capacityCap = maxNumResults > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxNumResults;
    int initialCapacity = Math.min(capacityCap, MAX_INITIAL_RESULT_HOLDER_CAPACITY);

    switch (functionName.toLowerCase()) {
      case AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MAX_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MIN_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.SUM_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.COUNT_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MAX_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MIN_MV_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.SUM_MV_AGGREGATION_FUNCTION:
        return new DoubleGroupByResultHolder(initialCapacity, capacityCap, function.getDefaultValue());

      default:
        return new ObjectGroupByResultHolder(initialCapacity, capacityCap);
    }
  }
}
