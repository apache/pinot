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
package com.linkedin.pinot.core.operator.groupby;

import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.utils.Pair;


public class ResultHolderFactory {
  public static final int INITIAL_RESULT_HOLDER_CAPACITY = 10000;

  /**
   * Creates and returns the appropriate implementation of ResultHolder,
   * based on aggregation function.
   *
   * @param maxNumResults
   * @return
   */
  public static ResultHolder getResultHolder(AggregationFunction function, long maxNumResults) {
    String functionName = function.getName();

    int capacityCap = maxNumResults > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxNumResults;
    int initialCapacity = Math.min(capacityCap, INITIAL_RESULT_HOLDER_CAPACITY);

    switch (functionName.toLowerCase()) {
      case AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MAX_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.MIN_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.SUM_AGGREGATION_FUNCTION:
        double defaultValue = function.getDefaultValue();
        DoubleResultArray doubleResultArray = new DoubleResultArray(initialCapacity, defaultValue);
        return new DoubleArrayBasedResultHolder(doubleResultArray, initialCapacity, capacityCap);

      case AggregationFunctionFactory.AVG_AGGREGATION_FUNCTION:
        DoubleLongResultArray doubleLongResultArray = new DoubleLongResultArray(initialCapacity, new Pair<>(0.0, 0L));
        return new PairArrayBasedResultHolder(doubleLongResultArray, initialCapacity, capacityCap);

      case AggregationFunctionFactory.MINMAXRANGE_AGGREGATION_FUNCTION:
        DoubleDoubleResultArray doubleDoubleResultArray = new DoubleDoubleResultArray(initialCapacity,
            new Pair<>(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
        return new PairArrayBasedResultHolder(doubleDoubleResultArray, initialCapacity, capacityCap);

      default:
        return new ObjectArrayBasedResultHolder(initialCapacity, capacityCap);
    }
  }
}
