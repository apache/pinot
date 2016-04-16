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
import com.linkedin.pinot.core.query.utils.Pair;


public class ResultHolderFactory {
  public static final int MAX_NUM_GROUP_KEYS_FOR_ARRAY_BASED = 1000000;
  public static final int INITIAL_RESULT_HOLDER_CAPACITY = 10000;

  /**
   * Creates and returns the appropriate implementation of ResultHolder,
   * based on the maximum number of results.
   *
   * For max number of results below a certain threshold {@value #MAX_NUM_GROUP_KEYS_FOR_ARRAY_BASED}
   * ArrayBasedResultHolder is returned, otherwise, MapBasedResultHolder is returned.
   *
   * @param maxNumResults
   * @return
   */
  public static ResultHolder getResultHolder(AggregationFunction function, long maxNumResults) {
    String functionName = function.getName();

    if (maxNumResults <= MAX_NUM_GROUP_KEYS_FOR_ARRAY_BASED) {
      int initialCapacity = (int) Math.min(maxNumResults, INITIAL_RESULT_HOLDER_CAPACITY);

      switch (functionName.toLowerCase()) {
        case "count":
        case "sum":
        case "min":
        case "max":
          double defaultValue = function.getDefaultValue();
          DoubleResultArray doubleResultArray = new DoubleResultArray(initialCapacity, defaultValue);
          return new DoubleArrayBasedResultHolder(doubleResultArray, initialCapacity, (int) maxNumResults, defaultValue);

        case "avg":
          DoubleLongResultArray
              doubleLongResultArray = new DoubleLongResultArray(initialCapacity, new Pair<Double, Long>(0.0, 0L));
          return new PairArrayBasedResultHolder(doubleLongResultArray, initialCapacity);

        case "minmaxrange":
          DoubleDoubleResultArray doubleDoubleResultArray = new DoubleDoubleResultArray(initialCapacity,
              new Pair<Double, Double>(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
          return new PairArrayBasedResultHolder(doubleDoubleResultArray, initialCapacity);

        default:
          throw new RuntimeException("Aggregation function not implemented in ResultHolder: " + functionName);
      }
    } else {
      switch (functionName.toLowerCase()) {
        case "count":
        case "sum":
        case "min":
        case "max":
          double defaultValue = function.getDefaultValue();
          return new MapBasedDoubleResultHolder(defaultValue);

        case "avg":
        case "minmaxrange":
          return new MapBasedPairResultHolder();

        default:
          throw new RuntimeException("Aggregation function not implemented in ResultHolder: " + functionName);
      }
    }
  }
}
