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

import com.linkedin.pinot.common.request.AggregationInfo;
import java.util.List;


/**
 * Factory class to create instances of aggregation function of the given name.
 */
public class AggregationFunctionFactory {
  public static final String SUM_AGGREGATION_FUNCTION = "sum";
  public static final String MAX_AGGREGATION_FUNCTION = "max";
  public static final String MIN_AGGREGATION_FUNCTION = "min";
  public static final String AVG_AGGREGATION_FUNCTION = "avg";

  /**
   * Given the name of aggregation function, create and return a new instance
   * of the corresponding aggregation function and return.
   *
   * @param functionName
   * @return
   */
  public static AggregationFunction getAggregationFunction(String functionName) {
    AggregationFunction function;

    switch (functionName.toLowerCase()) {
      case SUM_AGGREGATION_FUNCTION:
        function = new SumAggregationFunction();
        break;

      case MIN_AGGREGATION_FUNCTION:
        function = new MinAggregationFunction();
        break;

      case MAX_AGGREGATION_FUNCTION:
        function = new MaxAggregationFunction();
        break;

      case AVG_AGGREGATION_FUNCTION:
        function = new AvgAggregationFunction();
        break;

      default:
        throw new RuntimeException("Unsupported aggregation function: " + functionName);
    }

    return function;
  }
}
