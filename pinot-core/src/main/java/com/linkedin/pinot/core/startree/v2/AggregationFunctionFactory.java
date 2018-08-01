/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.startree.v2;

import javax.annotation.Nonnull;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;


/**
 * Factory class to create instances of aggregation function of the given name.
 */
public class AggregationFunctionFactory {

  public AggregationFunctionFactory() {
  }

  /**
   * Given the name of the aggregation function, returns a new instance of the corresponding aggregation function.
   */
  public static AggregationFunction getAggregationFunction(@Nonnull String functionName) {

    switch (AggregationFunctionType.valueOf(functionName.toUpperCase())) {
      case SUM:
        return new SumAggregationFunction();
      case MIN:
        return new MinAggregationFunction();
      case MAX:
        return new MaxAggregationFunction();
      case COUNT:
        return new CountAggregationFunction();
      case DISTINCTCOUNTHLL:
        return new DistinctCountHLLAggregationFunction();
      case PERCENTILETDIGEST:
        return new PercentileTDigestAggregationFunction();
      case PERCENTILEEST:
        return new PercentileEstAggregationFunction();
      default:
        throw new UnsupportedOperationException();
    }
  }
}
