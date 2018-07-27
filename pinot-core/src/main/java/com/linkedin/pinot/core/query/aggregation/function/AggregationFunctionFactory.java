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
package com.linkedin.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
import javax.annotation.Nonnull;


/**
 * Factory class to create instances of aggregation function of the given name.
 */
public class AggregationFunctionFactory {
  private AggregationFunctionFactory() {
  }

  /**
   * Given the name of the aggregation function, returns a new instance of the corresponding aggregation function.
   */
  @Nonnull
  public static AggregationFunction getAggregationFunction(@Nonnull String functionName) {
    try {
      String upperCaseFunctionName = functionName.toUpperCase();
      if (upperCaseFunctionName.startsWith("PERCENTILE")) {
        String remainingFunctionName = upperCaseFunctionName.substring(10);
        if (remainingFunctionName.matches("\\d+")) {
          // Percentile
          return new PercentileAggregationFunction(parsePercentile(remainingFunctionName));
        } else if (remainingFunctionName.matches("EST\\d+")) {
          // PercentileEst
          return new PercentileEstAggregationFunction(parsePercentile(remainingFunctionName.substring(3)));
        } else if (remainingFunctionName.matches("TDIGEST\\d+")) {
          // PercentileTDigest
          return new PercentileTDigestAggregationFunction(parsePercentile(remainingFunctionName.substring(7)));
        } else if (remainingFunctionName.matches("\\d+MV")) {
          // PercentileMV
          return new PercentileMVAggregationFunction(
              parsePercentile(remainingFunctionName.substring(0, remainingFunctionName.length() - 2)));
        } else if (remainingFunctionName.matches("EST\\d+MV")) {
          // PercentileEstMV
          return new PercentileEstMVAggregationFunction(
              parsePercentile(remainingFunctionName.substring(3, remainingFunctionName.length() - 2)));
        } else if (remainingFunctionName.matches("TDIGEST\\d+MV")) {
          // PercentileTDigestMV
          return new PercentileTDigestMVAggregationFunction(
              parsePercentile(remainingFunctionName.substring(7, remainingFunctionName.length() - 2)));
        } else {
          throw new IllegalArgumentException();
        }
      } else {
        switch (AggregationFunctionType.valueOf(upperCaseFunctionName)) {
          case COUNT:
            return new CountAggregationFunction();
          case MIN:
            return new MinAggregationFunction();
          case MAX:
            return new MaxAggregationFunction();
          case SUM:
            return new SumAggregationFunction();
          case AVG:
            return new AvgAggregationFunction();
          case MINMAXRANGE:
            return new MinMaxRangeAggregationFunction();
          case DISTINCTCOUNT:
            return new DistinctCountAggregationFunction();
          case DISTINCTCOUNTHLL:
            return new DistinctCountHLLAggregationFunction();
          case FASTHLL:
            return new FastHLLAggregationFunction();
          case COUNTMV:
            return new CountMVAggregationFunction();
          case MINMV:
            return new MinMVAggregationFunction();
          case MAXMV:
            return new MaxMVAggregationFunction();
          case SUMMV:
            return new SumMVAggregationFunction();
          case AVGMV:
            return new AvgMVAggregationFunction();
          case MINMAXRANGEMV:
            return new MinMaxRangeMVAggregationFunction();
          case DISTINCTCOUNTMV:
            return new DistinctCountMVAggregationFunction();
          case DISTINCTCOUNTHLLMV:
            return new DistinctCountHLLMVAggregationFunction();
          default:
            throw new IllegalArgumentException();
        }
      }
    } catch (Exception e) {
      throw new BadQueryRequestException("Invalid aggregation function name: " + functionName);
    }
  }

  private static int parsePercentile(String percentileString) {
    int percentile = Integer.parseInt(percentileString);
    Preconditions.checkState(percentile >= 0 && percentile <= 100);
    return percentile;
  }
}
