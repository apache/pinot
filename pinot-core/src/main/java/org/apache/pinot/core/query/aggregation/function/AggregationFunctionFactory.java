/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.pql.parsers.pql2.ast.SelectAstNode;


/**
 * Factory class to create instances of aggregation function of the given name.
 */
public class AggregationFunctionFactory {
  private AggregationFunctionFactory() {
  }

  /**
   * Given the name of the aggregation function, returns a new instance of the corresponding aggregation function.
   * The limit is currently used for DISTINCT aggregation function. Unlike other aggregation functions,
   * DISTINCT can produce multi-row resultset. So the user specificed limit in query needs to be
   * passed down to function.
   */
  public static AggregationFunction getAggregationFunction(AggregationInfo aggregationInfo,
      @Nullable BrokerRequest brokerRequest) {
    String functionName = aggregationInfo.getAggregationType();
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
          case DISTINCTCOUNTRAWHLL:
            return new DistinctCountRawHLLAggregationFunction();
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
          case DISTINCTCOUNTRAWHLLMV:
            return new DistinctCountRawHLLMVAggregationFunction();
          case DISTINCT:
            return new DistinctAggregationFunction(AggregationFunctionUtils.getColumn(aggregationInfo),
                brokerRequest != null ? brokerRequest.getLimit() : SelectAstNode.DEFAULT_RECORD_LIMIT);
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
