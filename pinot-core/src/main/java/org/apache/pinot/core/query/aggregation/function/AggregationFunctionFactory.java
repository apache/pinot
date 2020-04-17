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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.parsers.CompilerConstants;


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
    String argumentsString = AggregationFunctionUtils.getColumn(aggregationInfo);
    List<String> arguments = Arrays.asList(argumentsString.split(CompilerConstants.AGGREGATION_FUNCTION_ARG_SEPARATOR));

    try {
      String upperCaseFunctionName = functionName.toUpperCase();
      if (upperCaseFunctionName.startsWith("PERCENTILE")) {
        String remainingFunctionName = upperCaseFunctionName.substring(10);
        List<String> args = new ArrayList<>(arguments);
        if (remainingFunctionName.matches("\\d+")) {
          // Percentile
          args.add(remainingFunctionName);
          return new PercentileAggregationFunction(args);
        } else if (remainingFunctionName.matches("EST\\d+")) {
          // PercentileEst
          args.add(remainingFunctionName.substring(3));
          return new PercentileEstAggregationFunction(args);
        } else if (remainingFunctionName.matches("TDIGEST\\d+")) {
          // PercentileTDigest
          args.add(remainingFunctionName.substring(7));
          return new PercentileTDigestAggregationFunction(args);
        } else if (remainingFunctionName.matches("\\d+MV")) {
          // PercentileMV
          args.add(remainingFunctionName.substring(0, remainingFunctionName.length() - 2));
          return new PercentileMVAggregationFunction(args);
        } else if (remainingFunctionName.matches("EST\\d+MV")) {
          // PercentileEstMV
          args.add(remainingFunctionName.substring(3, remainingFunctionName.length() - 2));
          return new PercentileEstMVAggregationFunction(args);
        } else if (remainingFunctionName.matches("TDIGEST\\d+MV")) {
          // PercentileTDigestMV
          args.add(remainingFunctionName.substring(7, remainingFunctionName.length() - 2));
          return new PercentileTDigestMVAggregationFunction(args);
        } else {
          throw new IllegalArgumentException();
        }
      } else {
        String column = arguments.get(0);
        switch (AggregationFunctionType.valueOf(upperCaseFunctionName)) {
          case COUNT:
            return new CountAggregationFunction(column);
          case MIN:
            return new MinAggregationFunction(column);
          case MAX:
            return new MaxAggregationFunction(column);
          case SUM:
            return new SumAggregationFunction(column);
          case AVG:
            return new AvgAggregationFunction(column);
          case MINMAXRANGE:
            return new MinMaxRangeAggregationFunction(column);
          case DISTINCTCOUNT:
            return new DistinctCountAggregationFunction(column);
          case DISTINCTCOUNTHLL:
            return new DistinctCountHLLAggregationFunction(column);
          case DISTINCTCOUNTRAWHLL:
            return new DistinctCountRawHLLAggregationFunction(column);
          case FASTHLL:
            return new FastHLLAggregationFunction(column);
          case COUNTMV:
            return new CountMVAggregationFunction(column);
          case MINMV:
            return new MinMVAggregationFunction(column);
          case MAXMV:
            return new MaxMVAggregationFunction(column);
          case SUMMV:
            return new SumMVAggregationFunction(column);
          case AVGMV:
            return new AvgMVAggregationFunction(column);
          case MINMAXRANGEMV:
            return new MinMaxRangeMVAggregationFunction(column);
          case DISTINCTCOUNTMV:
            return new DistinctCountMVAggregationFunction(column);
          case DISTINCTCOUNTHLLMV:
            return new DistinctCountHLLMVAggregationFunction(column);
          case DISTINCTCOUNTRAWHLLMV:
            return new DistinctCountRawHLLMVAggregationFunction(column);
          case DISTINCT:
            Preconditions.checkState(brokerRequest != null,
                "Broker request must be provided for 'DISTINCT' aggregation function");
            return new DistinctAggregationFunction(arguments, brokerRequest.getOrderBy(),
                brokerRequest.getLimit());
          default:
            throw new IllegalArgumentException();
        }
      }
    } catch (Exception e) {
      throw new BadQueryRequestException("Invalid aggregation function name: " + functionName);
    }
  }
}
