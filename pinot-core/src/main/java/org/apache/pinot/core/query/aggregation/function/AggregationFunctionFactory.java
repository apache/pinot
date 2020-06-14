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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.query.exception.BadQueryRequestException;


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
    List<String> arguments = AggregationFunctionUtils.getArguments(aggregationInfo);

    try {
      String upperCaseFunctionName = functionName.toUpperCase();
      String column = arguments.get(0);
      if (upperCaseFunctionName.startsWith("PERCENTILE")) {
        String remainingFunctionName = upperCaseFunctionName.substring(10);
        int numArguments = arguments.size();
        if (numArguments == 1) {
          // Single argument percentile (e.g. Percentile99(foo), PercentileTDigest95(bar), etc.)
          if (remainingFunctionName.matches("\\d+")) {
            // Percentile
            return new PercentileAggregationFunction(column,
                AggregationFunctionUtils.parsePercentile(remainingFunctionName));
          } else if (remainingFunctionName.matches("EST\\d+")) {
            // PercentileEst
            String percentileString = remainingFunctionName.substring(3);
            return new PercentileEstAggregationFunction(column,
                AggregationFunctionUtils.parsePercentile(percentileString));
          } else if (remainingFunctionName.matches("TDIGEST\\d+")) {
            // PercentileTDigest
            String percentileString = remainingFunctionName.substring(7);
            return new PercentileTDigestAggregationFunction(column,
                AggregationFunctionUtils.parsePercentile(percentileString));
          } else if (remainingFunctionName.matches("\\d+MV")) {
            // PercentileMV
            String percentileString = remainingFunctionName.substring(0, remainingFunctionName.length() - 2);
            return new PercentileMVAggregationFunction(column,
                AggregationFunctionUtils.parsePercentile(percentileString));
          } else if (remainingFunctionName.matches("EST\\d+MV")) {
            // PercentileEstMV
            String percentileString = remainingFunctionName.substring(3, remainingFunctionName.length() - 2);
            return new PercentileEstMVAggregationFunction(column,
                AggregationFunctionUtils.parsePercentile(percentileString));
          } else if (remainingFunctionName.matches("TDIGEST\\d+MV")) {
            // PercentileTDigestMV
            String percentileString = remainingFunctionName.substring(7, remainingFunctionName.length() - 2);
            return new PercentileTDigestMVAggregationFunction(column,
                AggregationFunctionUtils.parsePercentile(percentileString));
          }
        } else if (numArguments == 2) {
          // Double arguments percentile (e.g. percentile(foo, 99), percentileTDigest(bar, 95), etc.)
          int percentile = AggregationFunctionUtils.parsePercentile(arguments.get(1));
          if (remainingFunctionName.isEmpty()) {
            // Percentile
            return new PercentileAggregationFunction(column, percentile);
          }
          if (remainingFunctionName.equals("EST")) {
            // PercentileEst
            return new PercentileEstAggregationFunction(column, percentile);
          }
          if (remainingFunctionName.equals("TDIGEST")) {
            // PercentileTDigest
            return new PercentileTDigestAggregationFunction(column, percentile);
          }
          if (remainingFunctionName.equals("MV")) {
            // PercentileMV
            return new PercentileMVAggregationFunction(column, percentile);
          }
          if (remainingFunctionName.equals("ESTMV")) {
            // PercentileEstMV
            return new PercentileEstMVAggregationFunction(column, percentile);
          }
          if (remainingFunctionName.equals("TDIGESTMV")) {
            // PercentileTDigestMV
            return new PercentileTDigestMVAggregationFunction(column, percentile);
          }
        }
        throw new IllegalArgumentException("Invalid percentile function");
      } else {
        switch (AggregationFunctionType.valueOf(upperCaseFunctionName)) {
          case COUNT:
            return new CountAggregationFunction();
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
            return new DistinctCountHLLAggregationFunction(arguments);
          case DISTINCTCOUNTRAWHLL:
            return new DistinctCountRawHLLAggregationFunction(arguments);
          case FASTHLL:
            return new FastHLLAggregationFunction(column);
          case DISTINCTCOUNTTHETASKETCH:
            return new DistinctCountThetaSketchAggregationFunction(arguments);
          case DISTINCTCOUNTRAWTHETASKETCH:
            return new DistinctCountRawThetaSketchAggregationFunction(arguments);
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
            return new DistinctCountHLLMVAggregationFunction(arguments);
          case DISTINCTCOUNTRAWHLLMV:
            return new DistinctCountRawHLLMVAggregationFunction(arguments);
          case DISTINCT:
            Preconditions.checkState(brokerRequest != null,
                "Broker request must be provided for 'DISTINCT' aggregation function");
            return new DistinctAggregationFunction(arguments, brokerRequest.getOrderBy(), brokerRequest.getLimit());
          default:
            throw new IllegalArgumentException();
        }
      }
    } catch (Exception e) {
      throw new BadQueryRequestException("Invalid aggregation: " + aggregationInfo);
    }
  }
}
