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
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;


/**
 * Factory class to create instances of aggregation function of the given name.
 */
@SuppressWarnings("rawtypes")
public class AggregationFunctionFactory {
  private AggregationFunctionFactory() {
  }

  /**
   * Given the function information, returns a new instance of the corresponding aggregation function.
   * <p>NOTE: Underscores in the function name are ignored.
   * <p>NOTE: We pass the query context to this method because DISTINCT is currently modeled as an aggregation function
   *          and requires the order-by and limit information from the query.
   * <p>TODO: Consider modeling DISTINCT as unique selection instead of aggregation so that early-termination, limit and
   *          offset can be applied easier
   */
  public static AggregationFunction getAggregationFunction(FunctionContext function, QueryContext queryContext) {
    try {
      String upperCaseFunctionName = StringUtils.remove(function.getFunctionName(), '_').toUpperCase();
      List<ExpressionContext> arguments = function.getArguments();
      ExpressionContext firstArgument = arguments.get(0);
      if (upperCaseFunctionName.startsWith("PERCENTILE")) {
        String remainingFunctionName = upperCaseFunctionName.substring(10);
        if (remainingFunctionName.equals("SMARTTDIGEST")) {
          return new PercentileSmartTDigestAggregationFunction(arguments);
        }
        int numArguments = arguments.size();
        if (numArguments == 1) {
          // Single argument percentile (e.g. Percentile99(foo), PercentileTDigest95(bar), etc.)
          if (remainingFunctionName.matches("\\d+")) {
            // Percentile
            return new PercentileAggregationFunction(firstArgument, parsePercentileToInt(remainingFunctionName));
          } else if (remainingFunctionName.matches("EST\\d+")) {
            // PercentileEst
            String percentileString = remainingFunctionName.substring(3);
            return new PercentileEstAggregationFunction(firstArgument, parsePercentileToInt(percentileString));
          } else if (remainingFunctionName.matches("RAWEST\\d+")) {
            // PercentileRawEst
            String percentileString = remainingFunctionName.substring(6);
            return new PercentileRawEstAggregationFunction(firstArgument, parsePercentileToInt(percentileString));
          } else if (remainingFunctionName.matches("TDIGEST\\d+")) {
            // PercentileTDigest
            String percentileString = remainingFunctionName.substring(7);
            return new PercentileTDigestAggregationFunction(firstArgument, parsePercentileToInt(percentileString));
          } else if (remainingFunctionName.matches("RAWTDIGEST\\d+")) {
            // PercentileRawTDigest
            String percentileString = remainingFunctionName.substring(10);
            return new PercentileRawTDigestAggregationFunction(firstArgument, parsePercentileToInt(percentileString));
          } else if (remainingFunctionName.matches("\\d+MV")) {
            // PercentileMV
            String percentileString = remainingFunctionName.substring(0, remainingFunctionName.length() - 2);
            return new PercentileMVAggregationFunction(firstArgument, parsePercentileToInt(percentileString));
          } else if (remainingFunctionName.matches("EST\\d+MV")) {
            // PercentileEstMV
            String percentileString = remainingFunctionName.substring(3, remainingFunctionName.length() - 2);
            return new PercentileEstMVAggregationFunction(firstArgument, parsePercentileToInt(percentileString));
          } else if (remainingFunctionName.matches("RAWEST\\d+MV")) {
            // PercentileRawEstMV
            String percentileString = remainingFunctionName.substring(6, remainingFunctionName.length() - 2);
            return new PercentileRawEstMVAggregationFunction(firstArgument, parsePercentileToInt(percentileString));
          } else if (remainingFunctionName.matches("TDIGEST\\d+MV")) {
            // PercentileTDigestMV
            String percentileString = remainingFunctionName.substring(7, remainingFunctionName.length() - 2);
            return new PercentileTDigestMVAggregationFunction(firstArgument, parsePercentileToInt(percentileString));
          } else if (remainingFunctionName.matches("RAWTDIGEST\\d+MV")) {
            // PercentileRawTDigestMV
            String percentileString = remainingFunctionName.substring(10, remainingFunctionName.length() - 2);
            return new PercentileRawTDigestMVAggregationFunction(firstArgument, parsePercentileToInt(percentileString));
          }
        } else if (numArguments == 2) {
          // Double arguments percentile (e.g. percentile(foo, 99), percentileTDigest(bar, 95), etc.) where the
          // second argument is a decimal number from 0.0 to 100.0.
          // Have to use literal string because we need to cast int to double here.
          double percentile = parsePercentileToDouble(arguments.get(1).getLiteralString());
          if (remainingFunctionName.isEmpty()) {
            // Percentile
            return new PercentileAggregationFunction(firstArgument, percentile);
          }
          if (remainingFunctionName.equals("EST")) {
            // PercentileEst
            return new PercentileEstAggregationFunction(firstArgument, percentile);
          }
          if (remainingFunctionName.equals("RAWEST")) {
            // PercentileRawEst
            return new PercentileRawEstAggregationFunction(firstArgument, percentile);
          }
          if (remainingFunctionName.equals("TDIGEST")) {
            // PercentileTDigest
            return new PercentileTDigestAggregationFunction(firstArgument, percentile);
          }
          if (remainingFunctionName.equals("RAWTDIGEST")) {
            // PercentileRawTDigest
            return new PercentileRawTDigestAggregationFunction(firstArgument, percentile);
          }
          if (remainingFunctionName.equals("MV")) {
            // PercentileMV
            return new PercentileMVAggregationFunction(firstArgument, percentile);
          }
          if (remainingFunctionName.equals("ESTMV")) {
            // PercentileEstMV
            return new PercentileEstMVAggregationFunction(firstArgument, percentile);
          }
          if (remainingFunctionName.equals("RAWESTMV")) {
            // PercentileRawEstMV
            return new PercentileRawEstMVAggregationFunction(firstArgument, percentile);
          }
          if (remainingFunctionName.equals("TDIGESTMV")) {
            // PercentileTDigestMV
            return new PercentileTDigestMVAggregationFunction(firstArgument, percentile);
          }
          if (remainingFunctionName.equals("RAWTDIGESTMV")) {
            // PercentileRawTDigestMV
            return new PercentileRawTDigestMVAggregationFunction(firstArgument, percentile);
          }
        }
        throw new IllegalArgumentException("Invalid percentile function: " + function);
      } else {
        switch (AggregationFunctionType.valueOf(upperCaseFunctionName)) {
          case COUNT:
            return new CountAggregationFunction(firstArgument, queryContext.isNullHandlingEnabled());
          case MIN:
            return new MinAggregationFunction(firstArgument, queryContext.isNullHandlingEnabled());
          case MAX:
            return new MaxAggregationFunction(firstArgument, queryContext.isNullHandlingEnabled());
          case SUM:
            return new SumAggregationFunction(firstArgument, queryContext.isNullHandlingEnabled());
          case SUMPRECISION:
            return new SumPrecisionAggregationFunction(arguments, queryContext.isNullHandlingEnabled());
          case AVG:
            return new AvgAggregationFunction(firstArgument, queryContext.isNullHandlingEnabled());
          case MODE:
            return new ModeAggregationFunction(arguments);
          case FIRSTWITHTIME:
            if (arguments.size() == 3) {
              ExpressionContext timeCol = arguments.get(1);
              ExpressionContext dataType = arguments.get(2);
              if (dataType.getType() != ExpressionContext.Type.LITERAL) {
                throw new IllegalArgumentException("Third argument of firstWithTime Function should be literal."
                    + " The function can be used as firstWithTime(dataColumn, timeColumn, 'dataType')");
              }
              FieldSpec.DataType fieldDataType
                  = FieldSpec.DataType.valueOf(dataType.getLiteralString().toUpperCase());
              switch (fieldDataType) {
                case BOOLEAN:
                case INT:
                  return new FirstIntValueWithTimeAggregationFunction(
                      firstArgument, timeCol, fieldDataType == FieldSpec.DataType.BOOLEAN);
                case LONG:
                  return new FirstLongValueWithTimeAggregationFunction(firstArgument, timeCol);
                case FLOAT:
                  return new FirstFloatValueWithTimeAggregationFunction(firstArgument, timeCol);
                case DOUBLE:
                  return new FirstDoubleValueWithTimeAggregationFunction(firstArgument, timeCol);
                case STRING:
                  return new FirstStringValueWithTimeAggregationFunction(firstArgument, timeCol);
                default:
                  throw new IllegalArgumentException("Unsupported Value Type for firstWithTime Function:" + dataType);
              }
            } else {
              throw new IllegalArgumentException("Three arguments are required for firstWithTime Function."
                  + " The function can be used as firstWithTime(dataColumn, timeColumn, 'dataType')");
            }
          case LASTWITHTIME:
            if (arguments.size() == 3) {
              ExpressionContext timeCol = arguments.get(1);
              ExpressionContext dataType = arguments.get(2);
              if (dataType.getType() != ExpressionContext.Type.LITERAL) {
                throw new IllegalArgumentException("Third argument of lastWithTime Function should be literal."
                    + " The function can be used as lastWithTime(dataColumn, timeColumn, 'dataType')");
              }
              FieldSpec.DataType fieldDataType = FieldSpec.DataType.valueOf(dataType.getLiteralString().toUpperCase());
              switch (fieldDataType) {
                case BOOLEAN:
                case INT:
                  return new LastIntValueWithTimeAggregationFunction(firstArgument, timeCol,
                      fieldDataType == FieldSpec.DataType.BOOLEAN);
                case LONG:
                  return new LastLongValueWithTimeAggregationFunction(firstArgument, timeCol);
                case FLOAT:
                  return new LastFloatValueWithTimeAggregationFunction(firstArgument, timeCol);
                case DOUBLE:
                  return new LastDoubleValueWithTimeAggregationFunction(firstArgument, timeCol);
                case STRING:
                  return new LastStringValueWithTimeAggregationFunction(firstArgument, timeCol);
                default:
                  throw new IllegalArgumentException("Unsupported Value Type for lastWithTime Function:" + dataType);
              }
            } else {
              throw new IllegalArgumentException("Three arguments are required for lastWithTime Function."
                  + " The function can be used as lastWithTime(dataColumn, timeColumn, 'dataType')");
            }
          case MINMAXRANGE:
            return new MinMaxRangeAggregationFunction(firstArgument);
          case DISTINCTCOUNT:
            return new DistinctCountAggregationFunction(firstArgument);
          case DISTINCTCOUNTBITMAP:
            return new DistinctCountBitmapAggregationFunction(firstArgument);
          case SEGMENTPARTITIONEDDISTINCTCOUNT:
            return new SegmentPartitionedDistinctCountAggregationFunction(firstArgument);
          case DISTINCTCOUNTHLL:
            return new DistinctCountHLLAggregationFunction(arguments);
          case DISTINCTCOUNTRAWHLL:
            return new DistinctCountRawHLLAggregationFunction(arguments);
          case DISTINCTCOUNTSMARTHLL:
            return new DistinctCountSmartHLLAggregationFunction(arguments);
          case FASTHLL:
            return new FastHLLAggregationFunction(firstArgument);
          case DISTINCTCOUNTTHETASKETCH:
            return new DistinctCountThetaSketchAggregationFunction(arguments);
          case DISTINCTCOUNTRAWTHETASKETCH:
            return new DistinctCountRawThetaSketchAggregationFunction(arguments);
          case IDSET:
            return new IdSetAggregationFunction(arguments);
          case COUNTMV:
            return new CountMVAggregationFunction(firstArgument);
          case MINMV:
            return new MinMVAggregationFunction(firstArgument);
          case MAXMV:
            return new MaxMVAggregationFunction(firstArgument);
          case SUMMV:
            return new SumMVAggregationFunction(firstArgument);
          case AVGMV:
            return new AvgMVAggregationFunction(firstArgument);
          case MINMAXRANGEMV:
            return new MinMaxRangeMVAggregationFunction(firstArgument);
          case DISTINCTCOUNTMV:
            return new DistinctCountMVAggregationFunction(firstArgument);
          case DISTINCTCOUNTBITMAPMV:
            return new DistinctCountBitmapMVAggregationFunction(firstArgument);
          case DISTINCTCOUNTHLLMV:
            return new DistinctCountHLLMVAggregationFunction(arguments);
          case DISTINCTCOUNTRAWHLLMV:
            return new DistinctCountRawHLLMVAggregationFunction(arguments);
          case DISTINCT:
            return new DistinctAggregationFunction(arguments, queryContext.getOrderByExpressions(),
                queryContext.getLimit());
          case STUNION:
            return new StUnionAggregationFunction(firstArgument);
          case HISTOGRAM:
            return new HistogramAggregationFunction(arguments);
          case COVARPOP:
            return new CovarianceAggregationFunction(arguments, false);
          case COVARSAMP:
            return new CovarianceAggregationFunction(arguments, true);
          default:
            throw new IllegalArgumentException();
        }
      }
    } catch (Exception e) {
      throw new BadQueryRequestException(
          "Invalid aggregation function: " + function + "; Reason: " + e.getMessage());
    }
  }

  private static int parsePercentileToInt(String percentileString) {
    int percentile = Integer.parseInt(percentileString);
    Preconditions.checkArgument(percentile >= 0 && percentile <= 100, "Invalid percentile: %s", percentile);
    return percentile;
  }

  private static double parsePercentileToDouble(String percentileString) {
    double percentile = Double.parseDouble(percentileString);
    Preconditions.checkArgument(percentile >= 0d && percentile <= 100d, "Invalid percentile: %s", percentile);
    return percentile;
  }
}
