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
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
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
   */
  public static AggregationFunction getAggregationFunction(FunctionContext function, boolean nullHandlingEnabled) {
    try {
      String upperCaseFunctionName = StringUtils.remove(function.getFunctionName(), '_').toUpperCase();
      List<ExpressionContext> arguments = function.getArguments();
      ExpressionContext firstArgument = arguments.get(0);
      if (upperCaseFunctionName.startsWith("PERCENTILE")) {
        String remainingFunctionName = upperCaseFunctionName.substring(10);
        if (remainingFunctionName.equals("SMARTTDIGEST")) {
          return new PercentileSmartTDigestAggregationFunction(arguments);
        }
        if (remainingFunctionName.equals("KLL")) {
          return new PercentileKLLAggregationFunction(arguments);
        }
        if (remainingFunctionName.equals("KLLMV")) {
          return new PercentileKLLMVAggregationFunction(arguments);
        }
        if (remainingFunctionName.equals("RAWKLL")) {
          return new PercentileRawKLLAggregationFunction(arguments);
        }
        if (remainingFunctionName.equals("RAWKLLMV")) {
          return new PercentileRawKLLMVAggregationFunction(arguments);
        }
        int numArguments = arguments.size();
        if (numArguments == 1) {
          // Single argument percentile (e.g. Percentile99(foo), PercentileTDigest95(bar), etc.)
          // NOTE: This convention is deprecated. DO NOT add new functions here
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
          double percentile = parsePercentileToDouble(arguments.get(1).getLiteral().getStringValue());
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
        } else if (numArguments == 3) {
          // Triple arguments percentile (e.g. percentileTDigest(bar, 95, 1000), etc.) where the
          // second argument is a decimal number from 0.0 to 100.0 and third argument is a decimal number indicating
          // the compression_factor for the TDigest. This can only be used for TDigest type percentile functions to
          // pass in a custom compression_factor. If the two argument version is used the default compression_factor
          // of 100.0 is used.
          // Have to use literal string because we need to cast int to double here.
          double percentile = parsePercentileToDouble(arguments.get(1).getLiteral().getStringValue());
          int compressionFactor = parseCompressionFactorToInt(arguments.get(2).getLiteral().getStringValue());
          if (remainingFunctionName.equals("TDIGEST")) {
            // PercentileTDigest
            return new PercentileTDigestAggregationFunction(firstArgument, percentile, compressionFactor);
          }
          if (remainingFunctionName.equals("RAWTDIGEST")) {
            // PercentileRawTDigest
            return new PercentileRawTDigestAggregationFunction(firstArgument, percentile, compressionFactor);
          }
          if (remainingFunctionName.equals("TDIGESTMV")) {
            // PercentileTDigestMV
            return new PercentileTDigestMVAggregationFunction(firstArgument, percentile, compressionFactor);
          }
          if (remainingFunctionName.equals("RAWTDIGESTMV")) {
            // PercentileRawTDigestMV
            return new PercentileRawTDigestMVAggregationFunction(firstArgument, percentile, compressionFactor);
          }
        }
        throw new IllegalArgumentException("Invalid percentile function: " + function);
      } else {
        switch (AggregationFunctionType.valueOf(upperCaseFunctionName)) {
          case COUNT:
            return new CountAggregationFunction(firstArgument, nullHandlingEnabled);
          case MIN:
            return new MinAggregationFunction(firstArgument, nullHandlingEnabled);
          case MAX:
            return new MaxAggregationFunction(firstArgument, nullHandlingEnabled);
          case SUM:
            return new SumAggregationFunction(firstArgument, nullHandlingEnabled);
          case SUMPRECISION:
            return new SumPrecisionAggregationFunction(arguments, nullHandlingEnabled);
          case AVG:
            return new AvgAggregationFunction(firstArgument, nullHandlingEnabled);
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
              DataType fieldDataType = DataType.valueOf(dataType.getLiteral().getStringValue().toUpperCase());
              switch (fieldDataType) {
                case BOOLEAN:
                case INT:
                  return new FirstIntValueWithTimeAggregationFunction(firstArgument, timeCol,
                      fieldDataType == DataType.BOOLEAN);
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
              DataType fieldDataType = DataType.valueOf(dataType.getLiteral().getStringValue().toUpperCase());
              switch (fieldDataType) {
                case BOOLEAN:
                case INT:
                  return new LastIntValueWithTimeAggregationFunction(firstArgument, timeCol,
                      fieldDataType == DataType.BOOLEAN);
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
          case DISTINCTSUM:
            return new DistinctSumAggregationFunction(firstArgument);
          case DISTINCTAVG:
            return new DistinctAvgAggregationFunction(firstArgument);
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
          case DISTINCTSUMMV:
            return new DistinctSumMVAggregationFunction(firstArgument);
          case DISTINCTAVGMV:
            return new DistinctAvgMVAggregationFunction(firstArgument);
          case STUNION:
            return new StUnionAggregationFunction(firstArgument);
          case HISTOGRAM:
            return new HistogramAggregationFunction(arguments);
          case COVARPOP:
            return new CovarianceAggregationFunction(arguments, false);
          case COVARSAMP:
            return new CovarianceAggregationFunction(arguments, true);
          case BOOLAND:
            return new BooleanAndAggregationFunction(firstArgument, nullHandlingEnabled);
          case BOOLOR:
            return new BooleanOrAggregationFunction(firstArgument, nullHandlingEnabled);
          case VARPOP:
            return new VarianceAggregationFunction(firstArgument, false, false);
          case VARSAMP:
            return new VarianceAggregationFunction(firstArgument, true, false);
          case STDDEVPOP:
            return new VarianceAggregationFunction(firstArgument, false, true);
          case STDDEVSAMP:
            return new VarianceAggregationFunction(firstArgument, true, true);
          case SKEWNESS:
            return new FourthMomentAggregationFunction(firstArgument, FourthMomentAggregationFunction.Type.SKEWNESS);
          case KURTOSIS:
            return new FourthMomentAggregationFunction(firstArgument, FourthMomentAggregationFunction.Type.KURTOSIS);
          case FOURTHMOMENT:
            return new FourthMomentAggregationFunction(firstArgument, FourthMomentAggregationFunction.Type.MOMENT);
          case DISTINCTCOUNTTUPLESKETCH:
            // mode actually doesn't matter here because we only care about keys, not values
            return new DistinctCountIntegerTupleSketchAggregationFunction(arguments, IntegerSummary.Mode.Sum);
          case DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH:
            return new IntegerTupleSketchAggregationFunction(arguments, IntegerSummary.Mode.Sum);
          case SUMVALUESINTEGERSUMTUPLESKETCH:
            return new SumValuesIntegerTupleSketchAggregationFunction(arguments, IntegerSummary.Mode.Sum);
          case AVGVALUEINTEGERSUMTUPLESKETCH:
            return new AvgValueIntegerTupleSketchAggregationFunction(arguments, IntegerSummary.Mode.Sum);
          case PARENTARGMAX:
            return new ParentArgMinMaxAggregationFunction(arguments, true);
          case PARENTARGMIN:
            return new ParentArgMinMaxAggregationFunction(arguments, false);
          case CHILDARGMAX:
            return new ChildArgMinMaxAggregationFunction(arguments, true);
          case CHILDARGMIN:
            return new ChildArgMinMaxAggregationFunction(arguments, false);
          case ARGMAX:
          case ARGMIN:
            throw new IllegalArgumentException(
                "Aggregation function: " + function + " is only supported in selection without alias.");
          case FUNNELCOUNT:
            return new FunnelCountAggregationFunction(arguments);

          default:
            throw new IllegalArgumentException();
        }
      }
    } catch (Exception e) {
      throw new BadQueryRequestException("Invalid aggregation function: " + function + "; Reason: " + e.getMessage());
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

  private static int parseCompressionFactorToInt(String compressionFactorString) {
    int compressionFactor = Integer.parseInt(compressionFactorString);
    Preconditions.checkArgument(compressionFactor >= 0, "Invalid compressionFactor: %d", compressionFactor);
    return compressionFactor;
  }
}
