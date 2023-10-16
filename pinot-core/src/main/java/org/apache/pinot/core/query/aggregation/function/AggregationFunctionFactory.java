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
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelCountAggregationFunctionFactory;
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
      String upperCaseFunctionName =
          AggregationFunctionType.getNormalizedAggregationFunctionName(function.getFunctionName());
      List<ExpressionContext> arguments = function.getArguments();
      int numArguments = arguments.size();
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
          double percentile = arguments.get(1).getLiteral().getDoubleValue();
          Preconditions.checkArgument(percentile >= 0 && percentile <= 100, "Invalid percentile: %s", percentile);
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
          double percentile = arguments.get(1).getLiteral().getDoubleValue();
          Preconditions.checkArgument(percentile >= 0 && percentile <= 100, "Invalid percentile: %s", percentile);
          int compressionFactor = arguments.get(2).getLiteral().getIntValue();
          Preconditions.checkArgument(compressionFactor >= 0, "Invalid compressionFactor: %d", compressionFactor);
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
        AggregationFunctionType functionType = AggregationFunctionType.valueOf(upperCaseFunctionName);
        switch (functionType) {
          case COUNT:
            return new CountAggregationFunction(arguments, nullHandlingEnabled);
          case MIN:
            return new MinAggregationFunction(arguments, nullHandlingEnabled);
          case MAX:
            return new MaxAggregationFunction(arguments, nullHandlingEnabled);
          case SUM:
          case SUM0:
            return new SumAggregationFunction(arguments, nullHandlingEnabled);
          case SUMPRECISION:
            return new SumPrecisionAggregationFunction(arguments, nullHandlingEnabled);
          case AVG:
            return new AvgAggregationFunction(arguments, nullHandlingEnabled);
          case MODE:
            return new ModeAggregationFunction(arguments);
          case FIRSTWITHTIME: {
            Preconditions.checkArgument(numArguments == 3,
                "FIRST_WITH_TIME expects 3 arguments, got: %s. The function can be used as "
                    + "firstWithTime(dataColumn, timeColumn, 'dataType')", numArguments);
            ExpressionContext timeCol = arguments.get(1);
            ExpressionContext dataTypeExp = arguments.get(2);
            Preconditions.checkArgument(dataTypeExp.getType() == ExpressionContext.Type.LITERAL,
                "FIRST_WITH_TIME expects the 3rd argument to be literal, got: %s. The function can be used as "
                    + "firstWithTime(dataColumn, timeColumn, 'dataType')", dataTypeExp.getType());
            DataType dataType = DataType.valueOf(dataTypeExp.getLiteral().getStringValue().toUpperCase());
            switch (dataType) {
              case BOOLEAN:
                return new FirstIntValueWithTimeAggregationFunction(firstArgument, timeCol, true);
              case INT:
                return new FirstIntValueWithTimeAggregationFunction(firstArgument, timeCol, false);
              case LONG:
                return new FirstLongValueWithTimeAggregationFunction(firstArgument, timeCol);
              case FLOAT:
                return new FirstFloatValueWithTimeAggregationFunction(firstArgument, timeCol);
              case DOUBLE:
                return new FirstDoubleValueWithTimeAggregationFunction(firstArgument, timeCol);
              case STRING:
                return new FirstStringValueWithTimeAggregationFunction(firstArgument, timeCol);
              default:
                throw new IllegalArgumentException("Unsupported data type for FIRST_WITH_TIME: " + dataType);
            }
          }
          case LASTWITHTIME: {
            Preconditions.checkArgument(numArguments == 3,
                "LAST_WITH_TIME expects 3 arguments, got: %s. The function can be used as "
                    + "lastWithTime(dataColumn, timeColumn, 'dataType')", numArguments);
            ExpressionContext timeCol = arguments.get(1);
            ExpressionContext dataTypeExp = arguments.get(2);
            Preconditions.checkArgument(dataTypeExp.getType() == ExpressionContext.Type.LITERAL,
                "LAST_WITH_TIME expects the 3rd argument to be literal, got: %s. The function can be used as "
                    + "lastWithTime(dataColumn, timeColumn, 'dataType')", dataTypeExp.getType());
            DataType dataType = DataType.valueOf(dataTypeExp.getLiteral().getStringValue().toUpperCase());
            switch (dataType) {
              case BOOLEAN:
                return new LastIntValueWithTimeAggregationFunction(firstArgument, timeCol, true);
              case INT:
                return new LastIntValueWithTimeAggregationFunction(firstArgument, timeCol, false);
              case LONG:
                return new LastLongValueWithTimeAggregationFunction(firstArgument, timeCol);
              case FLOAT:
                return new LastFloatValueWithTimeAggregationFunction(firstArgument, timeCol);
              case DOUBLE:
                return new LastDoubleValueWithTimeAggregationFunction(firstArgument, timeCol);
              case STRING:
                return new LastStringValueWithTimeAggregationFunction(firstArgument, timeCol);
              default:
                throw new IllegalArgumentException("Unsupported data type for LAST_WITH_TIME: " + dataType);
            }
          }
          case MINMAXRANGE:
            return new MinMaxRangeAggregationFunction(arguments);
          case DISTINCTCOUNT:
            return new DistinctCountAggregationFunction(arguments, nullHandlingEnabled);
          case DISTINCTCOUNTBITMAP:
            return new DistinctCountBitmapAggregationFunction(arguments);
          case SEGMENTPARTITIONEDDISTINCTCOUNT:
            return new SegmentPartitionedDistinctCountAggregationFunction(arguments);
          case DISTINCTCOUNTHLL:
            return new DistinctCountHLLAggregationFunction(arguments);
          case DISTINCTCOUNTRAWHLL:
            return new DistinctCountRawHLLAggregationFunction(arguments);
          case DISTINCTCOUNTSMARTHLL:
            return new DistinctCountSmartHLLAggregationFunction(arguments);
          case FASTHLL:
            return new FastHLLAggregationFunction(arguments);
          case DISTINCTCOUNTTHETASKETCH:
            return new DistinctCountThetaSketchAggregationFunction(arguments);
          case DISTINCTCOUNTRAWTHETASKETCH:
            return new DistinctCountRawThetaSketchAggregationFunction(arguments);
          case DISTINCTSUM:
            return new DistinctSumAggregationFunction(arguments, nullHandlingEnabled);
          case DISTINCTAVG:
            return new DistinctAvgAggregationFunction(arguments, nullHandlingEnabled);
          case IDSET:
            return new IdSetAggregationFunction(arguments);
          case COUNTMV:
            return new CountMVAggregationFunction(arguments);
          case MINMV:
            return new MinMVAggregationFunction(arguments);
          case MAXMV:
            return new MaxMVAggregationFunction(arguments);
          case SUMMV:
            return new SumMVAggregationFunction(arguments);
          case AVGMV:
            return new AvgMVAggregationFunction(arguments);
          case MINMAXRANGEMV:
            return new MinMaxRangeMVAggregationFunction(arguments);
          case DISTINCTCOUNTMV:
            return new DistinctCountMVAggregationFunction(arguments);
          case DISTINCTCOUNTBITMAPMV:
            return new DistinctCountBitmapMVAggregationFunction(arguments);
          case DISTINCTCOUNTHLLMV:
            return new DistinctCountHLLMVAggregationFunction(arguments);
          case DISTINCTCOUNTRAWHLLMV:
            return new DistinctCountRawHLLMVAggregationFunction(arguments);
          case DISTINCTCOUNTHLLPLUS:
            return new DistinctCountHLLPlusAggregationFunction(arguments);
          case DISTINCTCOUNTRAWHLLPLUS:
            return new DistinctCountRawHLLPlusAggregationFunction(arguments);
          case DISTINCTCOUNTHLLPLUSMV:
            return new DistinctCountHLLPlusMVAggregationFunction(arguments);
          case DISTINCTCOUNTRAWHLLPLUSMV:
            return new DistinctCountRawHLLPlusMVAggregationFunction(arguments);
          case DISTINCTSUMMV:
            return new DistinctSumMVAggregationFunction(arguments);
          case DISTINCTAVGMV:
            return new DistinctAvgMVAggregationFunction(arguments);
          case STUNION:
            return new StUnionAggregationFunction(arguments);
          case HISTOGRAM:
            return new HistogramAggregationFunction(arguments);
          case COVARPOP:
            return new CovarianceAggregationFunction(arguments, false);
          case COVARSAMP:
            return new CovarianceAggregationFunction(arguments, true);
          case BOOLAND:
            return new BooleanAndAggregationFunction(arguments, nullHandlingEnabled);
          case BOOLOR:
            return new BooleanOrAggregationFunction(arguments, nullHandlingEnabled);
          case VARPOP:
            return new VarianceAggregationFunction(arguments, false, false, nullHandlingEnabled);
          case VARSAMP:
            return new VarianceAggregationFunction(arguments, true, false, nullHandlingEnabled);
          case STDDEVPOP:
            return new VarianceAggregationFunction(arguments, false, true, nullHandlingEnabled);
          case STDDEVSAMP:
            return new VarianceAggregationFunction(arguments, true, true, nullHandlingEnabled);
          case SKEWNESS:
            return new FourthMomentAggregationFunction(arguments, FourthMomentAggregationFunction.Type.SKEWNESS);
          case KURTOSIS:
            return new FourthMomentAggregationFunction(arguments, FourthMomentAggregationFunction.Type.KURTOSIS);
          case FOURTHMOMENT:
            return new FourthMomentAggregationFunction(arguments, FourthMomentAggregationFunction.Type.MOMENT);
          case DISTINCTCOUNTTUPLESKETCH:
            // mode actually doesn't matter here because we only care about keys, not values
            return new DistinctCountIntegerTupleSketchAggregationFunction(arguments, IntegerSummary.Mode.Sum);
          case DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH:
            return new IntegerTupleSketchAggregationFunction(arguments, IntegerSummary.Mode.Sum);
          case SUMVALUESINTEGERSUMTUPLESKETCH:
            return new SumValuesIntegerTupleSketchAggregationFunction(arguments, IntegerSummary.Mode.Sum);
          case AVGVALUEINTEGERSUMTUPLESKETCH:
            return new AvgValueIntegerTupleSketchAggregationFunction(arguments, IntegerSummary.Mode.Sum);
          case PARENTEXPRMAX:
            return new ParentExprMinMaxAggregationFunction(arguments, true);
          case PARENTEXPRMIN:
            return new ParentExprMinMaxAggregationFunction(arguments, false);
          case CHILDEXPRMAX:
            return new ChildExprMinMaxAggregationFunction(arguments, true);
          case CHILDEXPRMIN:
            return new ChildExprMinMaxAggregationFunction(arguments, false);
          case EXPRMAX:
          case EXPRMIN:
            throw new IllegalArgumentException(
                "Aggregation function: " + functionType + " is only supported in selection without alias.");
          case FUNNELCOUNT:
            return new FunnelCountAggregationFunctionFactory(arguments).get();
          case FREQUENTSTRINGSSKETCH:
            return new FrequentStringsSketchAggregationFunction(arguments);
          case FREQUENTLONGSSKETCH:
            return new FrequentLongsSketchAggregationFunction(arguments);
          case DISTINCTCOUNTCPCSKETCH:
            return new DistinctCountCPCSketchAggregationFunction(arguments);
          case DISTINCTCOUNTRAWCPCSKETCH:
            return new DistinctCountRawCPCSketchAggregationFunction(arguments);

          default:
            throw new IllegalArgumentException("Unsupported aggregation function type: " + functionType);
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
}
