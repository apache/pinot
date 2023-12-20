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
package org.apache.pinot.segment.local.aggregator;

import java.util.List;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * The {@code ValueAggregatorFactory} class is the factory for all value aggregators.
 */
@SuppressWarnings("rawtypes")
public class ValueAggregatorFactory {
  private ValueAggregatorFactory() {
  }

  /**
   * Returns a new instance of value aggregator for the given aggregation type.
   *
   * @param aggregationType Aggregation type
   * @return Value aggregator
   */
  public static ValueAggregator getValueAggregator(AggregationFunctionType aggregationType,
      List<ExpressionContext> arguments) {
    switch (aggregationType) {
      case COUNT:
        return new CountValueAggregator();
      case MIN:
        return new MinValueAggregator();
      case MAX:
        return new MaxValueAggregator();
      case SUM:
        return new SumValueAggregator();
      case SUMPRECISION:
        return new SumPrecisionValueAggregator(arguments);
      case AVG:
        return new AvgValueAggregator();
      case MINMAXRANGE:
        return new MinMaxRangeValueAggregator();
      case DISTINCTCOUNTBITMAP:
        return new DistinctCountBitmapValueAggregator();
      case DISTINCTCOUNTHLL:
      case DISTINCTCOUNTRAWHLL:
        return new DistinctCountHLLValueAggregator(arguments);
      case PERCENTILEEST:
      case PERCENTILERAWEST:
        return new PercentileEstValueAggregator();
      case PERCENTILETDIGEST:
      case PERCENTILERAWTDIGEST:
        return new PercentileTDigestValueAggregator();
      case DISTINCTCOUNTTHETASKETCH:
      case DISTINCTCOUNTRAWTHETASKETCH:
        return new DistinctCountThetaSketchValueAggregator();
      case DISTINCTCOUNTHLLPLUS:
      case DISTINCTCOUNTRAWHLLPLUS:
        return new DistinctCountHLLPlusValueAggregator(arguments);
      case DISTINCTCOUNTTUPLESKETCH:
      case DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH:
      case AVGVALUEINTEGERSUMTUPLESKETCH:
      case SUMVALUESINTEGERSUMTUPLESKETCH:
        return new IntegerTupleSketchValueAggregator(IntegerSummary.Mode.Sum);
      case DISTINCTCOUNTCPCSKETCH:
      case DISTINCTCOUNTRAWCPCSKETCH:
        return new DistinctCountCPCSketchValueAggregator(arguments);
      case DISTINCTCOUNTULL:
      case DISTINCTCOUNTRAWULL:
        return new DistinctCountULLValueAggregator(arguments);
      default:
        throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
    }
  }

  /**
   * Returns the data type of the aggregated value for the given aggregation type.
   *
   * @param aggregationType Aggregation type
   * @return Data type of the aggregated value
   */
  public static DataType getAggregatedValueType(AggregationFunctionType aggregationType) {
    switch (aggregationType) {
      case COUNT:
        return CountValueAggregator.AGGREGATED_VALUE_TYPE;
      case MIN:
        return MinValueAggregator.AGGREGATED_VALUE_TYPE;
      case MAX:
        return MaxValueAggregator.AGGREGATED_VALUE_TYPE;
      case SUM:
        return SumValueAggregator.AGGREGATED_VALUE_TYPE;
      case SUMPRECISION:
        return SumPrecisionValueAggregator.AGGREGATED_VALUE_TYPE;
      case AVG:
        return AvgValueAggregator.AGGREGATED_VALUE_TYPE;
      case MINMAXRANGE:
        return MinMaxRangeValueAggregator.AGGREGATED_VALUE_TYPE;
      case DISTINCTCOUNTBITMAP:
        return DistinctCountBitmapValueAggregator.AGGREGATED_VALUE_TYPE;
      case DISTINCTCOUNTHLL:
      case DISTINCTCOUNTRAWHLL:
        return DistinctCountHLLValueAggregator.AGGREGATED_VALUE_TYPE;
      case PERCENTILEEST:
      case PERCENTILERAWEST:
        return PercentileEstValueAggregator.AGGREGATED_VALUE_TYPE;
      case PERCENTILETDIGEST:
      case PERCENTILERAWTDIGEST:
        return PercentileTDigestValueAggregator.AGGREGATED_VALUE_TYPE;
      case DISTINCTCOUNTTHETASKETCH:
      case DISTINCTCOUNTRAWTHETASKETCH:
        return DistinctCountThetaSketchValueAggregator.AGGREGATED_VALUE_TYPE;
      case DISTINCTCOUNTHLLPLUS:
      case DISTINCTCOUNTRAWHLLPLUS:
        return DistinctCountHLLPlusValueAggregator.AGGREGATED_VALUE_TYPE;
      case DISTINCTCOUNTTUPLESKETCH:
      case DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH:
      case AVGVALUEINTEGERSUMTUPLESKETCH:
      case SUMVALUESINTEGERSUMTUPLESKETCH:
        return IntegerTupleSketchValueAggregator.AGGREGATED_VALUE_TYPE;
      case DISTINCTCOUNTCPCSKETCH:
      case DISTINCTCOUNTRAWCPCSKETCH:
        return DistinctCountCPCSketchValueAggregator.AGGREGATED_VALUE_TYPE;
      case DISTINCTCOUNTULL:
      case DISTINCTCOUNTRAWULL:
        return DistinctCountULLValueAggregator.AGGREGATED_VALUE_TYPE;
      default:
        throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
    }
  }

  /**
   * Returns the stored {@link AggregationFunctionType} used to create the underlying value in the segment or index.
   * Some aggregation function types share the same underlying value but are used for different purposes in queries.
   * @param aggregationType the aggregation type used in a query
   * @return the underlying value aggregation type used in storage e.g. StarTree index
   */
  public static AggregationFunctionType getAggregatedFunctionType(AggregationFunctionType aggregationType) {
    switch (aggregationType) {
      case COUNT:
        return CountValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case MIN:
        return MinValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case MAX:
        return MaxValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case SUM:
        return SumValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case SUMPRECISION:
        return SumPrecisionValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case AVG:
        return AvgValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case MINMAXRANGE:
        return MinMaxRangeValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case DISTINCTCOUNTBITMAP:
        return DistinctCountBitmapValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case DISTINCTCOUNTHLL:
      case DISTINCTCOUNTRAWHLL:
        return DistinctCountHLLValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case PERCENTILEEST:
      case PERCENTILERAWEST:
        return PercentileEstValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case PERCENTILETDIGEST:
      case PERCENTILERAWTDIGEST:
        return PercentileTDigestValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case DISTINCTCOUNTTHETASKETCH:
      case DISTINCTCOUNTRAWTHETASKETCH:
        return DistinctCountThetaSketchValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case DISTINCTCOUNTHLLPLUS:
      case DISTINCTCOUNTRAWHLLPLUS:
        return DistinctCountHLLPlusValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case DISTINCTCOUNTTUPLESKETCH:
      case DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH:
      case AVGVALUEINTEGERSUMTUPLESKETCH:
      case SUMVALUESINTEGERSUMTUPLESKETCH:
        return IntegerTupleSketchValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case DISTINCTCOUNTCPCSKETCH:
      case DISTINCTCOUNTRAWCPCSKETCH:
        return DistinctCountCPCSketchValueAggregator.AGGREGATION_FUNCTION_TYPE;
      case DISTINCTCOUNTULL:
      case DISTINCTCOUNTRAWULL:
        return DistinctCountULLValueAggregator.AGGREGATION_FUNCTION_TYPE;
      default:
        throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
    }
  }
}
