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
  public static ValueAggregator getValueAggregator(AggregationFunctionType aggregationType) {
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
        return new SumPrecisionValueAggregator();
      case AVG:
        return new AvgValueAggregator();
      case MINMAXRANGE:
        return new MinMaxRangeValueAggregator();
      case DISTINCTCOUNTBITMAP:
        return new DistinctCountBitmapValueAggregator();
      case DISTINCTCOUNTHLL:
      case DISTINCTCOUNTRAWHLL:
        return new DistinctCountHLLValueAggregator();
      case PERCENTILEEST:
        return new PercentileEstValueAggregator();
      case PERCENTILETDIGEST:
        return new PercentileTDigestValueAggregator();
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
        return PercentileEstValueAggregator.AGGREGATED_VALUE_TYPE;
      case PERCENTILETDIGEST:
        return PercentileTDigestValueAggregator.AGGREGATED_VALUE_TYPE;
      default:
        throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
    }
  }
}
