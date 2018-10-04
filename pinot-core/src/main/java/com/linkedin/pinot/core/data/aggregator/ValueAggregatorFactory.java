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
package com.linkedin.pinot.core.data.aggregator;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;


/**
 * The {@code ValueAggregatorFactory} class is the factory for all value aggregators.
 */
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
      case AVG:
        return new AvgValueAggregator();
      case MINMAXRANGE:
        return new MinMaxRangeValueAggregator();
      case DISTINCTCOUNTHLL:
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
      case AVG:
        return AvgValueAggregator.AGGREGATED_VALUE_TYPE;
      case MINMAXRANGE:
        return MinMaxRangeValueAggregator.AGGREGATED_VALUE_TYPE;
      case DISTINCTCOUNTHLL:
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
