/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.aggregation.function;

import com.linkedin.pinot.common.segment.SegmentMetadata;


/**
 * Factory class to create instances of aggregation function of the given name.
 */
public class AggregationFunctionFactory {
  public static final String COUNT_AGGREGATION_FUNCTION = "count";
  public static final String MAX_AGGREGATION_FUNCTION = "max";
  public static final String MIN_AGGREGATION_FUNCTION = "min";
  public static final String SUM_AGGREGATION_FUNCTION = "sum";
  public static final String AVG_AGGREGATION_FUNCTION = "avg";
  public static final String MINMAXRANGE_AGGREGATION_FUNCTION = "minmaxrange";
  public static final String DISTINCTCOUNT_AGGREGATION_FUNCTION = "distinctcount";
  public static final String DISTINCTCOUNTHLL_AGGREGATION_FUNCTION = "distinctcounthll";
  public static final String FASTHLL_AGGREGATION_FUNCTION = "fasthll";
  public static final String PERCENTILE50_AGGREGATION_FUNCTION = "percentile50";
  public static final String PERCENTILE90_AGGREGATION_FUNCTION = "percentile90";
  public static final String PERCENTILE95_AGGREGATION_FUNCTION = "percentile95";
  public static final String PERCENTILE99_AGGREGATION_FUNCTION = "percentile99";
  public static final String PERCENTILEEST50_AGGREGATION_FUNCTION = "percentileest50";
  public static final String PERCENTILEEST90_AGGREGATION_FUNCTION = "percentileest90";
  public static final String PERCENTILEEST95_AGGREGATION_FUNCTION = "percentileest95";
  public static final String PERCENTILEEST99_AGGREGATION_FUNCTION = "percentileest99";

  public static final String COUNT_MV_AGGREGATION_FUNCTION = "countmv";
  public static final String MAX_MV_AGGREGATION_FUNCTION = "maxmv";
  public static final String MIN_MV_AGGREGATION_FUNCTION = "minmv";
  public static final String SUM_MV_AGGREGATION_FUNCTION = "summv";
  public static final String AVG_MV_AGGREGATION_FUNCTION = "avgmv";
  public static final String MINMAXRANGE_MV_AGGREGATION_FUNCTION = "minmaxrangemv";
  public static final String DISTINCTCOUNT_MV_AGGREGATION_FUNCTION = "distinctcountmv";
  public static final String DISTINCTCOUNTHLL_MV_AGGREGATION_FUNCTION = "distinctcounthllmv";
  public static final String PERCENTILE50_MV_AGGREGATION_FUNCTION = "percentile50mv";
  public static final String PERCENTILE90_MV_AGGREGATION_FUNCTION = "percentile90mv";
  public static final String PERCENTILE95_MV_AGGREGATION_FUNCTION = "percentile95mv";
  public static final String PERCENTILE99_MV_AGGREGATION_FUNCTION = "percentile99mv";
  public static final String PERCENTILEEST50_MV_AGGREGATION_FUNCTION = "percentileest50mv";
  public static final String PERCENTILEEST90_MV_AGGREGATION_FUNCTION = "percentileest90mv";
  public static final String PERCENTILEEST95_MV_AGGREGATION_FUNCTION = "percentileest95mv";
  public static final String PERCENTILEEST99_MV_AGGREGATION_FUNCTION = "percentileest99mv";

  /**
   * Given the name of aggregation function, create and return a new instance
   * of the corresponding aggregation function and return.
   *
   * @param functionName
   * @return
   */
  public static AggregationFunction getAggregationFunction(String functionName, SegmentMetadata segmentMetadata) {
    switch (functionName.toLowerCase()) {
      case COUNT_AGGREGATION_FUNCTION:
        return new CountAggregationFunction();

      case MIN_AGGREGATION_FUNCTION:
        return new MinAggregationFunction();

      case MAX_AGGREGATION_FUNCTION:
        return new MaxAggregationFunction();

      case SUM_AGGREGATION_FUNCTION:
        return new SumAggregationFunction();

      case AVG_AGGREGATION_FUNCTION:
        return new AvgAggregationFunction();

      case MINMAXRANGE_AGGREGATION_FUNCTION:
        return new MinMaxRangeAggregationFunction();

      case DISTINCTCOUNT_AGGREGATION_FUNCTION:
        return new DistinctCountAggregationFunction();

      case DISTINCTCOUNTHLL_AGGREGATION_FUNCTION:
        return new DistinctCountHLLAggregationFunction();

      case FASTHLL_AGGREGATION_FUNCTION:
        return new FastHllAggregationFunction(segmentMetadata.getHllLog2m());

      case PERCENTILE50_AGGREGATION_FUNCTION:
        return new PercentileAggregationFunction(50);

      case PERCENTILE90_AGGREGATION_FUNCTION:
        return new PercentileAggregationFunction(90);

      case PERCENTILE95_AGGREGATION_FUNCTION:
        return new PercentileAggregationFunction(95);

      case PERCENTILE99_AGGREGATION_FUNCTION:
        return new PercentileAggregationFunction(99);

      case PERCENTILEEST50_AGGREGATION_FUNCTION:
        return new PercentileestAggregationFunction(50);

      case PERCENTILEEST90_AGGREGATION_FUNCTION:
        return new PercentileestAggregationFunction(90);

      case PERCENTILEEST95_AGGREGATION_FUNCTION:
        return new PercentileestAggregationFunction(95);

      case PERCENTILEEST99_AGGREGATION_FUNCTION:
        return new PercentileestAggregationFunction(99);

      case COUNT_MV_AGGREGATION_FUNCTION:
        return new CountMVAggregationFunction();

      case MIN_MV_AGGREGATION_FUNCTION:
        return new MinMVAggregationFunction();

      case MAX_MV_AGGREGATION_FUNCTION:
        return new MaxMVAggregationFunction();

      case SUM_MV_AGGREGATION_FUNCTION:
        return new SumMVAggregationFunction();

      case AVG_MV_AGGREGATION_FUNCTION:
        return new AvgMVAggregationFunction();

      case MINMAXRANGE_MV_AGGREGATION_FUNCTION:
        return new MinMaxRangeMVAggregationFunction();

      case DISTINCTCOUNT_MV_AGGREGATION_FUNCTION:
        return new DistinctCountMVAggregationFunction();

      case DISTINCTCOUNTHLL_MV_AGGREGATION_FUNCTION:
        return new DistinctCountHLLMVAggregationFunction();

      case PERCENTILE50_MV_AGGREGATION_FUNCTION:
        return new PercentileMVAggregationFunction(50);

      case PERCENTILE90_MV_AGGREGATION_FUNCTION:
        return new PercentileMVAggregationFunction(90);

      case PERCENTILE95_MV_AGGREGATION_FUNCTION:
        return new PercentileMVAggregationFunction(95);

      case PERCENTILE99_MV_AGGREGATION_FUNCTION:
        return new PercentileMVAggregationFunction(99);

      case PERCENTILEEST50_MV_AGGREGATION_FUNCTION:
        return new PercentileestMVAggregationFunction(50);

      case PERCENTILEEST90_MV_AGGREGATION_FUNCTION:
        return new PercentileestMVAggregationFunction(90);

      case PERCENTILEEST95_MV_AGGREGATION_FUNCTION:
        return new PercentileestMVAggregationFunction(95);

      case PERCENTILEEST99_MV_AGGREGATION_FUNCTION:
        return new PercentileestMVAggregationFunction(99);

      default:
        throw new RuntimeException("Unsupported aggregation function: " + functionName);
    }
  }
}
