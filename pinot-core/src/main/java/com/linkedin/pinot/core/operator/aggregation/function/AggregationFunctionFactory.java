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

import javax.annotation.Nonnull;


/**
 * Factory class to create instances of aggregation function of the given name.
 */
public class AggregationFunctionFactory {
  private AggregationFunctionFactory() {
  }

  public enum AggregationFunctionType {
    // Single-value aggregation functions.
    COUNT("count"),
    MIN("min"),
    MAX("max"),
    SUM("sum"),
    AVG("avg"),
    MINMAXRANGE("minMaxRange"),
    DISTINCTCOUNT("distinctCount"),
    DISTINCTCOUNTHLL("distinctCountHLL"),
    FASTHLL("fastHLL"),
    PERCENTILE50("percentile50"),
    PERCENTILE90("percentile90"),
    PERCENTILE95("percentile95"),
    PERCENTILE99("percentile99"),
    PERCENTILEEST50("percentileEst50"),
    PERCENTILEEST90("percentileEst90"),
    PERCENTILEEST95("percentileEst95"),
    PERCENTILEEST99("percentileEst99"),
    // Multi-value aggregation functions.
    COUNTMV("countMV"),
    MINMV("minMV"),
    MAXMV("maxMV"),
    SUMMV("sumMV"),
    AVGMV("avgMV"),
    MINMAXRANGEMV("minMaxRangeMV"),
    DISTINCTCOUNTMV("distinctCountMV"),
    DISTINCTCOUNTHLLMV("distinctCountHLLMV"),
    FASTHLLMV("fastHLLMV"),
    PERCENTILE50MV("percentile50MV"),
    PERCENTILE90MV("percentile90MV"),
    PERCENTILE95MV("percentile95MV"),
    PERCENTILE99MV("percentile99MV"),
    PERCENTILEEST50MV("percentileEst50MV"),
    PERCENTILEEST90MV("percentileEst90MV"),
    PERCENTILEEST95MV("percentileEst95MV"),
    PERCENTILEEST99MV("percentileEst99MV");

    private final String _name;

    AggregationFunctionType(@Nonnull String name) {
      _name = name;
    }

    @Nonnull
    public String getName() {
      return _name;
    }
  }

  /**
   * Given the name of aggregation function, create and return a new instance of the corresponding aggregation function.
   */
  @Nonnull
  public static AggregationFunction getAggregationFunction(@Nonnull String functionName) {
    switch (AggregationFunctionType.valueOf(functionName.toUpperCase())) {
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
      case FASTHLL:
        return new FastHLLAggregationFunction();
      case PERCENTILE50:
        return new PercentileAggregationFunction(50);
      case PERCENTILE90:
        return new PercentileAggregationFunction(90);
      case PERCENTILE95:
        return new PercentileAggregationFunction(95);
      case PERCENTILE99:
        return new PercentileAggregationFunction(99);
      case PERCENTILEEST50:
        return new PercentileEstAggregationFunction(50);
      case PERCENTILEEST90:
        return new PercentileEstAggregationFunction(90);
      case PERCENTILEEST95:
        return new PercentileEstAggregationFunction(95);
      case PERCENTILEEST99:
        return new PercentileEstAggregationFunction(99);
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
      case FASTHLLMV:
        return new FastHLLMVAggregationFunction();
      case PERCENTILE50MV:
        return new PercentileMVAggregationFunction(50);
      case PERCENTILE90MV:
        return new PercentileMVAggregationFunction(90);
      case PERCENTILE95MV:
        return new PercentileMVAggregationFunction(95);
      case PERCENTILE99MV:
        return new PercentileMVAggregationFunction(99);
      case PERCENTILEEST50MV:
        return new PercentileEstMVAggregationFunction(50);
      case PERCENTILEEST90MV:
        return new PercentileEstMVAggregationFunction(90);
      case PERCENTILEEST95MV:
        return new PercentileEstMVAggregationFunction(95);
      case PERCENTILEEST99MV:
        return new PercentileEstMVAggregationFunction(99);
      default:
        throw new UnsupportedOperationException();
    }
  }
}
