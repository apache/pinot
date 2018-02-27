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
package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
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
    PERCENTILE10("percentile10"),
    PERCENTILE20("percentile20"),
    PERCENTILE30("percentile30"),
    PERCENTILE40("percentile40"),
    PERCENTILE50("percentile50"),
    PERCENTILE60("percentile60"),
    PERCENTILE70("percentile70"),
    PERCENTILE80("percentile80"),
    PERCENTILE90("percentile90"),
    PERCENTILE95("percentile95"),
    PERCENTILE99("percentile99"),
    PERCENTILEEST10("percentileEst10"),
    PERCENTILEEST20("percentileEst20"),
    PERCENTILEEST30("percentileEst30"),
    PERCENTILEEST40("percentileEst40"),
    PERCENTILEEST50("percentileEst50"),
    PERCENTILEEST60("percentileEst60"),
    PERCENTILEEST70("percentileEst70"),
    PERCENTILEEST80("percentileEst80"),
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
    PERCENTILE10MV("percentile10MV"),
    PERCENTILE20MV("percentile20MV"),
    PERCENTILE30MV("percentile30MV"),
    PERCENTILE40MV("percentile40MV"),
    PERCENTILE50MV("percentile50MV"),
    PERCENTILE60MV("percentile60MV"),
    PERCENTILE70MV("percentile70MV"),
    PERCENTILE80MV("percentile80MV"),
    PERCENTILE90MV("percentile90MV"),
    PERCENTILE95MV("percentile95MV"),
    PERCENTILE99MV("percentile99MV"),
    PERCENTILEEST10MV("percentileEst10MV"),
    PERCENTILEEST20MV("percentileEst20MV"),
    PERCENTILEEST30MV("percentileEst30MV"),
    PERCENTILEEST40MV("percentileEst40MV"),
    PERCENTILEEST50MV("percentileEst50MV"),
    PERCENTILEEST60MV("percentileEst60MV"),
    PERCENTILEEST70MV("percentileEst70MV"),
    PERCENTILEEST80MV("percentileEst80MV"),
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

    public boolean isOfType(AggregationFunctionType... aggregationFunctionTypes) {
      for (AggregationFunctionType aggFuncType : aggregationFunctionTypes) {
        if (this.equals(aggFuncType)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Given the name of aggregation function, create and return a new instance of the corresponding aggregation function.
   */
  @Nonnull
  public static AggregationFunction getAggregationFunction(@Nonnull String functionName) {
    AggregationFunctionType aggregationFunctionType;
    try {
      aggregationFunctionType = AggregationFunctionType.valueOf(functionName.toUpperCase());
    } catch (Exception e) {
      throw new BadQueryRequestException("Invalid aggregation function name: " + functionName);
    }
    switch (aggregationFunctionType) {
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
      case PERCENTILE10:
        return new PercentileAggregationFunction(10);
      case PERCENTILE20:
        return new PercentileAggregationFunction(20);
      case PERCENTILE30:
        return new PercentileAggregationFunction(30);
      case PERCENTILE40:
        return new PercentileAggregationFunction(40);
      case PERCENTILE50:
        return new PercentileAggregationFunction(50);
      case PERCENTILE60:
        return new PercentileAggregationFunction(60);
      case PERCENTILE70:
        return new PercentileAggregationFunction(70);
      case PERCENTILE80:
        return new PercentileAggregationFunction(80);
      case PERCENTILE90:
        return new PercentileAggregationFunction(90);
      case PERCENTILE95:
        return new PercentileAggregationFunction(95);
      case PERCENTILE99:
        return new PercentileAggregationFunction(99);
      case PERCENTILEEST10:
        return new PercentileEstAggregationFunction(10);
      case PERCENTILEEST20:
        return new PercentileEstAggregationFunction(20);
      case PERCENTILEEST30:
        return new PercentileEstAggregationFunction(30);
      case PERCENTILEEST40:
        return new PercentileEstAggregationFunction(40);
      case PERCENTILEEST50:
        return new PercentileEstAggregationFunction(50);
      case PERCENTILEEST60:
        return new PercentileEstAggregationFunction(60);
      case PERCENTILEEST70:
        return new PercentileEstAggregationFunction(70);
      case PERCENTILEEST80:
        return new PercentileEstAggregationFunction(80);
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
      case PERCENTILE10MV:
        return new PercentileMVAggregationFunction(10);
      case PERCENTILE20MV:
        return new PercentileMVAggregationFunction(20);
      case PERCENTILE30MV:
        return new PercentileMVAggregationFunction(30);
      case PERCENTILE40MV:
        return new PercentileMVAggregationFunction(40);
      case PERCENTILE50MV:
        return new PercentileMVAggregationFunction(50);
      case PERCENTILE60MV:
        return new PercentileMVAggregationFunction(60);
      case PERCENTILE70MV:
        return new PercentileMVAggregationFunction(70);
      case PERCENTILE80MV:
        return new PercentileMVAggregationFunction(80);
      case PERCENTILE90MV:
        return new PercentileMVAggregationFunction(90);
      case PERCENTILE95MV:
        return new PercentileMVAggregationFunction(95);
      case PERCENTILE99MV:
        return new PercentileMVAggregationFunction(99);
      case PERCENTILEEST10MV:
        return new PercentileEstMVAggregationFunction(10);
      case PERCENTILEEST20MV:
        return new PercentileEstMVAggregationFunction(20);
      case PERCENTILEEST30MV:
        return new PercentileEstMVAggregationFunction(30);
      case PERCENTILEEST40MV:
        return new PercentileEstMVAggregationFunction(40);
      case PERCENTILEEST50MV:
        return new PercentileEstMVAggregationFunction(50);
      case PERCENTILEEST60MV:
        return new PercentileEstMVAggregationFunction(60);
      case PERCENTILEEST70MV:
        return new PercentileEstMVAggregationFunction(70);
      case PERCENTILEEST80MV:
        return new PercentileEstMVAggregationFunction(80);
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
