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
package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
import javax.annotation.Nonnull;


public enum AggregationFunctionType {
  // Aggregation functions for single-valued columns
  COUNT("count"),
  MIN("min"),
  MAX("max"),
  SUM("sum"),
  AVG("avg"),
  MINMAXRANGE("minMaxRange"),
  DISTINCTCOUNT("distinctCount"),
  DISTINCTCOUNTHLL("distinctCountHLL"),
  FASTHLL("fastHLL"),
  PERCENTILE("percentile"),
  PERCENTILEEST("percentileEst"),
  PERCENTILETDIGEST("percentileTDigest"),
  // Aggregation functions for multi-valued columns
  COUNTMV("countMV"),
  MINMV("minMV"),
  MAXMV("maxMV"),
  SUMMV("sumMV"),
  AVGMV("avgMV"),
  MINMAXRANGEMV("minMaxRangeMV"),
  DISTINCTCOUNTMV("distinctCountMV"),
  DISTINCTCOUNTHLLMV("distinctCountHLLMV"),
  PERCENTILEMV("percentileMV"),
  PERCENTILEESTMV("percentileEstMV"),
  PERCENTILETDIGESTMV("percentileTDigestMV");

  private final String _name;

  AggregationFunctionType(@Nonnull String name) {
    _name = name;
  }

  @Nonnull
  public String getName() {
    return _name;
  }

  public boolean isOfType(@Nonnull AggregationFunctionType... aggregationFunctionTypes) {
    for (AggregationFunctionType aggregationFunctionType : aggregationFunctionTypes) {
      if (this == aggregationFunctionType) {
        return true;
      }
    }
    return false;
  }

  /**
   * Given the name of the aggregation function, returns the corresponding aggregation function type.
   */
  @Nonnull
  public static AggregationFunctionType getAggregationFunctionType(@Nonnull String functionName) {
    try {
      String upperCaseFunctionName = functionName.toUpperCase();
      if (upperCaseFunctionName.startsWith("PERCENTILE")) {
        String remainingFunctionName = upperCaseFunctionName.substring(10);
        if (remainingFunctionName.matches("\\d+")) {
          return PERCENTILE;
        } else if (remainingFunctionName.matches("EST\\d+")) {
          return PERCENTILEEST;
        } else if (remainingFunctionName.matches("TDIGEST\\d+")) {
          return PERCENTILETDIGEST;
        } else if (remainingFunctionName.matches("\\d+MV")) {
          return PERCENTILEMV;
        } else if (remainingFunctionName.matches("EST\\d+MV")) {
          return PERCENTILEESTMV;
        } else if (remainingFunctionName.matches("TDIGEST\\d+MV")) {
          return PERCENTILETDIGESTMV;
        } else {
          throw new IllegalArgumentException();
        }
      } else {
        return AggregationFunctionType.valueOf(upperCaseFunctionName);
      }
    } catch (Exception e) {
      throw new BadQueryRequestException("Invalid aggregation function name: " + functionName);
    }
  }
}
