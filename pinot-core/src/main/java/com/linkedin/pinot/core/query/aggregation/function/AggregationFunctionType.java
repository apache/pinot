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
  // Aggregation functions for multi-valued columns
  COUNTMV("countMV"),
  MINMV("minMV"),
  MAXMV("maxMV"),
  SUMMV("sumMV"),
  AVGMV("avgMV"),
  MINMAXRANGEMV("minMaxRangeMV"),
  DISTINCTCOUNTMV("distinctCountMV"),
  DISTINCTCOUNTHLLMV("distinctCountHLLMV"),
  FASTHLLMV("fastHLLMV");

  private final String _name;

  AggregationFunctionType(@Nonnull String name) {
    _name = name;
  }

  @Nonnull
  public String getName() {
    return _name;
  }

  public static boolean isOfType(@Nonnull String functionName,
      @Nonnull AggregationFunctionType... aggregationFunctionTypes) {
    String upperCaseFunctionName = functionName.toUpperCase();
    for (AggregationFunctionType aggregationFunctionType : aggregationFunctionTypes) {
      if (upperCaseFunctionName.equals(aggregationFunctionType.name())) {
        return true;
      }
    }
    return false;
  }
}
