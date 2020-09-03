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
package org.apache.pinot.common.function;

import org.apache.commons.lang3.StringUtils;


/**
 * NOTE: No underscore is allowed in the enum name.
 */
public enum AggregationFunctionType {
  // Aggregation functions for single-valued columns
  COUNT("count"),
  MIN("min"),
  MAX("max"),
  SUM("sum"),
  AVG("avg"),
  MINMAXRANGE("minMaxRange"),
  DISTINCTCOUNT("distinctCount"),
  DISTINCTCOUNTBITMAP("distinctCountBitmap"),
  SEGMENTPARTITIONEDDISTINCTCOUNT("segmentPartitionedDistinctCount"),
  DISTINCTCOUNTHLL("distinctCountHLL"),
  DISTINCTCOUNTRAWHLL("distinctCountRawHLL"),
  FASTHLL("fastHLL"),
  DISTINCTCOUNTTHETASKETCH("distinctCountThetaSketch"),
  DISTINCTCOUNTRAWTHETASKETCH("distinctCountRawThetaSketch"),
  PERCENTILE("percentile"),
  PERCENTILEEST("percentileEst"),
  PERCENTILETDIGEST("percentileTDigest"),
  RAWTHETASKETCH("rawThetaSketch"),
  IDSET("idSet"),

  // Geo aggregation functions
  STUNION("STUnion"),

  // Aggregation functions for multi-valued columns
  COUNTMV("countMV"),
  MINMV("minMV"),
  MAXMV("maxMV"),
  SUMMV("sumMV"),
  AVGMV("avgMV"),
  MINMAXRANGEMV("minMaxRangeMV"),
  DISTINCTCOUNTMV("distinctCountMV"),
  DISTINCTCOUNTBITMAPMV("distinctCountBitmapMV"),
  DISTINCTCOUNTHLLMV("distinctCountHLLMV"),
  DISTINCTCOUNTRAWHLLMV("distinctCountRawHLLMV"),
  PERCENTILEMV("percentileMV"),
  PERCENTILEESTMV("percentileEstMV"),
  PERCENTILETDIGESTMV("percentileTDigestMV"),
  DISTINCT("distinct");

  private final String _name;

  AggregationFunctionType(String name) {
    _name = name;
  }

  public String getName() {
    return _name;
  }

  public boolean isOfType(AggregationFunctionType... aggregationFunctionTypes) {
    for (AggregationFunctionType aggregationFunctionType : aggregationFunctionTypes) {
      if (this == aggregationFunctionType) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the corresponding aggregation function type for the given function name.
   * <p>NOTE: Underscores in the function name are ignored.
   */
  public static AggregationFunctionType getAggregationFunctionType(String functionName) {
    String upperCaseFunctionName = StringUtils.remove(functionName, '_').toUpperCase();
    if (upperCaseFunctionName.startsWith("PERCENTILE")) {
      String remainingFunctionName = upperCaseFunctionName.substring(10);
      if (remainingFunctionName.isEmpty() || remainingFunctionName.matches("\\d+")) {
        return PERCENTILE;
      } else if (remainingFunctionName.equals("EST") || remainingFunctionName.matches("EST\\d+")) {
        return PERCENTILEEST;
      } else if (remainingFunctionName.equals("TDIGEST") || remainingFunctionName.matches("TDIGEST\\d+")) {
        return PERCENTILETDIGEST;
      } else if (remainingFunctionName.equals("MV") || remainingFunctionName.matches("\\d+MV")) {
        return PERCENTILEMV;
      } else if (remainingFunctionName.equals("ESTMV") || remainingFunctionName.matches("EST\\d+MV")) {
        return PERCENTILEESTMV;
      } else if (remainingFunctionName.equals("TDIGESTMV") || remainingFunctionName.matches("TDIGEST\\d+MV")) {
        return PERCENTILETDIGESTMV;
      } else {
        throw new IllegalArgumentException("Invalid aggregation function name: " + functionName);
      }
    } else {
      try {
        return AggregationFunctionType.valueOf(upperCaseFunctionName);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid aggregation function name: " + functionName);
      }
    }
  }
}
