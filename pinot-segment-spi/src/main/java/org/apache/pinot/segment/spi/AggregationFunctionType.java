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
package org.apache.pinot.segment.spi;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * NOTE: No underscore is allowed in the enum name.
 */
public enum AggregationFunctionType {
  // Aggregation functions for single-valued columns
  COUNT("count"),
  MIN("min"),
  MAX("max"),
  SUM("sum"),
  SUMPRECISION("sumPrecision"),
  AVG("avg"),
  MODE("mode"),
  FIRSTWITHTIME("firstWithTime"),
  LASTWITHTIME("lastWithTime"),
  MINMAXRANGE("minMaxRange"),
  DISTINCTCOUNT("distinctCount"),
  DISTINCTCOUNTBITMAP("distinctCountBitmap"),
  SEGMENTPARTITIONEDDISTINCTCOUNT("segmentPartitionedDistinctCount"),
  DISTINCTCOUNTHLL("distinctCountHLL"),
  DISTINCTCOUNTRAWHLL("distinctCountRawHLL"),
  DISTINCTCOUNTSMARTHLL("distinctCountSmartHLL"),
  FASTHLL("fastHLL"),
  DISTINCTCOUNTTHETASKETCH("distinctCountThetaSketch"),
  DISTINCTCOUNTRAWTHETASKETCH("distinctCountRawThetaSketch"),
  DISTINCTSUM("distinctSum"),
  DISTINCTAVG("distinctAvg"),
  PERCENTILE("percentile"),
  PERCENTILEEST("percentileEst"),
  PERCENTILERAWEST("percentileRawEst"),
  PERCENTILETDIGEST("percentileTDigest"),
  PERCENTILERAWTDIGEST("percentileRawTDigest"),
  PERCENTILESMARTTDIGEST("percentileSmartTDigest"),
  PERCENTILEKLL("percentileKLL"),
  PERCENTILERAWKLL("percentileRawKLL"),
  IDSET("idSet"),
  HISTOGRAM("histogram"),
  COVARPOP("covarPop"),
  COVARSAMP("covarSamp"),
  VARPOP("varPop"),
  VARSAMP("varSamp"),
  STDDEVPOP("stdDevPop"),
  STDDEVSAMP("stdDevSamp"),
  SKEWNESS("skewness"),
  KURTOSIS("kurtosis"),
  FOURTHMOMENT("fourthmoment"),

  // DataSketches Tuple Sketch support
  DISTINCTCOUNTTUPLESKETCH("distinctCountTupleSketch"),

  // DataSketches Tuple Sketch support for Integer based Tuple Sketches
  DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH("distinctCountRawIntegerSumTupleSketch"),

  SUMVALUESINTEGERSUMTUPLESKETCH("sumValuesIntegerSumTupleSketch"),
  AVGVALUEINTEGERSUMTUPLESKETCH("avgValueIntegerSumTupleSketch"),

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
  DISTINCTSUMMV("distinctSumMV"),
  DISTINCTAVGMV("distinctAvgMV"),
  PERCENTILEMV("percentileMV"),
  PERCENTILEESTMV("percentileEstMV"),
  PERCENTILERAWESTMV("percentileRawEstMV"),
  PERCENTILETDIGESTMV("percentileTDigestMV"),
  PERCENTILERAWTDIGESTMV("percentileRawTDigestMV"),
  PERCENTILEKLLMV("percentileKLLMV"),
  PERCENTILERAWKLLMV("percentileRawKLLMV"),

  // boolean aggregate functions
  BOOLAND("boolAnd"),
  BOOLOR("boolOr"),

  // argMin and argMax
  ARGMIN("argMin"),
  ARGMAX("argMax"),
  PARENTARGMIN(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + ARGMIN.getName()),
  PARENTARGMAX(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + ARGMAX.getName()),
  CHILDARGMIN(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX + ARGMIN.getName()),
  CHILDARGMAX(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX + ARGMAX.getName()),

  // funnel aggregate functions
  FUNNELCOUNT("funnelCount");

  private static final Set<String> NAMES = Arrays.stream(values()).flatMap(func -> Stream.of(func.name(),
      func.getName(), func.getName().toLowerCase())).collect(Collectors.toSet());

  private final String _name;

  AggregationFunctionType(String name) {
    _name = name;
  }

  public String getName() {
    return _name;
  }

  public static boolean isAggregationFunction(String functionName) {
    if (NAMES.contains(functionName)) {
      return true;
    }
    if (functionName.regionMatches(true, 0, "percentile", 0, 10)) {
      try {
        getAggregationFunctionType(functionName);
        return true;
      } catch (Exception ignore) {
        return false;
      }
    }
    String upperCaseFunctionName = StringUtils.remove(functionName, '_').toUpperCase();
    return NAMES.contains(upperCaseFunctionName);
  }

  /**
   * Returns the corresponding aggregation function type for the given function name.
   * <p>NOTE: Underscores in the function name are ignored.
   */
  public static AggregationFunctionType getAggregationFunctionType(String functionName) {
    if (functionName.regionMatches(true, 0, "percentile", 0, 10)) {
      String remainingFunctionName = StringUtils.remove(functionName, '_').substring(10).toUpperCase();
      if (remainingFunctionName.isEmpty() || remainingFunctionName.matches("\\d+")) {
        return PERCENTILE;
      } else if (remainingFunctionName.equals("EST") || remainingFunctionName.matches("EST\\d+")) {
        return PERCENTILEEST;
      } else if (remainingFunctionName.equals("RAWEST") || remainingFunctionName.matches("RAWEST\\d+")) {
        return PERCENTILERAWEST;
      } else if (remainingFunctionName.equals("TDIGEST") || remainingFunctionName.matches("TDIGEST\\d+")) {
        return PERCENTILETDIGEST;
      } else if (remainingFunctionName.equals("RAWTDIGEST") || remainingFunctionName.matches("RAWTDIGEST\\d+")) {
        return PERCENTILERAWTDIGEST;
      } else if (remainingFunctionName.equals("KLL") || remainingFunctionName.matches("KLL\\d+")) {
        return PERCENTILEKLL;
      } else if (remainingFunctionName.equals("RAWKLL") || remainingFunctionName.matches("RAWKLL\\d+")) {
        return PERCENTILERAWKLL;
      } else if (remainingFunctionName.equals("MV") || remainingFunctionName.matches("\\d+MV")) {
        return PERCENTILEMV;
      } else if (remainingFunctionName.equals("ESTMV") || remainingFunctionName.matches("EST\\d+MV")) {
        return PERCENTILEESTMV;
      } else if (remainingFunctionName.equals("RAWESTMV") || remainingFunctionName.matches("RAWEST\\d+MV")) {
        return PERCENTILERAWESTMV;
      } else if (remainingFunctionName.equals("TDIGESTMV") || remainingFunctionName.matches("TDIGEST\\d+MV")) {
        return PERCENTILETDIGESTMV;
      } else if (remainingFunctionName.equals("RAWTDIGESTMV") || remainingFunctionName.matches("RAWTDIGEST\\d+MV")) {
        return PERCENTILERAWTDIGESTMV;
      } else if (remainingFunctionName.equals("KLLMV") || remainingFunctionName.matches("KLL\\d+MV")) {
        return PERCENTILEKLLMV;
      } else if (remainingFunctionName.equals("RAWKLLMV") || remainingFunctionName.matches("RAWKLL\\d+MV")) {
        return PERCENTILEKLLMV;
      } else {
        throw new IllegalArgumentException("Invalid aggregation function name: " + functionName);
      }
    } else {
      try {
        return AggregationFunctionType.valueOf(StringUtils.remove(functionName, '_').toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid aggregation function name: " + functionName);
      }
    }
  }
}
