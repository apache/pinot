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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * NOTES:
 * - No underscore is allowed in the enum name.
 * - '$' is allowed in the name field but not in the enum name.
 *
 * This enum is used both in the v1 engine and multistage engine to define the allowed Pinot aggregation functions.
 * The v1 engine only relies on the 'name' field, whereas all the other fields are used in the multistage engine
 * to register the aggregation function with Calcite. This allows using a unified approach to aggregations across both
 * the v1 and multistage engines.
 */
public enum AggregationFunctionType {
  // Aggregation functions for single-valued columns
  COUNT("count", Collections.emptyList(), true, null, SqlKind.COUNT, ReturnTypes.BIGINT, null,
      CalciteSystemProperty.STRICT.value() ? OperandTypes.ANY : OperandTypes.ONE_OR_MORE, SqlFunctionCategory.NUMERIC,
      null, ReturnTypes.BIGINT, null, null, null),
  MIN("min", Collections.emptyList(), false, null, SqlKind.MIN, ReturnTypes.DOUBLE, null,
      OperandTypes.COMPARABLE_ORDERED, SqlFunctionCategory.SYSTEM, null, ReturnTypes.DOUBLE, null, null, null),
  MAX("max", Collections.emptyList(), false, null, SqlKind.MAX, ReturnTypes.DOUBLE, null,
      OperandTypes.COMPARABLE_ORDERED, SqlFunctionCategory.SYSTEM, null, ReturnTypes.DOUBLE, null, null, null),
  // In multistage SUM is reduced via the PinotAvgSumAggregateReduceFunctionsRule, need not set up anything for reduce
  SUM("sum", Collections.emptyList(), false, null, SqlKind.SUM, ReturnTypes.DOUBLE, null, OperandTypes.NUMERIC,
      SqlFunctionCategory.NUMERIC, null, ReturnTypes.DOUBLE, null, null, null),
  SUM0("$sum0", Collections.emptyList(), false, null, SqlKind.SUM0, ReturnTypes.DOUBLE, null, OperandTypes.NUMERIC,
      SqlFunctionCategory.NUMERIC, null, ReturnTypes.DOUBLE, null, null, null),
  SUMPRECISION("sumPrecision"),
  AVG("avg", Collections.emptyList(), true, null, SqlKind.AVG, ReturnTypes.AVG_AGG_FUNCTION, null,
      OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC, null, null, "AVG_REDUCE", ReturnTypes.AVG_AGG_FUNCTION,
      OperandTypes.NUMERIC_NUMERIC),
  MODE("mode"),
  FIRSTWITHTIME("firstWithTime"),
  LASTWITHTIME("lastWithTime"),
  MINMAXRANGE("minMaxRange"),
  DISTINCTCOUNT("distinctCount", Collections.emptyList(), false, null, SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT,
      null, OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION, null,
      ReturnTypes.explicit(SqlTypeName.OTHER), null, null, null),
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
  SKEWNESS("skewness", Collections.emptyList(), false, null, SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
      OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION, "fourthMoment",
      ReturnTypes.explicit(SqlTypeName.OTHER), null, null, null),
  KURTOSIS("kurtosis", Collections.emptyList(), false, null, SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
      OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION, "fourthMoment",
      ReturnTypes.explicit(SqlTypeName.OTHER), null, null, null),
  FOURTHMOMENT("fourthMoment"),

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
  BOOLAND("boolAnd", Collections.emptyList(), false, null, SqlKind.OTHER_FUNCTION, ReturnTypes.BOOLEAN, null,
      OperandTypes.BOOLEAN, SqlFunctionCategory.USER_DEFINED_FUNCTION, null, ReturnTypes.BOOLEAN, null, null, null),
  BOOLOR("boolOr", Collections.emptyList(), false, null, SqlKind.OTHER_FUNCTION, ReturnTypes.BOOLEAN, null,
      OperandTypes.BOOLEAN, SqlFunctionCategory.USER_DEFINED_FUNCTION, null, ReturnTypes.BOOLEAN, null, null, null),

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
  private final List<String> _alternativeNames;
  private final boolean _isNativeCalciteAggregationFunctionType;

  // Fields for registering the aggregation function with Calcite in multistage. These are typically used for the
  // user facing aggregation functions and the return and operand types should reflect that which is user facing.
  private final SqlIdentifier _sqlIdentifier;
  private final SqlKind _sqlKind;
  private final SqlReturnTypeInference _sqlReturnTypeInference;
  private final SqlOperandTypeInference _sqlOperandTypeInference;
  private final SqlOperandTypeChecker _sqlOperandTypeChecker;
  private final SqlFunctionCategory _sqlFunctionCategory;

  // Fields for the intermediate stage functions used in multistage. These intermediate stage aggregation functions
  // are typically internal to Pinot and are used to handle the complex types used in the intermediate and final
  // aggregation stages. The intermediate function name may be the same as the user facing function name.
  private final String _intermediateFunctionName;
  private final SqlReturnTypeInference _sqlIntermediateReturnTypeInference;

  // Fields for the Calcite aggregation reduce step in multistage. The aggregation reduce is only used for special
  // functions like AVG which is split into SUM / COUNT. This is needed for proper null handling if COUNT is 0.
  private final String _reduceFunctionName;
  private final SqlReturnTypeInference _sqlReduceReturnTypeInference;
  private final SqlOperandTypeChecker _sqlReduceOperandTypeChecker;

  /**
   * Constructor to use for aggregation functions which are only supported in v1 engine today
   */
  AggregationFunctionType(String name) {
    this(name, Collections.emptyList(), false, null, null, null, null, null, null, null, null, null, null, null);
  }

  /**
   * Constructor to use for aggregation functions which are supported in both v1 and multistage engines
   */
  AggregationFunctionType(String name, List<String> alternativeNames, boolean isNativeCalciteAggregationFunctionType,
      SqlIdentifier sqlIdentifier, SqlKind sqlKind, SqlReturnTypeInference sqlReturnTypeInference,
      SqlOperandTypeInference sqlOperandTypeInference, SqlOperandTypeChecker sqlOperandTypeChecker,
      SqlFunctionCategory sqlFunctionCategory, String intermediateFunctionName,
      SqlReturnTypeInference sqlIntermediateReturnTypeInference, String reduceFunctionName,
      SqlReturnTypeInference sqlReduceReturnTypeInference, SqlOperandTypeChecker sqlReduceOperandTypeChecker) {
    _name = name;
    _alternativeNames = alternativeNames;
    _isNativeCalciteAggregationFunctionType = isNativeCalciteAggregationFunctionType;
    _sqlIdentifier = sqlIdentifier;
    _sqlKind = sqlKind;
    _sqlReturnTypeInference = sqlReturnTypeInference;
    _sqlOperandTypeInference = sqlOperandTypeInference;
    _sqlOperandTypeChecker = sqlOperandTypeChecker;
    _sqlFunctionCategory = sqlFunctionCategory;
    _intermediateFunctionName = intermediateFunctionName == null ? name : intermediateFunctionName;
    _sqlIntermediateReturnTypeInference = sqlIntermediateReturnTypeInference;
    _reduceFunctionName = reduceFunctionName;
    _sqlReduceReturnTypeInference = sqlReduceReturnTypeInference;
    _sqlReduceOperandTypeChecker = sqlReduceOperandTypeChecker;
  }

  public String getName() {
    return _name;
  }

  public List<String> getAlternativeNames() {
    return _alternativeNames;
  }

  public boolean isNativeCalciteAggregationFunctionType() {
    return _isNativeCalciteAggregationFunctionType;
  }

  public SqlIdentifier getSqlIdentifier() {
    return _sqlIdentifier;
  }

  public SqlKind getSqlKind() {
    return _sqlKind;
  }

  public SqlReturnTypeInference getSqlReturnTypeInference() {
    return _sqlReturnTypeInference;
  }

  public SqlOperandTypeInference getSqlOperandTypeInference() {
    return _sqlOperandTypeInference;
  }

  public SqlOperandTypeChecker getSqlOperandTypeChecker() {
    return _sqlOperandTypeChecker;
  }

  public SqlFunctionCategory getSqlFunctionCategory() {
    return _sqlFunctionCategory;
  }

  public String getIntermediateFunctionName() {
    return _intermediateFunctionName;
  }

  public SqlReturnTypeInference getSqlIntermediateReturnTypeInference() {
    return _sqlIntermediateReturnTypeInference;
  }

  public String getReduceFunctionName() {
    return _reduceFunctionName;
  }

  public SqlReturnTypeInference getSqlReduceReturnTypeInference() {
    return _sqlReduceReturnTypeInference;
  }

  public SqlOperandTypeChecker getSqlReduceOperandTypeChecker() {
    return _sqlReduceOperandTypeChecker;
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
    String upperCaseFunctionName = getNormalizedAggregationFunctionName(functionName);
    return NAMES.contains(upperCaseFunctionName);
  }

  public static String getNormalizedAggregationFunctionName(String functionName) {
    return StringUtils.remove(StringUtils.remove(functionName, '_').toUpperCase(), "$");
  }

  /**
   * Returns the corresponding aggregation function type for the given function name.
   * <p>NOTE: Underscores in the function name are ignored.
   */
  public static AggregationFunctionType getAggregationFunctionType(String functionName) {
    if (functionName.regionMatches(true, 0, "percentile", 0, 10)) {
      // This style of aggregation functions is not supported in the multistage engine
      String remainingFunctionName = getNormalizedAggregationFunctionName(functionName).substring(10).toUpperCase();
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
        return AggregationFunctionType.valueOf(getNormalizedAggregationFunctionName(functionName));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid aggregation function name: " + functionName);
      }
    }
  }
}
