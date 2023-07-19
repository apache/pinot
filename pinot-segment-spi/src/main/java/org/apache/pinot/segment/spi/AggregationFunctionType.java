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

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
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
  COUNT("count", null, SqlKind.COUNT, SqlFunctionCategory.NUMERIC, OperandTypes.ONE_OR_MORE,
      ReturnTypes.explicit(SqlTypeName.BIGINT), ReturnTypes.explicit(SqlTypeName.BIGINT)),
  // TODO: min/max only supports NUMERIC in Pinot, where Calcite supports COMPARABLE_ORDERED
  MIN("min", null, SqlKind.MIN, SqlFunctionCategory.SYSTEM, OperandTypes.NUMERIC, ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
      ReturnTypes.explicit(SqlTypeName.DOUBLE)),
  MAX("max", null, SqlKind.MAX, SqlFunctionCategory.SYSTEM, OperandTypes.NUMERIC, ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
      ReturnTypes.explicit(SqlTypeName.DOUBLE)),
  SUM("sum", null, SqlKind.SUM, SqlFunctionCategory.NUMERIC, OperandTypes.NUMERIC, ReturnTypes.AGG_SUM,
      ReturnTypes.explicit(SqlTypeName.DOUBLE)),
  SUM0("$sum0", null, SqlKind.SUM0, SqlFunctionCategory.NUMERIC, OperandTypes.NUMERIC,
      ReturnTypes.AGG_SUM_EMPTY_IS_ZERO, ReturnTypes.explicit(SqlTypeName.DOUBLE)),
  SUMPRECISION("sumPrecision"),
  AVG("avg"),
  MODE("mode"),

  FIRSTWITHTIME("firstWithTime", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER)),
      ReturnTypes.ARG0, ReturnTypes.explicit(SqlTypeName.OTHER)),
  LASTWITHTIME("lastWithTime", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER)),
      ReturnTypes.ARG0, ReturnTypes.explicit(SqlTypeName.OTHER)),
  MINMAXRANGE("minMaxRange"),
  /**
   * for all distinct count family functions:
   * (1) distinct_count only supports single argument;
   * (2) count(distinct ...) support multi-argument and will be converted into DISTINCT + COUNT
   */
  DISTINCTCOUNT("distinctCount", null, SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.ANY, ReturnTypes.BIGINT,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTBITMAP("distinctCountBitmap"),
  SEGMENTPARTITIONEDDISTINCTCOUNT("segmentPartitionedDistinctCount"),
  DISTINCTCOUNTHLL("distinctCountHLL", ImmutableList.of("DISTINCT_COUNT_HLL"), SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.ANY, ReturnTypes.BIGINT,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTRAWHLL("distinctCountRawHLL"),
  DISTINCTCOUNTSMARTHLL("distinctCountSmartHLL"),
  FASTHLL("fastHLL"),
  DISTINCTCOUNTTHETASKETCH("distinctCountThetaSketch"),
  DISTINCTCOUNTRAWTHETASKETCH("distinctCountRawThetaSketch"),
  DISTINCTSUM("distinctSum"),
  DISTINCTAVG("distinctAvg"),

  PERCENTILE("percentile", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.ARG0,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILEEST("percentileEst"),
  PERCENTILERAWEST("percentileRawEst"),
  PERCENTILETDIGEST("percentileTDigest"),
  PERCENTILERAWTDIGEST("percentileRawTDigest"),
  PERCENTILESMARTTDIGEST("percentileSmartTDigest"),
  PERCENTILEKLL("percentileKLL"),
  PERCENTILERAWKLL("percentileRawKLL"),

  IDSET("idSet"),

  HISTOGRAM("histogram"),

  COVARPOP("covarPop", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.DOUBLE,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  COVARSAMP("covarSamp", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.DOUBLE,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  VARPOP("varPop", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  VARSAMP("varSamp", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  STDDEVPOP("stdDevPop", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  STDDEVSAMP("stdDevSamp", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  SKEWNESS("skewness", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  KURTOSIS("kurtosis", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
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
  BOOLAND("boolAnd", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.BOOLEAN, ReturnTypes.BOOLEAN, ReturnTypes.explicit(SqlTypeName.INTEGER)),
  BOOLOR("boolOr", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.BOOLEAN, ReturnTypes.BOOLEAN, ReturnTypes.explicit(SqlTypeName.INTEGER)),

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

  // --------------------------------------------------------------------------
  // Function signature used by Calcite.
  // --------------------------------------------------------------------------
  private final String _name;
  private final List<String> _alternativeNames;

  // Fields for registering the aggregation function with Calcite in multistage. These are typically used for the
  // user facing aggregation functions and the return and operand types should reflect that which is user facing.
  private final SqlKind _sqlKind;
  private final SqlFunctionCategory _sqlFunctionCategory;

  // override options for Pinot aggregate functions that expects different return type or operand type
  private final SqlReturnTypeInference _returnTypeInference;
  private final SqlOperandTypeChecker _operandTypeChecker;
  // override options for Pinot aggregate rules to insert intermediate results that are non-standard than return type.
  private final SqlReturnTypeInference _intermediateReturnTypeInference;

  /**
   * Constructor to use for aggregation functions which are only supported in v1 engine today
   */
  AggregationFunctionType(String name) {
    this(name, null, null, null);
  }

  /**
   * Constructor to use for aggregation functions which are supported in both v1 and multistage engines.
   * <ul>
   *   <li>single input operand.</li>
   *   <li>built-in input/output behavior expected by {@link org.apache.calcite.sql.fun.SqlStdOperatorTable}.</li>
   *   <li>intermediate output type the same as final output type.</li>
   * </ul>
   */
  AggregationFunctionType(String name, List<String> alternativeNames, SqlKind sqlKind,
      SqlFunctionCategory sqlFunctionCategory) {
    this(name, alternativeNames, sqlKind, sqlFunctionCategory, null, null, null);
  }

  /**
   * Constructor to use for aggregation functions which are supported in both v1 and multistage engines with
   * different behavior comparing to Calcite and requires literal operand inputs.
   *
   * @param name name of the agg function
   * @param alternativeNames alternative name of the agg function.
   * @param sqlKind sql kind indicator, used by Calcite
   * @param sqlFunctionCategory function catalog, used by Calcite
   * @param operandTypeChecker input operand type signature, used by Calcite
   * @param finalReturnType final output type signature, used by Calcite
   * @param intermediateReturnType intermediate output type signature, used by Pinot and Calcite
   */
  AggregationFunctionType(String name, @Nullable List<String> alternativeNames, @Nullable SqlKind sqlKind,
      @Nullable SqlFunctionCategory sqlFunctionCategory, @Nullable SqlOperandTypeChecker operandTypeChecker,
      @Nullable SqlReturnTypeInference finalReturnType, @Nullable SqlReturnTypeInference intermediateReturnType) {
    _name = name;
    if (alternativeNames == null || alternativeNames.size() == 0) {
      _alternativeNames = Collections.singletonList(getUnderscoreSplitAggregationFunctionName(_name));
    } else {
      _alternativeNames = alternativeNames;
    }
    _sqlKind = sqlKind;
    _sqlFunctionCategory = sqlFunctionCategory;

    _returnTypeInference = finalReturnType;
    _operandTypeChecker = operandTypeChecker;
    _intermediateReturnTypeInference = intermediateReturnType == null ? _returnTypeInference
        : intermediateReturnType;
  }

  public String getName() {
    return _name;
  }

  public List<String> getAlternativeNames() {
    return _alternativeNames;
  }

  public SqlKind getSqlKind() {
    return _sqlKind;
  }

  public SqlReturnTypeInference getIntermediateReturnTypeInference() {
    return _intermediateReturnTypeInference;
  }

  public SqlReturnTypeInference getReturnTypeInference() {
    return _returnTypeInference;
  }

  public SqlOperandTypeChecker getOperandTypeChecker() {
    return _operandTypeChecker;
  }

  public SqlFunctionCategory getSqlFunctionCategory() {
    return _sqlFunctionCategory;
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

  public static String getUnderscoreSplitAggregationFunctionName(String functionName) {
    // Skip functions that have numbers for now and return their name as is
    return functionName.matches(".*\\d.*")
        ? functionName
        : functionName.replaceAll("(.)(\\p{Upper}+|\\d+)", "$1_$2");
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
