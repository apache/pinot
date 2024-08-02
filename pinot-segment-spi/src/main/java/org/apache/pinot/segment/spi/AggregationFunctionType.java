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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.StringUtils;
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
  COUNT("count"),
  // TODO: min/max only supports NUMERIC in Pinot, where Calcite supports COMPARABLE_ORDERED
  MIN("min", SqlTypeName.DOUBLE),
  MAX("max", SqlTypeName.DOUBLE),
  SUM("sum", SqlTypeName.DOUBLE),
  SUM0("$sum0", SqlTypeName.DOUBLE),
  SUMPRECISION("sumPrecision", ReturnTypes.explicit(SqlTypeName.DECIMAL), OperandTypes.ANY, SqlTypeName.OTHER),
  AVG("avg", SqlTypeName.OTHER),
  MODE("mode", SqlTypeName.OTHER),
  FIRSTWITHTIME("firstWithTime", ReturnTypes.ARG0,
      OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER), SqlTypeName.OTHER),
  LASTWITHTIME("lastWithTime", ReturnTypes.ARG0,
      OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER), SqlTypeName.OTHER),
  MINMAXRANGE("minMaxRange", ReturnTypes.DOUBLE, OperandTypes.ANY, SqlTypeName.OTHER),

  /**
   * for all distinct count family functions:
   * (1) distinct_count only supports single argument;
   * (2) count(distinct ...) support multi-argument and will be converted into DISTINCT + COUNT
   */
  DISTINCTCOUNT("distinctCount", ReturnTypes.BIGINT, OperandTypes.ANY, SqlTypeName.OTHER),
  DISTINCTSUM("distinctSum", ReturnTypes.AGG_SUM, OperandTypes.NUMERIC, SqlTypeName.OTHER),
  DISTINCTAVG("distinctAvg", ReturnTypes.DOUBLE, OperandTypes.NUMERIC, SqlTypeName.OTHER),
  DISTINCTCOUNTBITMAP("distinctCountBitmap", ReturnTypes.BIGINT, OperandTypes.ANY, SqlTypeName.OTHER),
  SEGMENTPARTITIONEDDISTINCTCOUNT("segmentPartitionedDistinctCount", ReturnTypes.BIGINT, OperandTypes.ANY,
      SqlTypeName.OTHER),
  DISTINCTCOUNTHLL("distinctCountHLL", ReturnTypes.BIGINT,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTRAWHLL("distinctCountRawHLL", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTSMARTHLL("distinctCountSmartHLL", ReturnTypes.BIGINT,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER), i -> i == 1), SqlTypeName.OTHER),
  @Deprecated FASTHLL("fastHLL"),
  DISTINCTCOUNTHLLPLUS("distinctCountHLLPlus", ReturnTypes.BIGINT,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTRAWHLLPLUS("distinctCountRawHLLPlus", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTULL("distinctCountULL", ReturnTypes.BIGINT,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTRAWULL("distinctCountRawULL", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTTHETASKETCH("distinctCountThetaSketch", ReturnTypes.BIGINT,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTRAWTHETASKETCH("distinctCountRawThetaSketch", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTTUPLESKETCH("distinctCountTupleSketch", ReturnTypes.BIGINT, OperandTypes.BINARY, SqlTypeName.OTHER),
  DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH("distinctCountRawIntegerSumTupleSketch", ReturnTypes.VARCHAR,
      OperandTypes.BINARY, SqlTypeName.OTHER),
  SUMVALUESINTEGERSUMTUPLESKETCH("sumValuesIntegerSumTupleSketch", ReturnTypes.BIGINT, OperandTypes.BINARY,
      SqlTypeName.OTHER),
  AVGVALUEINTEGERSUMTUPLESKETCH("avgValueIntegerSumTupleSketch", ReturnTypes.BIGINT, OperandTypes.BINARY,
      SqlTypeName.OTHER),
  DISTINCTCOUNTCPCSKETCH("distinctCountCPCSketch", ReturnTypes.BIGINT,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.ANY), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTRAWCPCSKETCH("distinctCountRawCPCSketch", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.ANY), i -> i == 1), SqlTypeName.OTHER),

  PERCENTILE("percentile", ReturnTypes.ARG0, OperandTypes.ANY_NUMERIC, SqlTypeName.OTHER),
  PERCENTILEEST("percentileEst", ReturnTypes.BIGINT, OperandTypes.ANY_NUMERIC, SqlTypeName.OTHER),
  PERCENTILERAWEST("percentileRawEst", ReturnTypes.VARCHAR, OperandTypes.ANY_NUMERIC, SqlTypeName.OTHER),
  PERCENTILETDIGEST("percentileTDigest", ReturnTypes.DOUBLE,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER), i -> i == 2),
      SqlTypeName.OTHER),
  PERCENTILERAWTDIGEST("percentileRawTDigest", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER), i -> i == 2),
      SqlTypeName.OTHER),
  PERCENTILESMARTTDIGEST("percentileSmartTDigest", ReturnTypes.DOUBLE,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER), i -> i == 2),
      SqlTypeName.OTHER),
  PERCENTILEKLL("percentileKLL", ReturnTypes.DOUBLE,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER), i -> i == 2),
      SqlTypeName.OTHER),
  PERCENTILERAWKLL("percentileRawKLL", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER), i -> i == 2),
      SqlTypeName.OTHER),

  IDSET("idSet", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER), i -> i == 1), SqlTypeName.OTHER),

  HISTOGRAM("histogram", new ArrayReturnTypeInference(SqlTypeName.DOUBLE), OperandTypes.VARIADIC, SqlTypeName.OTHER),

  COVARPOP("covarPop", SqlTypeName.OTHER),
  COVARSAMP("covarSamp", SqlTypeName.OTHER),
  VARPOP("varPop", SqlTypeName.OTHER),
  VARSAMP("varSamp", SqlTypeName.OTHER),
  STDDEVPOP("stdDevPop", SqlTypeName.OTHER),
  STDDEVSAMP("stdDevSamp", SqlTypeName.OTHER),

  SKEWNESS("skewness", ReturnTypes.DOUBLE, OperandTypes.ANY, SqlTypeName.OTHER),
  KURTOSIS("kurtosis", ReturnTypes.DOUBLE, OperandTypes.ANY, SqlTypeName.OTHER),
  FOURTHMOMENT("fourthMoment", ReturnTypes.DOUBLE, OperandTypes.ANY, SqlTypeName.OTHER),

  // Datasketches Frequent Items support
  FREQUENTSTRINGSSKETCH("frequentStringsSketch", ReturnTypes.VARCHAR, OperandTypes.ANY, SqlTypeName.OTHER),
  FREQUENTLONGSSKETCH("frequentLongsSketch", ReturnTypes.VARCHAR, OperandTypes.ANY, SqlTypeName.OTHER),

  // Geo aggregation functions
  STUNION("STUnion", ReturnTypes.VARBINARY, OperandTypes.BINARY, SqlTypeName.OTHER),

  // boolean aggregate functions
  BOOLAND("boolAnd", ReturnTypes.BOOLEAN, OperandTypes.BOOLEAN, SqlTypeName.INTEGER),
  BOOLOR("boolOr", ReturnTypes.BOOLEAN, OperandTypes.BOOLEAN, SqlTypeName.INTEGER),

  // ExprMin and ExprMax
  // TODO: revisit support for ExprMin/Max count in V2, particularly plug query rewriter in the right place
  EXPRMIN("exprMin", ReturnTypes.ARG0, OperandTypes.VARIADIC, SqlTypeName.OTHER),
  EXPRMAX("exprMax", ReturnTypes.ARG0, OperandTypes.VARIADIC, SqlTypeName.OTHER),
  PINOTPARENTAGGEXPRMIN(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + EXPRMIN.getName(),
      ReturnTypes.explicit(SqlTypeName.OTHER), OperandTypes.VARIADIC, SqlTypeName.OTHER),
  PINOTPARENTAGGEXPRMAX(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + EXPRMAX.getName(),
      ReturnTypes.explicit(SqlTypeName.OTHER), OperandTypes.VARIADIC, SqlTypeName.OTHER),
  PINOTCHILDAGGEXPRMIN(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX + EXPRMIN.getName(),
      ReturnTypes.ARG1, OperandTypes.VARIADIC, SqlTypeName.OTHER),
  PINOTCHILDAGGEXPRMAX(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX + EXPRMAX.getName(),
      ReturnTypes.ARG1, OperandTypes.VARIADIC, SqlTypeName.OTHER),

  // Array aggregate functions
  ARRAYAGG("arrayAgg", ReturnTypes.TO_ARRAY,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.BOOLEAN), i -> i == 2),
      SqlTypeName.OTHER),
  LISTAGG("listAgg", SqlTypeName.OTHER),

  SUMARRAYLONG("sumArrayLong", new ArrayReturnTypeInference(SqlTypeName.BIGINT), OperandTypes.ARRAY, SqlTypeName.OTHER),
  SUMARRAYDOUBLE("sumArrayDouble", new ArrayReturnTypeInference(SqlTypeName.DOUBLE), OperandTypes.ARRAY,
      SqlTypeName.OTHER),

  // funnel aggregate functions
  FUNNELMAXSTEP("funnelMaxStep", ReturnTypes.INTEGER, OperandTypes.VARIADIC, SqlTypeName.OTHER),
  FUNNELCOMPLETECOUNT("funnelCompleteCount", ReturnTypes.INTEGER, OperandTypes.VARIADIC, SqlTypeName.OTHER),
  FUNNELMATCHSTEP("funnelMatchStep", new ArrayReturnTypeInference(SqlTypeName.INTEGER), OperandTypes.VARIADIC,
      SqlTypeName.OTHER),
  FUNNELCOUNT("funnelCount", new ArrayReturnTypeInference(SqlTypeName.BIGINT), OperandTypes.VARIADIC,
      SqlTypeName.OTHER),

  // Aggregation functions for multi-valued columns
  COUNTMV("countMV", ReturnTypes.BIGINT, OperandTypes.ARRAY, SqlTypeName.BIGINT),
  MINMV("minMV", ReturnTypes.DOUBLE, OperandTypes.ARRAY, SqlTypeName.DOUBLE),
  MAXMV("maxMV", ReturnTypes.DOUBLE, OperandTypes.ARRAY, SqlTypeName.DOUBLE),
  SUMMV("sumMV", ReturnTypes.DOUBLE, OperandTypes.ARRAY, SqlTypeName.DOUBLE),
  AVGMV("avgMV", ReturnTypes.DOUBLE, OperandTypes.ARRAY, SqlTypeName.OTHER),
  MINMAXRANGEMV("minMaxRangeMV", ReturnTypes.DOUBLE, OperandTypes.ARRAY, SqlTypeName.OTHER),
  DISTINCTCOUNTMV("distinctCountMV", ReturnTypes.BIGINT, OperandTypes.ARRAY, SqlTypeName.OTHER),
  DISTINCTSUMMV("distinctSumMV", ReturnTypes.DOUBLE, OperandTypes.ARRAY, SqlTypeName.OTHER),
  DISTINCTAVGMV("distinctAvgMV", ReturnTypes.DOUBLE, OperandTypes.ARRAY, SqlTypeName.OTHER),
  DISTINCTCOUNTBITMAPMV("distinctCountBitmapMV", ReturnTypes.BIGINT, OperandTypes.ARRAY, SqlTypeName.OTHER),
  DISTINCTCOUNTHLLMV("distinctCountHLLMV", ReturnTypes.BIGINT,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTRAWHLLMV("distinctCountRawHLLMV", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTHLLPLUSMV("distinctCountHLLPlusMV", ReturnTypes.BIGINT,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER), i -> i == 1), SqlTypeName.OTHER),
  DISTINCTCOUNTRAWHLLPLUSMV("distinctCountRawHLLPlusMV", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER), i -> i == 1), SqlTypeName.OTHER),
  PERCENTILEMV("percentileMV", ReturnTypes.DOUBLE,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC)), SqlTypeName.OTHER),
  PERCENTILEESTMV("percentileEstMV", ReturnTypes.DOUBLE,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC)), SqlTypeName.OTHER),
  PERCENTILERAWESTMV("percentileRawEstMV", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC)), SqlTypeName.OTHER),
  PERCENTILETDIGESTMV("percentileTDigestMV", ReturnTypes.DOUBLE,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER), i -> i == 2),
      SqlTypeName.OTHER),
  PERCENTILERAWTDIGESTMV("percentileRawTDigestMV", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER), i -> i == 2),
      SqlTypeName.OTHER),
  PERCENTILEKLLMV("percentileKLLMV", ReturnTypes.DOUBLE,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER), i -> i == 2),
      SqlTypeName.OTHER),
  PERCENTILERAWKLLMV("percentileRawKLLMV", ReturnTypes.VARCHAR,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER), i -> i == 2),
      SqlTypeName.OTHER);

  private static final Set<String> NAMES =
      Arrays.stream(values()).flatMap(func -> Stream.of(func.name(), func.getName(), func.getName().toLowerCase()))
          .collect(Collectors.toSet());

  private final String _name;

  // Fields used by multi-stage engine
  // When returnTypeInference is provided, the function will be registered as a USER_DEFINED_FUNCTION
  private final SqlReturnTypeInference _returnTypeInference;
  private final SqlOperandTypeChecker _operandTypeChecker;
  // override options for Pinot aggregate rules to insert intermediate results that are non-standard than return type.
  private final SqlReturnTypeInference _intermediateReturnTypeInference;

  AggregationFunctionType(String name) {
    this(name, null, null, (SqlReturnTypeInference) null);
  }

  AggregationFunctionType(String name, SqlTypeName intermediateReturnType) {
    this(name, null, null, ReturnTypes.explicit(intermediateReturnType));
  }

  AggregationFunctionType(String name, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker, SqlTypeName intermediateReturnType) {
    this(name, returnTypeInference, operandTypeChecker, ReturnTypes.explicit(intermediateReturnType));
  }

  AggregationFunctionType(String name, @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      @Nullable SqlReturnTypeInference intermediateReturnTypeInference) {
    _name = name;
    _returnTypeInference = returnTypeInference;
    _operandTypeChecker = operandTypeChecker;
    _intermediateReturnTypeInference = intermediateReturnTypeInference;
  }

  public String getName() {
    return _name;
  }

  @Nullable
  public SqlReturnTypeInference getReturnTypeInference() {
    return _returnTypeInference;
  }

  @Nullable
  public SqlOperandTypeChecker getOperandTypeChecker() {
    return _operandTypeChecker;
  }

  @Nullable
  public SqlReturnTypeInference getIntermediateReturnTypeInference() {
    return _intermediateReturnTypeInference;
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
    String normalizedFunctionName = getNormalizedAggregationFunctionName(functionName);
    if (normalizedFunctionName.regionMatches(false, 0, "PERCENTILE", 0, 10)) {
      // This style of aggregation functions is not supported in the multistage engine
      String remainingFunctionName = normalizedFunctionName.substring(10).toUpperCase();
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
        return AggregationFunctionType.valueOf(normalizedFunctionName);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid aggregation function name: " + functionName);
      }
    }
  }

  private static class ArrayReturnTypeInference implements SqlReturnTypeInference {
    final SqlTypeName _sqlTypeName;

    ArrayReturnTypeInference(SqlTypeName sqlTypeName) {
      _sqlTypeName = sqlTypeName;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      RelDataType elementType = typeFactory.createSqlType(_sqlTypeName);
      return typeFactory.createArrayType(elementType, -1);
    }
  }
}
