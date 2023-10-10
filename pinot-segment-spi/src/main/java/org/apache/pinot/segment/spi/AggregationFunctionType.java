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
  SUMPRECISION("sumPrecision", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.ANY, ReturnTypes.explicit(SqlTypeName.DECIMAL), ReturnTypes.explicit(SqlTypeName.OTHER)),
  // NO NEEDED in v2, AVG is compiled as SUM/COUNT
  AVG("avg"),
  MODE("mode"),
  FIRSTWITHTIME("firstWithTime", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.or(
          OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER)),
          OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER))
      ),
      ReturnTypes.ARG0, ReturnTypes.explicit(SqlTypeName.OTHER)),
  LASTWITHTIME("lastWithTime", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.or(
          OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER)),
          OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER))
      ),
      ReturnTypes.ARG0, ReturnTypes.explicit(SqlTypeName.OTHER)),
  MINMAXRANGE("minMaxRange", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.NUMERIC,
      OperandTypes.NUMERIC, ReturnTypes.ARG0, ReturnTypes.explicit(SqlTypeName.OTHER)),
  /**
   * for all distinct count family functions:
   * (1) distinct_count only supports single argument;
   * (2) count(distinct ...) support multi-argument and will be converted into DISTINCT + COUNT
   */
  DISTINCTCOUNT("distinctCount", ImmutableList.of("DISTINCT_COUNT"), SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.ANY, ReturnTypes.BIGINT,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTBITMAP("distinctCountBitmap", ImmutableList.of("DISTINCT_COUNT_BITMAP"), SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.ANY, ReturnTypes.BIGINT,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  SEGMENTPARTITIONEDDISTINCTCOUNT("segmentPartitionedDistinctCount",
      ImmutableList.of("SEGMENT_PARTITIONED_DISTINCT_COUNT"), SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.ANY, ReturnTypes.BIGINT, ReturnTypes.BIGINT),
  DISTINCTCOUNTHLL("distinctCountHLL", ImmutableList.of("DISTINCT_COUNT_HLL"), SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC), ordinal -> ordinal > 0),
      ReturnTypes.BIGINT, ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTRAWHLL("distinctCountRawHLL", ImmutableList.of("DISTINCT_COUNT_RAW_HLL"), SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER), ordinal -> ordinal > 0),
      ReturnTypes.VARCHAR_2000, ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTSMARTHLL("distinctCountSmartHLL", ImmutableList.of("DISTINCT_COUNT_SMART_HLL"), SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER), ordinal -> ordinal > 0),
      ReturnTypes.BIGINT, ReturnTypes.explicit(SqlTypeName.OTHER)),
  // DEPRECATED in v2
  @Deprecated
  FASTHLL("fastHLL"),
  DISTINCTCOUNTTHETASKETCH("distinctCountThetaSketch", null,
      SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER), ordinal -> ordinal > 0),
      ReturnTypes.BIGINT, ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTRAWTHETASKETCH("distinctCountRawThetaSketch", null,
      SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER), ordinal -> ordinal > 0),
      ReturnTypes.VARCHAR_2000, ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTSUM("distinctSum", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.NUMERIC,
      OperandTypes.NUMERIC, ReturnTypes.AGG_SUM, ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTAVG("distinctAvg", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.NUMERIC,
      OperandTypes.NUMERIC, ReturnTypes.explicit(SqlTypeName.DOUBLE), ReturnTypes.explicit(SqlTypeName.OTHER)),

  PERCENTILE("percentile", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.ARG0,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILEEST("percentileEst", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.ARG0,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILERAWEST("percentileRawEst", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.VARCHAR_2000,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILETDIGEST("percentileTDigest", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.ARG0,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILERAWTDIGEST("percentileRawTDigest", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.VARCHAR_2000,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  // DEPRECATED in v2
  @Deprecated
  PERCENTILESMARTTDIGEST("percentileSmartTDigest"),
  PERCENTILEKLL("percentileKLL", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.ARG0,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILERAWKLL("percentileRawKLL", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.VARCHAR_2000,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  // hyper log log plus plus functions
  DISTINCTCOUNTHLLPLUS("distinctCountHLLPlus", ImmutableList.of("DISTINCT_COUNT_HLL_PLUS"), SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC), ordinal -> ordinal > 0),
      ReturnTypes.BIGINT, ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTRAWHLLPLUS("distinctCountRawHLLPlus", ImmutableList.of("DISTINCT_COUNT_RAW_HLL_PLUS"),
      SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER), ordinal -> ordinal > 0),
      ReturnTypes.VARCHAR_2000, ReturnTypes.explicit(SqlTypeName.OTHER)),

  // DEPRECATED in v2
  @Deprecated
  IDSET("idSet"),

  // TODO: support histogram requires solving ARRAY constructor and multi-function signature without optional ordinal
  HISTOGRAM("histogram"),

  // TODO: support underscore separated version of the stats functions, resolving conflict in SqlStdOptTable
  // currently Pinot is missing generated agg functions impl from Calcite's AggregateReduceFunctionsRule
  COVARPOP("covarPop", Collections.emptyList(), SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.DOUBLE,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  COVARSAMP("covarSamp", Collections.emptyList(), SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)), ReturnTypes.DOUBLE,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  VARPOP("varPop", Collections.emptyList(), SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  VARSAMP("varSamp", Collections.emptyList(), SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  STDDEVPOP("stdDevPop", Collections.emptyList(), SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  STDDEVSAMP("stdDevSamp", Collections.emptyList(), SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  SKEWNESS("skewness", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  KURTOSIS("kurtosis", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.NUMERIC, ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  FOURTHMOMENT("fourthMoment"),

  // DataSketches Tuple Sketch support
  DISTINCTCOUNTTUPLESKETCH("distinctCountTupleSketch", ImmutableList.of("DISTINCT_COUNT_TUPLE_SKETCH"),
      SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.BINARY, ReturnTypes.BIGINT,
      ReturnTypes.explicit(SqlTypeName.OTHER)),

  // DataSketches Tuple Sketch support for Integer based Tuple Sketches
  DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH("distinctCountRawIntegerSumTupleSketch",
      ImmutableList.of("DISTINCT_COUNT_RAW_INTEGER_SUM_TUPLE_SKETCH"), SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.BINARY, ReturnTypes.VARCHAR_2000,
      ReturnTypes.explicit(SqlTypeName.OTHER)),

  SUMVALUESINTEGERSUMTUPLESKETCH("sumValuesIntegerSumTupleSketch",
      ImmutableList.of("SUM_VALUES_INTEGER_SUM_TUPLE_SKETCH"), SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.BINARY, ReturnTypes.BIGINT,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  AVGVALUEINTEGERSUMTUPLESKETCH("avgValueIntegerSumTupleSketch", ImmutableList.of("AVG_VALUE_INTEGER_SUM_TUPLE_SKETCH"),
      SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.BINARY, ReturnTypes.BIGINT,
      ReturnTypes.explicit(SqlTypeName.OTHER)),

  FREQUENTSTRINGSSKETCH("frequentStringsSketch"),
  FREQUENTLONGSSKETCH("frequentLongsSketch"),

  // Geo aggregation functions
  STUNION("STUnion", ImmutableList.of("ST_UNION"), SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.BINARY, ReturnTypes.explicit(SqlTypeName.VARBINARY), ReturnTypes.explicit(SqlTypeName.OTHER)),

  // Aggregation functions for multi-valued columns
  COUNTMV("countMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.explicit(SqlTypeName.BIGINT),
      ReturnTypes.explicit(SqlTypeName.BIGINT)),
  MINMV("minMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.DOUBLE)),
  MAXMV("maxMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.DOUBLE)),
  SUMMV("sumMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.DOUBLE)),
  AVGMV("avgMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  MINMAXRANGEMV("minMaxRangeMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.explicit(SqlTypeName.DOUBLE),
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTMV("distinctCountMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.BIGINT, ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTBITMAPMV("distinctCountBitmapMV", null, SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.BIGINT,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTHLLMV("distinctCountHLLMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.BIGINT, ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTRAWHLLMV("distinctCountRawHLLMV", null, SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.VARCHAR_2000,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTSUMMV("distinctSumMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTAVGMV("distinctAvgMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILEMV("percentileMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC)), ReturnTypes.DOUBLE,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILEESTMV("percentileEstMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC)), ReturnTypes.DOUBLE,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILERAWESTMV("percentileRawEstMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC)), ReturnTypes.VARCHAR_2000,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILETDIGESTMV("percentileTDigestMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC)), ReturnTypes.DOUBLE,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILERAWTDIGESTMV("percentileRawTDigestMV", null, SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC)), ReturnTypes.VARCHAR_2000,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILEKLLMV("percentileKLLMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
          ordinal -> ordinal > 1 && ordinal < 4), ReturnTypes.DOUBLE, ReturnTypes.explicit(SqlTypeName.OTHER)),
  PERCENTILERAWKLLMV("percentileRawKLLMV", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
          ordinal -> ordinal > 1 && ordinal < 4), ReturnTypes.VARCHAR_2000, ReturnTypes.explicit(SqlTypeName.OTHER)),
  // hyper log log plus plus functions
  DISTINCTCOUNTHLLPLUSMV("distinctCountHLLPlusMV", null, SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.BIGINT,
      ReturnTypes.explicit(SqlTypeName.OTHER)),
  DISTINCTCOUNTRAWHLLPLUSMV("distinctCountRawHLLPlusMV", null, SqlKind.OTHER_FUNCTION,
      SqlFunctionCategory.USER_DEFINED_FUNCTION, OperandTypes.family(SqlTypeFamily.ARRAY), ReturnTypes.VARCHAR_2000,
      ReturnTypes.explicit(SqlTypeName.OTHER)),

  // boolean aggregate functions
  BOOLAND("boolAnd", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.BOOLEAN, ReturnTypes.BOOLEAN, ReturnTypes.explicit(SqlTypeName.INTEGER)),
  BOOLOR("boolOr", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.BOOLEAN, ReturnTypes.BOOLEAN, ReturnTypes.explicit(SqlTypeName.INTEGER)),

  // ExprMin and ExprMax
  // TODO: revisit support for ExprMin/Max count in V2, particularly plug query rewriter in the right place
  EXPRMIN("exprMin", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY), ordinal -> ordinal > 1),
      ReturnTypes.ARG0, ReturnTypes.explicit(SqlTypeName.OTHER)),
  EXPRMAX("exprMax", null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY), ordinal -> ordinal > 1),
      ReturnTypes.ARG0, ReturnTypes.explicit(SqlTypeName.OTHER)),

  PARENTEXPRMIN(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + EXPRMIN.getName(),
      null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.INTEGER, SqlTypeFamily.ANY), ordinal -> ordinal > 2),
      ReturnTypes.explicit(SqlTypeName.OTHER), ReturnTypes.explicit(SqlTypeName.OTHER)),
  PARENTEXPRMAX(CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + EXPRMAX.getName(),
      null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.INTEGER, SqlTypeFamily.ANY), ordinal -> ordinal > 2),
      ReturnTypes.explicit(SqlTypeName.OTHER), ReturnTypes.explicit(SqlTypeName.OTHER)),

  CHILDEXPRMIN(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX + EXPRMIN.getName(),
      null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.INTEGER, SqlTypeFamily.ANY), ordinal -> ordinal > 3),
      ReturnTypes.ARG1, ReturnTypes.explicit(SqlTypeName.OTHER)),
  CHILDEXPRMAX(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX + EXPRMAX.getName(),
      null, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.INTEGER, SqlTypeFamily.ANY), ordinal -> ordinal > 3),
      ReturnTypes.ARG1, ReturnTypes.explicit(SqlTypeName.OTHER)),

  // funnel aggregate functions
  // TODO: revisit support for funnel count in V2
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
    if (alternativeNames == null) {
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
