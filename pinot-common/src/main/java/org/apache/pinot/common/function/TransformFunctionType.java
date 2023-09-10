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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;


/**
 * The {@code TransformFunctionType} enum represents all the transform functions supported by Calcite SQL parser in
 * v2 engine.
 * TODO: Add support for scalar functions auto registration.
 */
public enum TransformFunctionType {
  // arithmetic functions for single-valued columns
  ADD("add", "plus"),
  SUB("sub", "minus"),
  MULT("mult", "times"),
  DIV("div", "divide"),
  MOD("mod"),

  ABS("abs"),
  CEIL("ceil", "ceiling"),
  EXP("exp"),
  FLOOR("floor"),
  LOG("log", "ln"),
  LOG2("log2"),
  LOG10("log10"),
  SIGN("sign"),
  ROUND_DECIMAL("roundDecimal"),
  TRUNCATE("truncate"),
  POWER("power", "pow"),
  SQRT("sqrt"),

  LEAST("least"),
  GREATEST("greatest"),

  // predicate functions
  EQUALS("equals"),
  NOT_EQUALS("not_equals"),
  GREATER_THAN("greater_than"),
  GREATER_THAN_OR_EQUAL("greater_than_or_equal"),
  LESS_THAN("less_than"),
  LESS_THAN_OR_EQUAL("less_than_or_equal"),
  IN("in"),
  NOT_IN("not_in"),

  IS_TRUE("is_true"),
  IS_NOT_TRUE("is_not_true"),
  IS_FALSE("is_false"),
  IS_NOT_FALSE("is_not_false"),
  IS_NULL("is_null"),
  IS_NOT_NULL("is_not_null"),
  COALESCE("coalesce"),

  IS_DISTINCT_FROM("is_distinct_from"),
  IS_NOT_DISTINCT_FROM("is_not_distinct_from"),

  AND("and"),
  OR("or"),
  NOT("not"),   // NOT operator doesn't cover the transform for NOT IN and NOT LIKE

  // CASE WHEN function parsed as 'CASE_WHEN'
  CASE("case"),

  // date type conversion functions
  CAST("cast"),

  // object type
  ARRAY_TO_MV("arrayToMV",
      ReturnTypes.cascade(opBinding -> positionalComponentReturnType(opBinding, 0), SqlTypeTransforms.FORCE_NULLABLE),
      OperandTypes.family(SqlTypeFamily.ARRAY), "array_to_mv"),

  // string functions
  JSONEXTRACTSCALAR("jsonExtractScalar",
      ReturnTypes.cascade(opBinding -> positionalReturnTypeInferenceFromStringLiteral(opBinding, 2,
          SqlTypeName.VARCHAR), SqlTypeTransforms.FORCE_NULLABLE),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER,
          SqlTypeFamily.CHARACTER), ordinal -> ordinal > 2), "json_extract_scalar"),
  JSONEXTRACTKEY("jsonExtractKey", ReturnTypes.TO_ARRAY,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER)), "json_extract_key"),

  // date time functions
  TIMECONVERT("timeConvert",
      ReturnTypes.BIGINT_FORCE_NULLABLE,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)),
      "time_convert"),

  DATETIMECONVERT("dateTimeConvert",
      ReturnTypes.cascade(
          opBinding -> dateTimeConverterReturnTypeInference(opBinding),
          SqlTypeTransforms.FORCE_NULLABLE),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER,
          SqlTypeFamily.CHARACTER)), "date_time_convert"),

  DATETRUNC("dateTrunc",
      ReturnTypes.BIGINT_FORCE_NULLABLE,
      OperandTypes.family(
          ImmutableList.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER,
              SqlTypeFamily.CHARACTER),
          ordinal -> ordinal > 1)),

  FROMDATETIME("fromDateTime", ReturnTypes.TIMESTAMP_NULLABLE,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
          ordinal -> ordinal > 1)),

  TODATETIME("toDateTime", ReturnTypes.VARCHAR_2000_NULLABLE,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
          ordinal -> ordinal > 1)),

  TIMESTAMPADD("timestampAdd", ReturnTypes.TIMESTAMP_NULLABLE,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY)),
      "dateAdd"),

  TIMESTAMPDIFF("timestampDiff", ReturnTypes.BIGINT_NULLABLE,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY, SqlTypeFamily.ANY)), "dateDiff"),

  YEAR("year"),
  YEAR_OF_WEEK("yearOfWeek", "yow"),
  QUARTER("quarter"),
  MONTH_OF_YEAR("monthOfYear", "month"),
  WEEK_OF_YEAR("weekOfYear", "week"),
  DAY_OF_YEAR("dayOfYear", "doy"),
  DAY_OF_MONTH("dayOfMonth", "day"),
  DAY_OF_WEEK("dayOfWeek", "dow"),
  HOUR("hour"),
  MINUTE("minute"),
  SECOND("second"),
  MILLISECOND("millisecond"),

  EXTRACT("extract"),

  // array functions
  // The only column accepted by "cardinality" function is multi-value array, thus putting "cardinality" as alias.
  // TODO: once we support other types of multiset, we should make CARDINALITY its own function
  ARRAYLENGTH("arrayLength", ReturnTypes.INTEGER, OperandTypes.family(SqlTypeFamily.ARRAY), "cardinality"),
  ARRAYAVERAGE("arrayAverage", ReturnTypes.DOUBLE, OperandTypes.family(SqlTypeFamily.ARRAY)),
  ARRAYMIN("arrayMin", ReturnTypes.cascade(opBinding -> positionalComponentReturnType(opBinding, 0),
      SqlTypeTransforms.FORCE_NULLABLE), OperandTypes.family(SqlTypeFamily.ARRAY)),
  ARRAYMAX("arrayMax", ReturnTypes.cascade(opBinding -> positionalComponentReturnType(opBinding, 0),
      SqlTypeTransforms.FORCE_NULLABLE), OperandTypes.family(SqlTypeFamily.ARRAY)),
  ARRAYSUM("arraySum", ReturnTypes.DOUBLE, OperandTypes.family(SqlTypeFamily.ARRAY)),
  VALUEIN("valueIn"),
  MAPVALUE("mapValue", ReturnTypes.cascade(opBinding ->
      opBinding.getOperandType(2).getComponentType(), SqlTypeTransforms.FORCE_NULLABLE),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY)),
      "map_value"),

  // special functions
  INIDSET("inIdSet"),
  LOOKUP("lookUp"),
  GROOVY("groovy"),
  CLPDECODE("clpDecode"),

  // Regexp functions
  REGEXP_EXTRACT("regexpExtract", "regexp_extract"),
  REGEXPREPLACE("regexpReplace",
      ReturnTypes.VARCHAR_2000_NULLABLE,
      OperandTypes.family(
          ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER,
              SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER),
          ordinal -> ordinal > 2),
      "regexp_replace"),

  // Special type for annotation based scalar functions
  SCALAR("scalar"),

  // Geo constructors
  ST_GEOG_FROM_TEXT("ST_GeogFromText", ReturnTypes.explicit(SqlTypeName.VARBINARY), OperandTypes.STRING),
  ST_GEOM_FROM_TEXT("ST_GeomFromText", ReturnTypes.explicit(SqlTypeName.VARBINARY), OperandTypes.STRING),
  ST_GEOG_FROM_WKB("ST_GeogFromWKB", ReturnTypes.explicit(SqlTypeName.VARBINARY), OperandTypes.BINARY),
  ST_GEOM_FROM_WKB("ST_GeomFromWKB", ReturnTypes.explicit(SqlTypeName.VARBINARY), OperandTypes.BINARY),
  ST_POINT("ST_Point", ReturnTypes.explicit(SqlTypeName.VARBINARY),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
          ordinal -> ordinal > 1 && ordinal < 4)),
  ST_POLYGON("ST_Polygon", ReturnTypes.explicit(SqlTypeName.VARBINARY), OperandTypes.STRING),

  // Geo measurements
  ST_AREA("ST_Area", ReturnTypes.DOUBLE_NULLABLE, OperandTypes.BINARY),
  ST_DISTANCE("ST_Distance", ReturnTypes.DOUBLE_NULLABLE,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.BINARY, SqlTypeFamily.BINARY))),
  ST_GEOMETRY_TYPE("ST_GeometryType", ReturnTypes.VARCHAR_2000_NULLABLE, OperandTypes.BINARY),

  // Geo outputs
  ST_AS_BINARY("ST_AsBinary", ReturnTypes.explicit(SqlTypeName.VARBINARY), OperandTypes.BINARY),
  ST_AS_TEXT("ST_AsText", ReturnTypes.VARCHAR_2000_NULLABLE, OperandTypes.BINARY),

  // Geo relationship
  ST_CONTAINS("ST_Contains", ReturnTypes.INTEGER,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.BINARY, SqlTypeFamily.BINARY))),
  ST_EQUALS("ST_Equals", ReturnTypes.INTEGER,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.BINARY, SqlTypeFamily.BINARY))),
  ST_WITHIN("ST_Within", ReturnTypes.INTEGER,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.BINARY, SqlTypeFamily.BINARY))),

  // Geo indexing
  GEOTOH3("geoToH3", ReturnTypes.explicit(SqlTypeName.BIGINT),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
          ordinal -> ordinal > 1 && ordinal < 4)),

  // Vector functions
  // TODO: Once VECTOR type is defined, we should update here.
  COSINE_DISTANCE("cosineDistance", ReturnTypes.explicit(SqlTypeName.DOUBLE),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC),
          ordinal -> ordinal > 1 && ordinal < 4), "cosine_distance"),
  INNER_PRODUCT("innerProduct", ReturnTypes.explicit(SqlTypeName.DOUBLE),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)), "inner_product"),
  L1_DISTANCE("l1Distance", ReturnTypes.explicit(SqlTypeName.DOUBLE),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)), "l1_distance"),
  L2_DISTANCE("l2Distance", ReturnTypes.explicit(SqlTypeName.DOUBLE),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)), "l2_distance"),
  VECTOR_DIMS("vectorDims", ReturnTypes.explicit(SqlTypeName.INTEGER),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY)), "vector_dims"),
  VECTOR_NORM("vectorNorm", ReturnTypes.explicit(SqlTypeName.DOUBLE),
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ARRAY)), "vector_norm"),

  ARRAY_VALUE_CONSTRUCTOR("arrayValueConstructor"),

  // Trigonometry
  SIN("sin"),
  COS("cos"),
  TAN("tan"),
  COT("cot"),
  ASIN("asin"),
  ACOS("acos"),
  ATAN("atan"),
  ATAN2("atan2"),
  SINH("sinh"),
  COSH("cosh"),
  TANH("tanh"),
  DEGREES("degrees"),
  RADIANS("radians");

  private final String _name;
  private final List<String> _alternativeNames;
  private final SqlKind _sqlKind;
  private final SqlReturnTypeInference _returnTypeInference;
  private final SqlOperandTypeChecker _operandTypeChecker;
  private final SqlFunctionCategory _sqlFunctionCategory;

  TransformFunctionType(String name, String... alternativeNames) {
    this(name, null, null, null, null, alternativeNames);
  }

  TransformFunctionType(String name, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker, String... alternativeNames) {
    this(name, SqlKind.OTHER_FUNCTION, returnTypeInference, operandTypeChecker,
        SqlFunctionCategory.USER_DEFINED_FUNCTION, alternativeNames);
  }

  /**
   * Constructor to use for transform functions which are supported in both v1 and multistage engines
   */
  TransformFunctionType(String name, SqlKind sqlKind, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory sqlFunctionCategory, String... alternativeNames) {
    _name = name;
    List<String> all = new ArrayList<>(alternativeNames.length + 2);
    all.add(name);
    all.add(name());
    all.addAll(Arrays.asList(alternativeNames));
    _alternativeNames = Collections.unmodifiableList(all);
    _sqlKind = sqlKind;
    _returnTypeInference = returnTypeInference;
    _operandTypeChecker = operandTypeChecker;
    _sqlFunctionCategory = sqlFunctionCategory;
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

  public SqlReturnTypeInference getReturnTypeInference() {
    return _returnTypeInference;
  }

  public SqlOperandTypeChecker getOperandTypeChecker() {
    return _operandTypeChecker;
  }

  public SqlFunctionCategory getSqlFunctionCategory() {
    return _sqlFunctionCategory;
  }

  /** Returns the optional explicit returning type specification. */
  private static RelDataType positionalReturnTypeInferenceFromStringLiteral(SqlOperatorBinding opBinding, int pos) {
    return positionalReturnTypeInferenceFromStringLiteral(opBinding, pos, SqlTypeName.ANY);
  }

  private static RelDataType positionalReturnTypeInferenceFromStringLiteral(SqlOperatorBinding opBinding, int pos,
      SqlTypeName defaultSqlType) {
    if (opBinding.getOperandCount() > pos
        && opBinding.isOperandLiteral(pos, false)) {
      String operandType = opBinding.getOperandLiteralValue(pos, String.class).toUpperCase();
      return inferTypeFromStringLiteral(operandType, opBinding.getTypeFactory());
    }
    return opBinding.getTypeFactory().createSqlType(defaultSqlType);
  }

  private static RelDataType positionalComponentReturnType(SqlOperatorBinding opBinding, int pos) {
    if (opBinding.getOperandCount() > pos) {
      return opBinding.getOperandType(pos).getComponentType();
    }
    throw new IllegalArgumentException("Invalid number of arguments for function " + opBinding.getOperator().getName());
  }

  private static RelDataType dateTimeConverterReturnTypeInference(SqlOperatorBinding opBinding) {
    int outputFormatPos = 2;
    if (opBinding.getOperandCount() > outputFormatPos
        && opBinding.isOperandLiteral(outputFormatPos, false)) {
      String outputFormatStr = opBinding.getOperandLiteralValue(outputFormatPos, String.class).toUpperCase();
      DateTimeFormatSpec dateTimeFormatSpec = new DateTimeFormatSpec(outputFormatStr);
      if ((dateTimeFormatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.EPOCH) || (
          dateTimeFormatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.TIMESTAMP)) {
        return opBinding.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
      }
    }
    return opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
  }

  private static RelDataType inferTypeFromStringLiteral(String operandTypeStr, RelDataTypeFactory typeFactory) {
    switch (operandTypeStr) {
      case "INT":
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case "LONG":
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case "STRING":
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case "BYTES":
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);
      default:
        SqlTypeName sqlTypeName = SqlTypeName.get(operandTypeStr);
        if (sqlTypeName == null) {
          throw new IllegalArgumentException("Invalid type: " + operandTypeStr);
        }
        return typeFactory.createSqlType(sqlTypeName);
    }
  }
}
