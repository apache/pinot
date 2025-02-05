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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
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

  // null handling functions, they never return null
  IS_TRUE("is_true"),
  IS_NOT_TRUE("is_not_true"),
  IS_FALSE("is_false"),
  IS_NOT_FALSE("is_not_false"),
  IS_NULL("is_null"),
  IS_NOT_NULL("is_not_null"),
  IS_DISTINCT_FROM("is_distinct_from"),
  IS_NOT_DISTINCT_FROM("is_not_distinct_from"),

  COALESCE("coalesce"),

  AND("and"),
  OR("or"),
  NOT("not"),   // NOT operator doesn't cover the transform for NOT IN and NOT LIKE

  // CASE WHEN function parsed as 'CASE_WHEN'
  CASE("case"),

  // date type conversion functions
  CAST("cast"),

  // JSON extract functions
  JSON_EXTRACT_SCALAR("jsonExtractScalar",
      opBinding -> positionalReturnTypeInferenceFromStringLiteral(opBinding, 2, SqlTypeName.VARCHAR),
      OperandTypes.family(
          List.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
          i -> i == 3)),
  JSON_EXTRACT_INDEX("jsonExtractIndex",
      opBinding -> positionalReturnTypeInferenceFromStringLiteral(opBinding, 2, SqlTypeName.VARCHAR),
      OperandTypes.family(
          List.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER,
              SqlTypeFamily.CHARACTER), i -> i > 2)),
  JSON_EXTRACT_KEY("jsonExtractKey", ReturnTypes.TO_ARRAY,
      OperandTypes.family(List.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER))),

  // Date time functions
  TIME_CONVERT("timeConvert", ReturnTypes.BIGINT,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER))),
  DATE_TIME_CONVERT("dateTimeConvert", TransformFunctionType::dateTimeConverterReturnTypeInference, OperandTypes.family(
      List.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER,
          SqlTypeFamily.CHARACTER), i -> i == 4)),
  DATE_TIME_CONVERT_WINDOW_HOP("dateTimeConvertWindowHop",
      ReturnTypes.cascade(TransformFunctionType::dateTimeConverterReturnTypeInference, SqlTypeTransforms.TO_ARRAY),
      OperandTypes.family(
          List.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER,
              SqlTypeFamily.CHARACTER))),
  DATE_TRUNC("dateTrunc", ReturnTypes.BIGINT, OperandTypes.family(
      List.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER,
          SqlTypeFamily.CHARACTER), i -> i > 1)),
  YEAR("year", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1)),
  YEAR_OF_WEEK("yearOfWeek", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1), "yow"),
  QUARTER("quarter", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1)),
  MONTH_OF_YEAR("monthOfYear", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1), "month"),
  WEEK_OF_YEAR("weekOfYear", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1), "week"),
  DAY_OF_YEAR("dayOfYear", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1), "doy"),
  DAY_OF_MONTH("dayOfMonth", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1), "day"),
  DAY_OF_WEEK("dayOfWeek", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1), "dow"),
  HOUR("hour", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1)),
  MINUTE("minute", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1)),
  SECOND("second", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1)),
  MILLISECOND("millisecond", ReturnTypes.BIGINT_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER), i -> i == 1)),
  EXTRACT("extract"),

  // Array functions
  // The only column accepted by "cardinality" function is multi-value array, thus putting "cardinality" as alias.
  // TODO: once we support other types of multiset, we should make CARDINALITY its own function
  ARRAY_LENGTH("arrayLength", ReturnTypes.INTEGER, OperandTypes.ARRAY, "cardinality"),
  ARRAY_AVERAGE("arrayAverage", ReturnTypes.DOUBLE, OperandTypes.ARRAY),
  ARRAY_MIN("arrayMin", TransformFunctionType::componentType, OperandTypes.ARRAY),
  ARRAY_MAX("arrayMax", TransformFunctionType::componentType, OperandTypes.ARRAY),
  ARRAY_SUM("arraySum", ReturnTypes.DOUBLE, OperandTypes.ARRAY),
  ARRAY_SUM_INT("arraySumInt", ReturnTypes.INTEGER, OperandTypes.ARRAY),
  ARRAY_SUM_LONG("arraySumLong", ReturnTypes.BIGINT, OperandTypes.ARRAY),

  // Special functions
  VALUE_IN("valueIn", ReturnTypes.ARG0, OperandTypes.variadic(SqlOperandCountRanges.from(2))),
  MAP_VALUE("mapValue",
      ReturnTypes.cascade(opBinding -> positionalComponentType(opBinding, 2), SqlTypeTransforms.FORCE_NULLABLE),
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.ANY, SqlTypeFamily.ARRAY))),
  IN_ID_SET("inIdSet", ReturnTypes.BOOLEAN, OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER)),
  LOOKUP("lookUp"),
  GROOVY("groovy"),
  SCALAR("scalar"),

  // CLP functions
  CLP_DECODE("clpDecode", ReturnTypes.VARCHAR_NULLABLE,
      OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER),
          i -> i == 3)),
  CLP_ENCODED_VARS_MATCH("clpEncodedVarsMatch", ReturnTypes.BOOLEAN_NOT_NULL,
      OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER)),

  // Regexp functions
  REGEXP_EXTRACT("regexpExtract", ReturnTypes.VARCHAR_NULLABLE, OperandTypes.family(
      List.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER),
      i -> i > 1)),

  // Geo constructors
  ST_GEOG_FROM_TEXT("ST_GeogFromText", ReturnTypes.VARBINARY, OperandTypes.CHARACTER),
  ST_GEOM_FROM_TEXT("ST_GeomFromText", ReturnTypes.VARBINARY, OperandTypes.CHARACTER),

  ST_GEOG_FROM_GEO_JSON("ST_GeogFromGeoJSON", ReturnTypes.VARBINARY, OperandTypes.CHARACTER),
  ST_GEOM_FROM_GEO_JSON("ST_GeomFromGeoJSON", ReturnTypes.VARBINARY, OperandTypes.CHARACTER),

  ST_GEOG_FROM_WKB("ST_GeogFromWKB", ReturnTypes.VARBINARY, OperandTypes.BINARY),
  ST_GEOM_FROM_WKB("ST_GeomFromWKB", ReturnTypes.VARBINARY, OperandTypes.BINARY),

  ST_POINT("ST_Point", ReturnTypes.VARBINARY,
      OperandTypes.family(List.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY), i -> i == 2)),
  ST_POLYGON("ST_Polygon", ReturnTypes.VARBINARY, OperandTypes.CHARACTER),

  // Geo measurements
  ST_AREA("ST_Area", ReturnTypes.DOUBLE, OperandTypes.BINARY),
  ST_DISTANCE("ST_Distance", ReturnTypes.DOUBLE, OperandTypes.BINARY_BINARY),
  ST_GEOMETRY_TYPE("ST_GeometryType", ReturnTypes.VARCHAR, OperandTypes.BINARY),

  // Geo outputs
  ST_AS_BINARY("ST_AsBinary", ReturnTypes.VARBINARY, OperandTypes.BINARY),
  ST_AS_TEXT("ST_AsText", ReturnTypes.VARCHAR, OperandTypes.BINARY),
  ST_AS_GEO_JSON("ST_AsGeoJSON", ReturnTypes.VARCHAR, OperandTypes.BINARY),

  // Geo relationship
  // TODO: Revisit whether we should return BOOLEAN instead
  ST_CONTAINS("ST_Contains", ReturnTypes.INTEGER, OperandTypes.BINARY_BINARY),
  ST_EQUALS("ST_Equals", ReturnTypes.INTEGER, OperandTypes.BINARY_BINARY),
  ST_WITHIN("ST_Within", ReturnTypes.INTEGER, OperandTypes.BINARY_BINARY),

  // Geo indexing
  GEO_TO_H3("geoToH3", ReturnTypes.BIGINT,
      OperandTypes.or(OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.INTEGER),
          OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER))),

  // Vector functions
  // TODO: Once VECTOR type is defined, we should update here.
  COSINE_DISTANCE("cosineDistance", ReturnTypes.DOUBLE,
      OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY, SqlTypeFamily.NUMERIC), id -> id == 2)),
  INNER_PRODUCT("innerProduct", ReturnTypes.DOUBLE, OperandTypes.ARRAY_ARRAY),
  L1_DISTANCE("l1Distance", ReturnTypes.DOUBLE, OperandTypes.ARRAY_ARRAY),
  L2_DISTANCE("l2Distance", ReturnTypes.DOUBLE, OperandTypes.ARRAY_ARRAY),
  VECTOR_DIMS("vectorDims", ReturnTypes.INTEGER, OperandTypes.ARRAY),
  VECTOR_NORM("vectorNorm", ReturnTypes.DOUBLE, OperandTypes.ARRAY),

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
  RADIANS("radians"),
  // Complex type handling
  ITEM("item"),
  // Time series functions
  TIME_SERIES_BUCKET("timeSeriesBucket");

  private final String _name;
  private final List<String> _names;
  private final SqlReturnTypeInference _returnTypeInference;
  private final SqlOperandTypeChecker _operandTypeChecker;

  TransformFunctionType(String name, String... alternativeNames) {
    this(name, null, null, alternativeNames);
  }

  TransformFunctionType(String name, @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker, String... alternativeNames) {
    _name = name;
    int numAlternativeNames = alternativeNames.length;
    if (numAlternativeNames == 0) {
      _names = List.of(name);
    } else {
      List<String> names = new ArrayList<>(numAlternativeNames + 1);
      names.add(name);
      names.addAll(Arrays.asList(alternativeNames));
      _names = List.copyOf(names);
    }
    _returnTypeInference = returnTypeInference;
    _operandTypeChecker = operandTypeChecker;
  }

  public String getName() {
    return _name;
  }

  public List<String> getNames() {
    return _names;
  }

  @Deprecated
  public List<String> getAlternativeNames() {
    return _names;
  }

  public SqlReturnTypeInference getReturnTypeInference() {
    return _returnTypeInference;
  }

  public SqlOperandTypeChecker getOperandTypeChecker() {
    return _operandTypeChecker;
  }

  /** Returns the optional explicit returning type specification. */
  private static RelDataType positionalReturnTypeInferenceFromStringLiteral(SqlOperatorBinding opBinding, int pos) {
    return positionalReturnTypeInferenceFromStringLiteral(opBinding, pos, SqlTypeName.ANY);
  }

  private static RelDataType positionalReturnTypeInferenceFromStringLiteral(SqlOperatorBinding opBinding, int pos,
      SqlTypeName defaultSqlType) {
    if (opBinding.getOperandCount() > pos && opBinding.isOperandLiteral(pos, false)) {
      String operandType = opBinding.getOperandLiteralValue(pos, String.class).toUpperCase();
      return inferTypeFromStringLiteral(operandType, opBinding.getTypeFactory());
    }
    return opBinding.getTypeFactory().createSqlType(defaultSqlType);
  }

  private static RelDataType componentType(SqlOperatorBinding opBinding) {
    return opBinding.getOperandType(0).getComponentType();
  }

  private static RelDataType positionalComponentType(SqlOperatorBinding opBinding, int pos) {
    return opBinding.getOperandType(pos).getComponentType();
  }

  private static RelDataType dateTimeConverterReturnTypeInference(SqlOperatorBinding opBinding) {
    int outputFormatPos = 2;
    if (opBinding.getOperandCount() > outputFormatPos && opBinding.isOperandLiteral(outputFormatPos, false)) {
      String outputFormatStr = opBinding.getOperandLiteralValue(outputFormatPos, String.class);
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
      case "INT_ARRAY":
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
      case "LONG":
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case "LONG_ARRAY":
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), -1);
      case "FLOAT":
        return typeFactory.createSqlType(SqlTypeName.REAL);
      case "FLOAT_ARRAY":
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.REAL), -1);
      case "DOUBLE_ARRAY":
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DOUBLE), -1);
      case "STRING":
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case "STRING_ARRAY":
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
      case "BYTES":
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);
      case "BIG_DECIMAL":
        return typeFactory.createSqlType(SqlTypeName.DECIMAL);
      default:
        SqlTypeName sqlTypeName = SqlTypeName.get(operandTypeStr);
        if (sqlTypeName == null) {
          throw new IllegalArgumentException("Invalid type: " + operandTypeStr);
        }
        return typeFactory.createSqlType(sqlTypeName);
    }
  }
}
