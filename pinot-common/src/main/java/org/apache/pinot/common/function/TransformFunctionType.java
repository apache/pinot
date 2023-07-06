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
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.commons.lang.StringUtils;


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

  // string functions
  JSONEXTRACTSCALAR("jsonExtractScalar", SqlKind.OTHER_FUNCTION,
      ReturnTypes.cascade(opBinding -> inferJsonExtractScalarExplicitTypeSpec(opBinding).orElse(
              opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 2000)),
          SqlTypeTransforms.FORCE_NULLABLE), null,
      OperandTypes.family(ImmutableList.of(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
          ordinal -> ordinal > 1),
      SqlFunctionCategory.USER_DEFINED_FUNCTION),
  JSONEXTRACTKEY("jsonExtractKey"),

  // date time functions
  TIMECONVERT("timeConvert"),
  DATETIMECONVERT("dateTimeConvert"),
  DATETRUNC("dateTrunc"),
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
  ARRAYLENGTH("arrayLength", "cardinality"),
  ARRAYAVERAGE("arrayAverage"),
  ARRAYMIN("arrayMin"),
  ARRAYMAX("arrayMax"),
  ARRAYSUM("arraySum"),
  VALUEIN("valueIn"),
  MAPVALUE("mapValue"),

  // special functions
  INIDSET("inIdSet"),
  LOOKUP("lookUp"),
  GROOVY("groovy"),
  CLPDECODE("clpDecode"),

  // Regexp functions
  REGEXP_EXTRACT("regexpExtract"),

  // Special type for annotation based scalar functions
  SCALAR("scalar"),

  // Geo constructors
  ST_GEOG_FROM_TEXT("ST_GeogFromText"),
  ST_GEOM_FROM_TEXT("ST_GeomFromText"),
  ST_GEOG_FROM_WKB("ST_GeogFromWKB"),
  ST_GEOM_FROM_WKB("ST_GeomFromWKB"),
  ST_POINT("ST_Point"),
  ST_POLYGON("ST_Polygon"),

  // Geo measurements
  ST_AREA("ST_Area"),
  ST_DISTANCE("ST_Distance"),
  ST_GEOMETRY_TYPE("ST_GeometryType"),

  // Geo outputs
  ST_AS_BINARY("ST_AsBinary"),
  ST_AS_TEXT("ST_AsText"),

  // Geo relationship
  ST_CONTAINS("ST_Contains"),
  ST_EQUALS("ST_Equals"),
  ST_WITHIN("ST_Within"),

  // Geo indexing
  GEOTOH3("geoToH3"),

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
  private final List<String> _aliases;
  private final SqlKind _sqlKind;
  private final SqlReturnTypeInference _sqlReturnTypeInference;
  private final SqlOperandTypeInference _sqlOperandTypeInference;
  private final SqlOperandTypeChecker _sqlOperandTypeChecker;
  private final SqlFunctionCategory _sqlFunctionCategory;

  TransformFunctionType(String name, String... aliases) {
    this(name, null, null, null, null, null, aliases);
  }

  /**
   * Constructor to use for transform functions which are supported in both v1 and multistage engines
   */
  TransformFunctionType(String name, SqlKind sqlKind, SqlReturnTypeInference sqlReturnTypeInference,
      SqlOperandTypeInference sqlOperandTypeInference, SqlOperandTypeChecker sqlOperandTypeChecker,
      SqlFunctionCategory sqlFunctionCategory, String... aliases) {
    _name = name;
    List<String> all = new ArrayList<>(aliases.length + 2);
    all.add(name);
    all.add(name());
    all.addAll(Arrays.asList(aliases));
    _aliases = Collections.unmodifiableList(all);
    _sqlKind = sqlKind;
    _sqlReturnTypeInference = sqlReturnTypeInference;
    _sqlOperandTypeInference = sqlOperandTypeInference;
    _sqlOperandTypeChecker = sqlOperandTypeChecker;
    _sqlFunctionCategory = sqlFunctionCategory;
  }

  public String getName() {
    return _name;
  }

  public List<String> getAliases() {
    return _aliases;
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

  /** Returns the optional explicit returning type specification. */
  private static Optional<RelDataType> inferJsonExtractScalarExplicitTypeSpec(SqlOperatorBinding opBinding) {
    if (opBinding.getOperandCount() > 2
        && opBinding.isOperandLiteral(2, false)) {
      String operandType = opBinding.getOperandLiteralValue(2, String.class).toUpperCase();
      return Optional.of(inferExplicitTypeSpec(operandType, opBinding.getTypeFactory()));
    }
    return Optional.empty();
  }

  private static RelDataType inferExplicitTypeSpec(String operandType, RelDataTypeFactory typeFactory) {
    switch (operandType) {
      case "INT":
      case "INTEGER":
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case "LONG":
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case "STRING":
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case "BYTES":
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);
      default:
        SqlTypeName sqlTypeName = SqlTypeName.get(operandType);
        if (sqlTypeName == null) {
          throw new IllegalArgumentException("Invalid type: " + operandType);
        }
        return typeFactory.createSqlType(sqlTypeName);
    }
  }

  public static String getNormalizedTransformFunctionName(String functionName) {
    return StringUtils.remove(functionName, '_').toUpperCase();
  }
}
