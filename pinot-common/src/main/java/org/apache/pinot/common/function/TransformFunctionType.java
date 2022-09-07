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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;


public enum TransformFunctionType {
  // Aggregation functions for single-valued columns
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

  EQUALS("equals"),
  NOT_EQUALS("not_equals"),
  GREATER_THAN("greater_than"),
  GREATER_THAN_OR_EQUAL("greater_than_or_equal"),
  LESS_THAN("less_than"),
  LESS_THAN_OR_EQUAL("less_than_or_equal"),
  IN("in"),

  IS_NULL("is_null"),
  IS_NOT_NULL("is_not_null"),

  AND("and"),
  OR("or"),
  NOT("not"),   // NOT operator doesn't cover the transform for NOT IN and NOT LIKE

  CAST("cast"),
  CASE("case"),
  JSONEXTRACTSCALAR("jsonExtractScalar"),
  JSONEXTRACTKEY("jsonExtractKey"),
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
  // The only column accepted by "cardinality" function is multi-value array, thus putting "cardinality" as alias.
  // TODO: once we support other types of multiset, we should make CARDINALITY its own function
  ARRAYLENGTH("arrayLength", "cardinality"),
  ARRAYAVERAGE("arrayAverage"),
  ARRAYMIN("arrayMin"),
  ARRAYMAX("arrayMax"),
  ARRAYSUM("arraySum"),
  VALUEIN("valueIn"),
  MAPVALUE("mapValue"),
  INIDSET("inIdSet"),
  LOOKUP("lookUp"),
  GROOVY("groovy"),

  EXTRACT("extract"),

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

  //Trigonometry
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

  private static final Set<String> NAMES = Arrays.stream(values()).flatMap(
      func -> func.getAliases().stream().flatMap(name -> Stream.of(name, StringUtils.remove(name, '_').toUpperCase(),
          StringUtils.remove(name, '_').toLowerCase(), name.toUpperCase(),
          name.toLowerCase()))).collect(Collectors.toSet());

  private final String _name;
  private final List<String> _aliases;

  TransformFunctionType(String name, String... aliases) {
    _name = name;
    List<String> all = new ArrayList<>(aliases.length + 2);
    all.add(name);
    all.add(name());
    all.addAll(Arrays.asList(aliases));
    _aliases = Collections.unmodifiableList(all);
  }

  public List<String> getAliases() {
    return _aliases;
  }

  public static boolean isTransformFunction(String functionName) {
    if (NAMES.contains(functionName)) {
      return true;
    }
    // scalar functions
    if (FunctionRegistry.containsFunction(functionName)) {
      return true;
    }
    return NAMES.contains(StringUtils.remove(functionName, '_').toUpperCase());
  }

  /**
   * Returns the corresponding transform function type for the given function name.
   */
  public static TransformFunctionType getTransformFunctionType(String functionName) {
    String upperCaseFunctionName = functionName.toUpperCase();
    try {
      return TransformFunctionType.valueOf(upperCaseFunctionName);
    } catch (IllegalArgumentException e) {
      if (FunctionRegistry.containsFunction(functionName)) {
        return SCALAR;
      }
      // Support function name of both jsonExtractScalar and json_extract_scalar
      if (upperCaseFunctionName.contains("_")) {
        return getTransformFunctionType(StringUtils.remove(upperCaseFunctionName, '_'));
      }
      throw new IllegalArgumentException("Invalid transform function name: " + functionName);
    }
  }

  public String getName() {
    return _name;
  }
}
