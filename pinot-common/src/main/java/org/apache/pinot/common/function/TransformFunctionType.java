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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;


public enum TransformFunctionType {
  // Aggregation functions for single-valued columns
  ADD("add"),
  SUB("sub"),
  MULT("mult"),
  DIV("div"),
  MOD("mod"),

  PLUS("plus"),
  MINUS("minus"),
  TIMES("times"),
  DIVIDE("divide"),

  ABS("abs"),
  CEIL("ceil"),
  EXP("exp"),
  FLOOR("floor"),
  LN("ln"),
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

  AND("and"),
  OR("or"),

  CAST("cast"),
  CASE("case"),
  JSONEXTRACTSCALAR("jsonExtractScalar"),
  JSONEXTRACTKEY("jsonExtractKey"),
  TIMECONVERT("timeConvert"),
  DATETIMECONVERT("dateTimeConvert"),
  DATETRUNC("dateTrunc"),
  ARRAYLENGTH("arrayLength"),
  ARRAYAVERAGE("arrayAverage"),
  ARRAYMIN("arrayMin"),
  ARRAYMAX("arrayMax"),
  ARRAYSUM("arraySum"),
  VALUEIN("valueIn"),
  MAPVALUE("mapValue"),
  INIDSET("inIdSet"),
  LOOKUP("lookUp"),
  GROOVY("groovy"),

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
  GEOTOH3("geoToH3");

  private static final Set<String> NAMES = Arrays.stream(values()).flatMap(
      func -> Stream.of(func.getName(), StringUtils.remove(func.getName(), '_').toUpperCase(),
          StringUtils.remove(func.getName(), '_').toLowerCase(), func.getName().toUpperCase(),
          func.getName().toLowerCase(), func.name(), func.name().toLowerCase())).collect(Collectors.toSet());

  private final String _name;

  TransformFunctionType(String name) {
    _name = name;
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
