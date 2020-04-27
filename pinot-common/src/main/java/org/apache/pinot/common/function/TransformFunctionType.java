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

  CAST("cast"),
  TIMECONVERT("timeConvert"),
  DATETIMECONVERT("dateTimeConvert"),
  DATETRUNC("dateTrunc"),
  ARRAYLENGTH("arrayLength"),
  VALUEIN("valueIn"),
  MAPVALUE("mapValue");

  private final String _name;

  TransformFunctionType(String name) {
    _name = name;
  }

  /**
   * Returns the corresponding transform function type for the given function name.
   */
  public static TransformFunctionType getTransformFunctionType(String functionName) {
    String upperCaseFunctionName = functionName.toUpperCase();
    try {
      return TransformFunctionType.valueOf(upperCaseFunctionName);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid transform function name: " + functionName);
    }
  }

  public String getName() {
    return _name;
  }
}
