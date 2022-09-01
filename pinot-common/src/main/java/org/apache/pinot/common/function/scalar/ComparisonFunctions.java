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
package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.annotations.ScalarFunction;


/*
 * All functions are registered with a) function name and function param types b) function names and number of function
 * params.
 * To enable function param type matching, primitive type function argument has to be wrapped as Object class because
 * literal is always an Object type.
 */
public class ComparisonFunctions {

  private static final double DOUBLE_COMPARISON_TOLERANCE = 1e-7d;

  private ComparisonFunctions() {
  }

  @ScalarFunction
  public static boolean greaterThan(double a, double b) {
    return a > b;
  }

  @ScalarFunction
  public static boolean greaterThanOrEqual(double a, double b) {
    return a >= b;
  }

  @ScalarFunction
  public static boolean lessThan(double a, double b) {
    return a < b;
  }

  @ScalarFunction
  public static boolean lessThanOrEqual(double a, double b) {
    return a <= b;
  }

  @ScalarFunction
  public static boolean notEquals(Double a, Double b) {
    return Math.abs(a - b) >= DOUBLE_COMPARISON_TOLERANCE;
  }

  @ScalarFunction
  public static boolean notEquals(Boolean a, Boolean b) {
    return !a.equals(b);
  }

  @ScalarFunction
  public static boolean equals(Double a, Double b) {
    // To avoid approximation errors
    return Math.abs(a - b) < DOUBLE_COMPARISON_TOLERANCE;
  }

  @ScalarFunction
  public static boolean equals(Boolean a, Boolean b) {
    return a.equals(b);
  }

  @ScalarFunction
  public static boolean between(double val, double a, double b) {
    return val > a && val < b;
  }
}
