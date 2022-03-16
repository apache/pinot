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


public class TrigonometricFunctions {
  private TrigonometricFunctions() {
  }

  @ScalarFunction
  public static double sin(double a) {
    return Math.sin(a);
  }

  @ScalarFunction
  public static double cos(double a) {
    return Math.cos(a);
  }

  @ScalarFunction
  public static double tan(double a) {
    return Math.tan(a);
  }

  @ScalarFunction
  public static double cot(double a) {
    return 1.0 / tan(a);
  }

  @ScalarFunction
  public static double asin(double a) {
    return Math.asin(a);
  }

  @ScalarFunction
  public static double acos(double a) {
    return Math.acos(a);
  }

  @ScalarFunction
  public static double atan(double a) {
    return Math.atan(a);
  }

  @ScalarFunction
  public static double atan2(double a, double b) {
    return Math.atan2(a, b);
  }

  @ScalarFunction
  public static double sinh(double a) {
    return Math.sinh(a);
  }

  @ScalarFunction
  public static double cosh(double a) {
    return Math.cosh(a);
  }

  @ScalarFunction
  public static double tanh(double a) {
    return Math.tanh(a);
  }

  @ScalarFunction
  public static double degrees(double a) {
    return Math.toDegrees(a);
  }

  @ScalarFunction
  public static double radians(double a) {
    return Math.toRadians(a);
  }
}
