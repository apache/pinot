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


/**
 * Arithmetic scalar functions.
 */
public class ArithmeticFunctions {
  private ArithmeticFunctions() {
  }

  @ScalarFunction
  public static double plus(double a, double b) {
    return a + b;
  }

  @ScalarFunction
  public static double minus(double a, double b) {
    return a - b;
  }

  @ScalarFunction
  public static double times(double a, double b) {
    return a * b;
  }

  @ScalarFunction
  public static double divide(double a, double b) {
    return a / b;
  }

  @ScalarFunction
  public static double mod(double a, double b) {
    return a % b;
  }

  @ScalarFunction
  public static double min(double a, double b) {
    return Double.min(a, b);
  }

  @ScalarFunction
  public static double max(double a, double b) {
    return Double.max(a, b);
  }

  @ScalarFunction
  public static double abs(double a) {
    return Math.abs(a);
  }

  @ScalarFunction
  public static double ceil(double a) {
    return Math.ceil(a);
  }

  @ScalarFunction
  public static double floor(double a) {
    return Math.floor(a);
  }

  @ScalarFunction
  public static double exp(double a) {
    return Math.exp(a);
  }

  @ScalarFunction
  public static double ln(double a) {
    return Math.log(a);
  }

  @ScalarFunction
  public static double sqrt(double a) {
    return Math.sqrt(a);
  }
}
