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

import java.math.BigDecimal;
import java.math.RoundingMode;
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
  public static double divide(double a, double b, double defaultValue) {
    return (b == 0) ? defaultValue : a / b;
  }

  @ScalarFunction
  public static double mod(double a, double b) {
    return a % b;
  }

  @ScalarFunction
  public static double least(double a, double b) {
    return Double.min(a, b);
  }

  @ScalarFunction
  public static double greatest(double a, double b) {
    return Double.max(a, b);
  }

  @Deprecated
  @ScalarFunction
  public static double min(double a, double b) {
    return least(a, b);
  }

  @Deprecated
  @ScalarFunction
  public static double max(double a, double b) {
    return greatest(a, b);
  }

  @ScalarFunction
  public static double abs(double a) {
    return Math.abs(a);
  }

  @ScalarFunction(names = {"ceil", "ceiling"})
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

  @ScalarFunction(names = {"ln", "log"})
  public static double ln(double a) {
    return Math.log(a);
  }

  @ScalarFunction
  public static double log2(double a) {
    return Math.log(a) / Math.log(2);
  }

  @ScalarFunction
  public static double log10(double a) {
    return Math.log10(a);
  }

  @ScalarFunction
  public static double sqrt(double a) {
    return Math.sqrt(a);
  }

  @ScalarFunction
  public static double sign(double a) {
    return Math.signum(a);
  }

  @ScalarFunction(names = {"pow", "power"})
  public static double power(double a, double exponent) {
    return Math.pow(a, exponent);
  }


  // Big Decimal Implementation has been used here to avoid overflows
  // when multiplying by Math.pow(10, scale) for rounding
  @ScalarFunction
  public static double roundDecimal(double a, int scale) {
    return BigDecimal.valueOf(a).setScale(scale, RoundingMode.HALF_UP).doubleValue();
  }

  // TODO: The function should ideally be named 'round'
  // but it is not possible because of existing DateTimeFunction with same name.
  @ScalarFunction
  public static double roundDecimal(double a) {
    return Math.round(a);
  }

  // Big Decimal Implementation has been used here to avoid overflows
  // when multiplying by Math.pow(10, scale) for rounding
  @ScalarFunction
  public static double truncate(double a, int scale) {
    return BigDecimal.valueOf(a).setScale(scale, RoundingMode.DOWN).doubleValue();
  }

  @ScalarFunction
  public static double truncate(double a) {
    return Math.signum(a) * Math.floor(Math.abs(a));
  }
}
