package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.annotations.ScalarFunction;


public class TrigonometricFunctions {
  private TrigonometricFunctions(){
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
    return Math.sin(a);
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
  public static double radian(double a) {
    return Math.toRadians(a);
  }
}
