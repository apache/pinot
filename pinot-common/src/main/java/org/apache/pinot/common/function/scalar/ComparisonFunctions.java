package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.annotations.ScalarFunction;

public class ComparisonFunctions {

  private static final double DOUBLE_COMPARISON_TOLERANCE = 1e-7d;

  @ScalarFunction(names = {"gt"})
  public static boolean greaterThan(double a, double b) {
    return a > b;
  }

  @ScalarFunction(names = {"gte"})
  public static boolean greaterThanOrEquals(double a, double b) {
    return a >= b;
  }

  @ScalarFunction(names = {"lt"})
  public static boolean lessThan(double a, double b) {
    return a < b;
  }

  @ScalarFunction(names = {"lte"})
  public static boolean lessThanOrEquals(double a, double b) {
    return a <= b;
  }

  @ScalarFunction(names = {"neq"})
  public static boolean notEquals(double a, double b) {
    return (Math.abs(a - b) >= DOUBLE_COMPARISON_TOLERANCE);
  }

  @ScalarFunction(names = {"eq"})
  public static boolean equals(double a, double b) {
    // To avoid approximation errors
    return (Math.abs(a - b) < DOUBLE_COMPARISON_TOLERANCE);
  }
}
