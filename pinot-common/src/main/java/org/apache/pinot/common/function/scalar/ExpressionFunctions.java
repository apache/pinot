package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.annotations.ScalarFunction;

public class ExpressionFunctions {
  @ScalarFunction(vargsFunction = true)
  public static boolean and(Boolean ... args) {
    for (Boolean arg : args) {
      if (!arg)
        return false;
    }
    return true;
  }

  @ScalarFunction(vargsFunction = true)
  public static boolean or(Boolean ... args) {
    for (boolean arg : args) {
      if (arg)
        return true;
    }
    return false;
  }
}
