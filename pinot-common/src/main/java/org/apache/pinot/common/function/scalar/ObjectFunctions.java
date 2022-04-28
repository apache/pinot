package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.annotations.ScalarFunction;


public class ObjectFunctions {
  private ObjectFunctions() {
  }

  @ScalarFunction(names = {"IS_NULL"})
  public static boolean isNull(Object obj) {
    return obj == null;
  }

  @ScalarFunction(names = {"IS_NOT_NULL"})
  public static boolean isNotNull(Object obj) {
    return !isNull(obj);
  }
}
