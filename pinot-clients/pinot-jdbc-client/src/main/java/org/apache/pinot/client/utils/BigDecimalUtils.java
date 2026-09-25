package org.apache.pinot.client.utils;

import java.math.BigDecimal;

public class BigDecimalUtils {

  private BigDecimalUtils() {
  }

  public static BigDecimal getBigDecimalFromString(String value) {
    return value == null ? null : new BigDecimal(value).setScale(getCalculatedScale(value));
  }

  static int getCalculatedScale(String value) {
    int index = value.indexOf(".");
    return index == -1 ? 0 : value.length() - index - 1;
  }
}
