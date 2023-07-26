package org.apache.pinot.spi.utils;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.RegisterSet;

public class HyperLogLogUtils {
  private HyperLogLogUtils() {
  }

  /**
   * Returns the byte size of the given HyperLogLog.
   */
  public static int byteSize(HyperLogLog value) {
    return value.getBytes().length;
  }

  /**
   * Returns the byte size of hyperloglog of a given log2m.
   */
  public static int byteSize(int log2m) {
    return (RegisterSet.getSizeForCount(1 << log2m) + 2) * Integer.BYTES;
  }
}