package com.linkedin.pinot.common;

import java.util.concurrent.atomic.AtomicLong;

public class Utils {

  private static final AtomicLong _uniqueIdGen = new AtomicLong(1);

  public static long getUniqueId()
  {
    return _uniqueIdGen.incrementAndGet();
  }
}
