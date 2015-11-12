package com.linkedin.thirdeye.dashboard.util;

import java.util.concurrent.TimeUnit;

public enum IntraPeriod {
  DAY(TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)),
  WEEK(TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS)),
  MONTH(TimeUnit.MILLISECONDS.convert(30, TimeUnit.DAYS));
  private final long millis;

  IntraPeriod(long millis) {
    this.millis = millis;
  }

  public long getMillis() {
    return millis;
  }
}