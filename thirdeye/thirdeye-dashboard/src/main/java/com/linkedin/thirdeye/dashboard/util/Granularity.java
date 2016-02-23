package com.linkedin.thirdeye.dashboard.util;

import java.util.concurrent.TimeUnit;

public enum Granularity {
  DAYS(TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)),
  HOURS(TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS));

  private final long length;

  Granularity(long millis) {
    this.length = millis;
  }

  public long getMillis() {
    return this.length;
  }

}
