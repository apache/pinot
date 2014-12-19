package com.linkedin.pinot.common.utils.time;

import java.util.concurrent.TimeUnit;


public class TimeUtils {

  public static long toMillis(String timeUnitString, String timeValue) {
    TimeUnit timeUnit = TimeUnit.valueOf(timeUnitString);
    return timeUnit.toMillis(Long.parseLong(timeValue));
  }
}
