package com.linkedin.thirdeye.completeness.checker;

import java.util.concurrent.TimeUnit;

/**
 * Constants needed for data completeness checker
 */
public class DataCompletenessConstants {

  public enum DataCompletenessType {
    CHECKER,
    CLEANUP
  }

  public static int LOOKBACK_TIME_DURATION = 3;
  public static TimeUnit LOOKBACK_TIMEUNIT = TimeUnit.DAYS;
}
