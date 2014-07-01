package com.linkedin.pinot.query.utils;

public class TimeConstants {
  public static final long ONE_SECOND_IN_MILLIS = 1000L;
  public static final long ONE_MINUTE_IN_MILLIS = 60000L;
  public static final long ONE_HOUR_IN_MILLIS = 3600000L;
  public static final long ONE_DAY_IN_MILLIS = 86400000L;
  public static final long ONE_WEEK_IN_MILLIS = 604800000L;
  //getting upper bound, use max num of days (31 days) in a month
  public static final long ONE_MONTH_IN_MILLIS = 2678400000L;
  //getting upper bound, use max num of days (366 days) in a year
  public static final long ONE_YEAR_IN_MILLIS = 31622400000L;
}
