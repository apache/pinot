package com.linkedin.pinot.query.utils;

public class TimeUtils {

  /**
   * Given a String representation of time granularity, return the equivalent number of
   * milliseconds. Accepted strings: 'SECONDS', 'MINUTES, 'HOURLY', 'DAILY', 'WEEKLY', 'MONTHLY',
   * 'YEARLY', 'nD' (ex: '14D'), 'nH' (ex: '12H') For monthly and yearly, the upper bound of time is
   * returned by using the max number of days in a month/year. If the string passed in is null,
   * automatically default to 'DAILY'.
   */
  public static long toMillis(String granularity) {
    // Default value: any granularity that doesn't have time granularity is default to DAILY.
    if (granularity == null) {
      granularity = "DAILY";
    }
    granularity = granularity.trim();
    if (granularity.toUpperCase().equals("DAILY")) {
      return TimeConstants.ONE_DAY_IN_MILLIS;
    } else if (granularity.toUpperCase().equals("HOURLY")) {
      return TimeConstants.ONE_HOUR_IN_MILLIS;
    } else if (granularity.toUpperCase().equals("WEEKLY")) {
      return TimeConstants.ONE_WEEK_IN_MILLIS;
    } else if (granularity.toUpperCase().equals("MINUTES")) {
      return TimeConstants.ONE_MINUTE_IN_MILLIS;
    } else if (granularity.toUpperCase().equals("SECONDS")) {
      return TimeConstants.ONE_SECOND_IN_MILLIS;
    } else if (granularity.toUpperCase().equals("MONTHLY")) {
      return TimeConstants.ONE_MONTH_IN_MILLIS;
    } else if (granularity.toUpperCase().equals("YEARLY")) {
      return TimeConstants.ONE_YEAR_IN_MILLIS;
    } else if (granularity.toUpperCase().endsWith("D")) {
      return (Integer.parseInt(granularity.substring(0, granularity.length() - 1).trim()) * TimeConstants.ONE_DAY_IN_MILLIS);
    } else if (granularity.toUpperCase().endsWith("H")) {
      return (Integer.parseInt(granularity.substring(0, granularity.length() - 1).trim()) * TimeConstants.ONE_HOUR_IN_MILLIS);
    } else {
      throw new UnsupportedOperationException("Input string: " + granularity
          + ", Accepted strings: 'SECONDS', 'MINUTES', 'HOURLY', 'DAILY', 'WEEKLY', "
          + "'MONTHLY', 'YEARLY', 'nD' (ex: '14D'), 'nH' (ex: '12H')");
    }
  }

}
