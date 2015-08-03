package com.linkedin.thirdeye.anomaly.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.linkedin.thirdeye.api.TimeGranularity;

/**
 *
 */
public class TimeGranularityUtils {

  /**
   * @param tg
   * @return
   *  The number of milliseconds of the time granularity representation
   */
  public static long toMillis(TimeGranularity tg) {
    return tg.getUnit().toMillis(tg.getSize());
  }

  /**
   * @param timeWindow
   * @param tg
   * @return
   *  New timeWindow representing the date truncated by time granularity
   */
  public static long truncateBy(long timeWindow, TimeGranularity tg) {
    DateTime dt = new DateTime(timeWindow, DateTimeZone.UTC);
    int millisOfDay = dt.getMillisOfDay();
    millisOfDay = (int) ((millisOfDay / toMillis(tg)) * toMillis(tg));
    return dt.withMillisOfDay(millisOfDay).getMillis();
  }

  private TimeGranularityUtils() {}

}
