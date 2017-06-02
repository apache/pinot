package com.linkedin.thirdeye.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


public class JodaDateTimeUtils {
  private static DateTimeFormatter ISO_DATETIME_FORMATTER = ISODateTimeFormat.dateTimeParser();

  /**
   * Parse the joda DateTime instance to ISO string, e.g. 2017-05-31T00:00:00-07:00
   * @param dateTime
   *      A joda DateTime instance
   * @return
   *      An ISO DateTime String
   */
  public static String toIsoDateTimeString(DateTime dateTime){
    return dateTime.toString(ISO_DATETIME_FORMATTER);
  }

  /**
   * Parse the ISO DateTime String to a joda DateTime instance
   * @param isoDateTimeString
   *      The ISO DateTime String, e.g. 2017-05-31T00:00:00-07:00
   * @return
   *      A joda DateTime instance
   */
  public static DateTime toDateTime(String isoDateTimeString) {
    return ISO_DATETIME_FORMATTER.parseDateTime(isoDateTimeString);
  }
}
