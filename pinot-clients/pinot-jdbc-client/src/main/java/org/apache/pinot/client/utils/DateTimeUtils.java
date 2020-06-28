package org.apache.pinot.client.utils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class DateTimeUtils {
  private static final String TIMESTAMP_FORMAT = "yyyy-mm-dd HH:MM:SS";
  private static final String DATE_FORMAT = "yyyy-mm-dd";
  private static final SimpleDateFormat _dateFormat = new SimpleDateFormat(DATE_FORMAT);
  private static final SimpleDateFormat _timestampFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);

  public static Date getDateFromString(String value, Calendar cal)
      throws ParseException {
    java.util.Date date = _dateFormat.parse(value);
    cal.setTime(date);
    Date sqlDate = new Date(cal.getTimeInMillis());
    return sqlDate;
  }

  public static Time getTimeFromString(String value, Calendar cal)
      throws ParseException {
    java.util.Date date = _timestampFormat.parse(value);
    cal.setTime(date);
    Time sqlTime = new Time(cal.getTimeInMillis());
    return sqlTime;
  }

  public static Timestamp getTimestampFromString(String value, Calendar cal)
      throws ParseException {
    java.util.Date date = _timestampFormat.parse(value);
    cal.setTime(date);
    Timestamp sqlTime = new Timestamp(cal.getTimeInMillis());
    return sqlTime;
  }

  public static Timestamp getTimestampFromLong(Long value) {
    Timestamp sqlTime = new Timestamp(value);
    return sqlTime;
  }

  public static String dateToString(Date date) {
    return _dateFormat.format(date);
  }

  public static String timeToString(Time time) {
    return _timestampFormat.format(time);
  }

  public static String timeStampToString(Timestamp timestamp) {
    return _timestampFormat.format(timestamp);
  }

  public static long timeStampToLong(Timestamp timestamp) {
    return timestamp.getTime();
  }
}
