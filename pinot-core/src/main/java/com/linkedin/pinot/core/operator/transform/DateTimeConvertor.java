package com.linkedin.pinot.core.operator.transform;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat;
import com.linkedin.pinot.common.utils.time.DateTimeFieldSpecUtils;

/**
 * Convertor for conversion of datetime values from an epoch/sdf format to another epoch/sdf format
 */
public abstract class DateTimeConvertor {

  private static final DateTime EPOCH_START_DATE = new DateTime(0L, DateTimeZone.UTC);

  /**
   * Time unit enum with range from MILLISECONDS to YEARS
   */
  public enum DateTimeTransformUnit {
    YEARS,
    MONTHS,
    WEEKS,
    DAYS,
    HOURS,
    MINUTES,
    SECONDS,
    MILLISECONDS;
  }

  private int inputTimeSize;
  private TimeUnit inputTimeUnit;
  private DateTimeFormatter inputDateTimeFormatter = null;

  private int outputTimeSize;
  private DateTimeTransformUnit outputTimeUnit;
  private DateTimeFormatter outputDateTimeFormatter = null;

  private Long outputGranularityMillis;

  public DateTimeConvertor(String inputFormat, String outputFormat, String outputGranularity) {

    inputTimeSize = DateTimeFieldSpecUtils.getColumnSizeFromFormat(inputFormat);
    inputTimeUnit = DateTimeFieldSpecUtils.getColumnUnitFromFormat(inputFormat);
    TimeFormat inputTimeFormat = DateTimeFieldSpecUtils.getTimeFormatFromFormat(inputFormat);
    if (inputTimeFormat.equals(TimeFormat.SIMPLE_DATE_FORMAT)) {
      String inputSDFFormat = DateTimeFieldSpecUtils.getSDFPatternFromFormat(inputFormat);
      inputDateTimeFormatter = DateTimeFormat.forPattern(inputSDFFormat).withZoneUTC();
    }

    outputTimeSize = DateTimeFieldSpecUtils.getColumnSizeFromFormat(outputFormat);
    String[] outputFormatTokens = outputFormat.split(DateTimeFieldSpecUtils.COLON_SEPARATOR);
    outputTimeUnit = DateTimeTransformUnit.valueOf(outputFormatTokens[DateTimeFieldSpecUtils.FORMAT_UNIT_POSITION]);
    TimeFormat outputTimeFormat = DateTimeFieldSpecUtils.getTimeFormatFromFormat(outputFormat);
    if (outputTimeFormat.equals(TimeFormat.SIMPLE_DATE_FORMAT)) {
      String outputSDFFormat = DateTimeFieldSpecUtils.getSDFPatternFromFormat(outputFormat);
      outputDateTimeFormatter = DateTimeFormat.forPattern(outputSDFFormat).withZoneUTC();
    }

    outputGranularityMillis = DateTimeFieldSpecUtils.granularityToMillis(outputGranularity);
  }



  /**
   * Converts the datetimeValue from one format to another
   * @param dateTimeValue
   * @return
   */
  public abstract Long convert(Object dateTimeValue);

  /**
   * Converts the input in epoch format to millis
   * @param dateTimeValue
   * @return
   */
  protected Long convertEpochToMillis(Object dateTimeValue) {

    Long timeColumnValueMS = 0L;
    if (dateTimeValue != null) {
      timeColumnValueMS = TimeUnit.MILLISECONDS.convert((Long) dateTimeValue * inputTimeSize, inputTimeUnit);
    }
    return timeColumnValueMS;
  }

  /**
   * Converts the input from sfd format to millis
   * @param dateTimeValue
   * @return
   */
  protected Long convertSDFToMillis(Object dateTimeValue) {

    Long timeColumnValueMS = 0L;
    if (dateTimeValue != null) {
      timeColumnValueMS = inputDateTimeFormatter.parseMillis(String.valueOf(dateTimeValue));
    }
    return timeColumnValueMS;
  }

  /**
   * Given a timestamp in millis, convert it to epoch format, while also handling time units
   * such as WEEKS, MONTHS, YEARS
   * <ul>
   * <li>1) given dateTimeColumnValueMS = 1498892400000 and format=1:HOURS:EPOCH,
   * dateTimeSpec.fromMillis(1498892400000) = 416359 (i.e. dateTimeColumnValueMS/(1000*60*60))</li>
   * <li>2) given dateTimeColumnValueMS = 1498892400000 and format=1:WEEKS:EPOCH,
   * dateTimeSpec.fromMillis(1498892400000) = 2478 (i.e. timeColumnValueMS/(1000*60*60*24*7))</li>
   * </ul>
   * @param dateTimeValueMillis - date time value in millis to convert
   * @param outputFormat
   * @return
   */
  protected Long convertMillisToEpoch(Long dateTimeValueMillis) {
    Long convertedDateTime = 0L;

    if (dateTimeValueMillis != null) {
      MutableDateTime inputDateTime = new MutableDateTime(dateTimeValueMillis, DateTimeZone.UTC);
      Interval interval = new Interval(EPOCH_START_DATE, inputDateTime);
      switch (outputTimeUnit) {

      case YEARS:
        convertedDateTime = (long) Years.yearsIn(interval).getYears();
        break;
      case MONTHS:
        convertedDateTime = (long) Months.monthsIn(interval).getMonths();
        break;
      case WEEKS:
        convertedDateTime = (long) Weeks.weeksIn(interval).getWeeks();
        break;
      case DAYS:
        convertedDateTime = (long) Days.daysIn(interval).getDays();
        break;
      case HOURS:
        convertedDateTime = (long) Hours.hoursIn(interval).getHours();
        break;
      case MINUTES:
        convertedDateTime = (long) Minutes.minutesIn(interval).getMinutes();
        break;
      case SECONDS:
        convertedDateTime = (long) Seconds.secondsIn(interval).getSeconds();
        break;
      case MILLISECONDS:
        convertedDateTime = dateTimeValueMillis;
        break;
      default:
        throw new IllegalArgumentException("Illegal argument for time unit: " + outputTimeUnit);
      }
      convertedDateTime = convertedDateTime / outputTimeSize;
    }
    return convertedDateTime;
  }

  /**
   * Given a timestamp in millis, convert it to sdf format
   * <ul>
   * <li>given dateTimeColumnValueMS = 1498892400000 and
   * format=1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd, dateTimeSpec.fromMillis(1498892400000) = 20170701</li>
   * </ul>
   * @param dateTimeValueMillis - date time value in millis to convert
   * @param outputFormat
   * @return
   */
  protected Long convertMillisToSDF(Long dateTimeValueMillis) {
    Long convertedDateTime = 0L;
    if (dateTimeValueMillis != null) {
      convertedDateTime = Long.valueOf(outputDateTimeFormatter.print(dateTimeValueMillis));
    }
    return convertedDateTime;
  }

  /**
   * Helper method to bucket a given timestamp in millis, into a output granularity
   * @param dateTimeColumnValueMS - value to bucket
   * @return millis value, bucketed in the granularity
   */
  protected Long bucketDateTimeValueMS(Long dateTimeColumnValueMS) {
    long bucketedDateTimeValueMS = 0L;
    if (dateTimeColumnValueMS != null) {
      bucketedDateTimeValueMS = (dateTimeColumnValueMS / outputGranularityMillis) * outputGranularityMillis;
    }
    return bucketedDateTimeValueMS;
  }

}
