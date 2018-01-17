/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.datetime.convertor;

import com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat;
import com.linkedin.pinot.common.data.DateTimeFormatSpec;
import com.linkedin.pinot.common.data.DateTimeFormatUnitSpec.DateTimeTransformUnit;
import com.linkedin.pinot.common.data.DateTimeGranularitySpec;
import java.util.concurrent.TimeUnit;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationField;
import org.joda.time.DurationFieldType;
import org.joda.time.format.DateTimeFormatter;

/**
 * Convertor for conversion of datetime values from an epoch/sdf format to another epoch/sdf format
 */
public abstract class DateTimeConvertor {

  private static final DateTime EPOCH_START_DATE = new DateTime(0L, DateTimeZone.UTC);
  private static final Chronology EPOCH_START_CHRONOLOGY = DateTimeUtils.getInstantChronology(EPOCH_START_DATE);
  private static final long EPOCH_START_DATE_MILLIS = 0L;


  private int inputTimeSize;
  private TimeUnit inputTimeUnit;
  private DateTimeFormatter inputDateTimeFormatter;

  private int outputTimeSize;
  private DateTimeTransformUnit outputTimeUnit;
  private DurationField outputDurationField;
  private DateTimeFormatter outputDateTimeFormatter;

  private Long outputGranularityMillis;

  public DateTimeConvertor(DateTimeFormatSpec inputFormat, DateTimeFormatSpec outputFormat,
      DateTimeGranularitySpec outputGranularity) {

    inputTimeSize = inputFormat.getColumnSize();
    inputTimeUnit = inputFormat.getColumnUnit();
    TimeFormat inputTimeFormat = inputFormat.getTimeFormat();
    if (inputTimeFormat.equals(TimeFormat.SIMPLE_DATE_FORMAT)) {
      inputDateTimeFormatter = inputFormat.getDateTimeFormatter();
    }

    outputTimeSize = outputFormat.getColumnSize();
    outputTimeUnit = outputFormat.getColumnDateTimeTransformUnit();
    TimeFormat outputTimeFormat = outputFormat.getTimeFormat();
    if (outputTimeFormat.equals(TimeFormat.EPOCH)) {
      DurationFieldType durationFieldType;
      switch (outputTimeUnit) {

      case YEARS:
        durationFieldType = DurationFieldType.years();
        break;
      case MONTHS:
        durationFieldType = DurationFieldType.months();
        break;
      case WEEKS:
        durationFieldType = DurationFieldType.weeks();
        break;
      case DAYS:
        durationFieldType = DurationFieldType.days();
        break;
      case HOURS:
        durationFieldType = DurationFieldType.hours();
        break;
      case MINUTES:
        durationFieldType = DurationFieldType.minutes();
        break;
      case SECONDS:
        durationFieldType = DurationFieldType.seconds();
        break;
      case MILLISECONDS:
        durationFieldType = DurationFieldType.millis();
        break;
      default:
        throw new IllegalArgumentException("Illegal argument for time unit: " + outputTimeUnit);
      }
      outputDurationField = durationFieldType.getField(EPOCH_START_CHRONOLOGY);

    } else {
      outputDateTimeFormatter = outputFormat.getDateTimeFormatter();
    }

    outputGranularityMillis = outputGranularity.granularityToMillis();
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
   *
   * Avoided using a switch case on {@link DateTimeTransformUnit}
   * by simulating the functionality of {@link org.joda.time.base.BaseSingleFieldPeriod#between()}
   * <ul>
   * <li>1) given dateTimeColumnValueMS = 1498892400000 and format=1:HOURS:EPOCH,
   * convertMillisToEpoch(1498892400000) = 416359 (i.e. dateTimeColumnValueMS/(1000*60*60))</li>
   * <li>2) given dateTimeColumnValueMS = 1498892400000 and format=1:WEEKS:EPOCH,
   * convertMillisToEpoch(1498892400000) = 2478 (i.e. dateTimeColumnValueMS/(1000*60*60*24*7))</li>
   * </ul>
   * @param dateTimeValueMillis - date time value in millis to convert
   * @return
   */
  protected Long convertMillisToEpoch(Long dateTimeValueMillis) {
    Long convertedDateTime = 0L;
    if (dateTimeValueMillis != null) {
      convertedDateTime = outputDurationField
          .getDifferenceAsLong(dateTimeValueMillis, EPOCH_START_DATE_MILLIS) / outputTimeSize;
    }
    return convertedDateTime;
  }

  /**
   * Given a timestamp in millis, convert it to sdf format
   * <ul>
   * <li>given dateTimeColumnValueMS = 1498892400000 and
   * format=1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd, convertMillisToSDF(1498892400000) = 20170701</li>
   * </ul>
   * @param dateTimeValueMillis - date time value in millis to convert
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
