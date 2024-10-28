/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function.scalar;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.joda.time.Chronology;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;


/**
 * Equivalent to {@code DateTimeConversionTransformFunction}.
 */
public class DateTimeConvert {
  private DateTimeFormatSpec _inputFormatSpec;
  private DateTimeFormatSpec _outputFormatSpec;
  private DateTimeGranularitySpec _granularitySpec;
  private Chronology _bucketingChronology;
  private MutableDateTime _dateTime;
  private StringBuilder _buffer;

  @ScalarFunction
  public Object dateTimeConvert(String timeValueStr, String inputFormatStr, String outputFormatStr,
      String outputGranularityStr) {
    if (_inputFormatSpec == null) {
      _inputFormatSpec = new DateTimeFormatSpec(inputFormatStr);
      _outputFormatSpec = new DateTimeFormatSpec(outputFormatStr);
      _granularitySpec = new DateTimeGranularitySpec(outputGranularityStr);
      _dateTime = new MutableDateTime(0L, DateTimeZone.UTC);
      _buffer = new StringBuilder();
    }

    long timeValueMs = _inputFormatSpec.fromFormatToMillis(timeValueStr);
    if (_outputFormatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT) {
      DateTimeFormatter outputFormatter = _outputFormatSpec.getDateTimeFormatter();
      _dateTime.setMillis(timeValueMs);
      _dateTime.setZone(outputFormatter.getZone());
      int size = _granularitySpec.getSize();

      switch (_granularitySpec.getTimeUnit()) {
        case MILLISECONDS:
          _dateTime.setMillisOfSecond((_dateTime.getMillisOfSecond() / size) * size);
          break;
        case SECONDS:
          _dateTime.setSecondOfMinute((_dateTime.getSecondOfMinute() / size) * size);
          _dateTime.secondOfMinute().roundFloor();
          break;
        case MINUTES:
          _dateTime.setMinuteOfHour((_dateTime.getMinuteOfHour() / size) * size);
          _dateTime.minuteOfHour().roundFloor();
          break;
        case HOURS:
          _dateTime.setHourOfDay((_dateTime.getHourOfDay() / size) * size);
          _dateTime.hourOfDay().roundFloor();
          break;
        case DAYS:
          _dateTime.setDayOfMonth(((_dateTime.getDayOfMonth() - 1) / size) * size + 1);
          _dateTime.dayOfMonth().roundFloor();
          break;
        default:
          break;
      }
      _buffer.setLength(0);
      outputFormatter.printTo(_buffer, _dateTime);
      return _buffer.toString();
    } else {
      long granularityMs = _granularitySpec.granularityToMillis();
      long roundedTimeValueMs = timeValueMs / granularityMs * granularityMs;
      if (_outputFormatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.EPOCH) {
        return _outputFormatSpec.getColumnUnit().convert(roundedTimeValueMs, TimeUnit.MILLISECONDS)
            / _outputFormatSpec.getColumnSize();
      }
      // _outputFormatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.TIMESTAMP
      return _outputFormatSpec.fromMillisToFormat(roundedTimeValueMs);
    }
  }

  @ScalarFunction
  public Object dateTimeConvert(String timeValueStr, String inputFormatStr, String outputFormatStr,
      String outputGranularityStr, String bucketingTimeZone) {
    if (_inputFormatSpec == null) {
      _inputFormatSpec = new DateTimeFormatSpec(inputFormatStr);
      _outputFormatSpec = new DateTimeFormatSpec(outputFormatStr);
      _granularitySpec = new DateTimeGranularitySpec(outputGranularityStr);

      try {
        // we're not using TimeZone.getTimeZone() because it's globally synchronized
        // and returns default TZ when str makes no sense
        _bucketingChronology =
            ISOChronology.getInstance(DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of(bucketingTimeZone))));
      } catch (DateTimeException dte) {
        throw new IllegalArgumentException("Error parsing bucketing time zone: " + dte.getMessage(), dte);
      }

      _dateTime = new MutableDateTime(0L, DateTimeZone.UTC);
      _buffer = new StringBuilder();
    }

    long timeValueMs = _inputFormatSpec.fromFormatToMillis(timeValueStr);

    _dateTime.setMillis(timeValueMs);
    _dateTime.setChronology(_bucketingChronology);
    int size = _granularitySpec.getSize();

    switch (_granularitySpec.getTimeUnit()) {
      case MILLISECONDS:
        _dateTime.setMillisOfSecond((_dateTime.getMillisOfSecond() / size) * size);
        break;
      case SECONDS:
        _dateTime.setSecondOfMinute((_dateTime.getSecondOfMinute() / size) * size);
        _dateTime.secondOfMinute().roundFloor();
        break;
      case MINUTES:
        _dateTime.setMinuteOfHour((_dateTime.getMinuteOfHour() / size) * size);
        _dateTime.minuteOfHour().roundFloor();
        break;
      case HOURS:
        _dateTime.setHourOfDay((_dateTime.getHourOfDay() / size) * size);
        _dateTime.hourOfDay().roundFloor();
        break;
      case DAYS:
        _dateTime.setDayOfMonth(((_dateTime.getDayOfMonth() - 1) / size) * size + 1);
        _dateTime.dayOfMonth().roundFloor();
        break;
      default:
        break;
    }

    if (_outputFormatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.SIMPLE_DATE_FORMAT) {
      _buffer.setLength(0);
      DateTimeFormatter outputFormatter = _outputFormatSpec.getDateTimeFormatter();
      outputFormatter.printTo(_buffer, _dateTime);
      return _buffer.toString();
    } else {
      timeValueMs = _dateTime.getMillis();
      return _outputFormatSpec.getColumnUnit().convert(timeValueMs, TimeUnit.MILLISECONDS)
          / _outputFormatSpec.getColumnSize();
    }
  }
}
