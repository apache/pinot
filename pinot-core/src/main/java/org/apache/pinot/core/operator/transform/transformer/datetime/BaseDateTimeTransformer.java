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
package org.apache.pinot.core.operator.transform.transformer.datetime;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.transform.transformer.DataTransformer;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeFormatUnitSpec.DateTimeTransformUnit;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.joda.time.Chronology;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;


/**
 * Base date time transformer to transform and bucket date time values from epoch/simple date format to epoch/simple
 * date format.
 * <p>NOTE: time size and time unit do not apply to simple date format.
 */
public abstract class BaseDateTimeTransformer<I, O> implements DataTransformer<I, O> {
  private final int _inputTimeSize;
  private final TimeUnit _inputTimeUnit;
  private final DateTimeFormatter _inputDateTimeFormatter;
  private final int _outputTimeSize;
  private final DateTimeTransformUnit _outputTimeUnit;
  private final DateTimeFormatter _outputDateTimeFormatter;
  private final DateTimeGranularitySpec _outputGranularity;
  private final long _outputGranularityMillis;
  private final SDFDateTimeTruncate _sdfDateTimeTruncate;
  private final MillisDateTimeTruncate _millisDateTimeTruncate;
  private final Chronology _bucketingChronology;
  // use reusable objects for parsing and formatting dates, instead of allocating them on each function call
  private final MutableDateTime _dateTime;
  private final StringBuilder _printBuffer;

  private interface SDFDateTimeTruncate {
    String truncate(MutableDateTime dateTime);
  }

  private interface MillisDateTimeTruncate {
    long truncate(MutableDateTime dateTime);
  }

  public BaseDateTimeTransformer(
      DateTimeFormatSpec inputFormat,
      DateTimeFormatSpec outputFormat,
      DateTimeGranularitySpec outputGranularity,
      @Nullable DateTimeZone bucketingTimeZone) {

    _inputTimeSize = inputFormat.getColumnSize();
    _inputTimeUnit = inputFormat.getColumnUnit();
    _inputDateTimeFormatter = inputFormat.getDateTimeFormatter();
    _outputTimeSize = outputFormat.getColumnSize();
    _outputTimeUnit = outputFormat.getColumnDateTimeTransformUnit();
    _outputDateTimeFormatter = outputFormat.getDateTimeFormatter();
    _outputGranularity = outputGranularity;
    _outputGranularityMillis = outputGranularity.granularityToMillis();
    _bucketingChronology = bucketingTimeZone != null ? ISOChronology.getInstance(bucketingTimeZone) : null;
    _dateTime = new MutableDateTime(0L, DateTimeZone.UTC);
    _printBuffer = new StringBuilder();

    final int size = _outputGranularity.getSize();

    // setup date time truncating based on output granularity
    // when size == 1, skip the needless set() calls
    switch (_outputGranularity.getTimeUnit()) {
      case MILLISECONDS:
        if (size != 1) {
          _sdfDateTimeTruncate = (dateTime) -> {
            dateTime.setMillisOfSecond((dateTime.getMillisOfSecond() / size) * size);
            return print(dateTime);
          };
          _millisDateTimeTruncate = (dateTime) -> {
            dateTime.setMillisOfSecond((dateTime.getMillisOfSecond() / size) * size);
            return dateTime.getMillis();
          };
        } else {
          _sdfDateTimeTruncate = (dateTime) -> print(dateTime);
          _millisDateTimeTruncate = (dateTime) -> dateTime.getMillis();
        }
        break;
      case SECONDS:
        if (size != 1) {
          _sdfDateTimeTruncate = (dateTime) -> {
            dateTime.setSecondOfMinute((dateTime.getSecondOfMinute() / size) * size);
            dateTime.secondOfMinute().roundFloor();
            return print(dateTime);
          };
          _millisDateTimeTruncate = (dateTime) -> {
            dateTime.setSecondOfMinute((dateTime.getSecondOfMinute() / size) * size);
            dateTime.secondOfMinute().roundFloor();
            return dateTime.getMillis();
          };
        } else {
          _sdfDateTimeTruncate = (dateTime) -> {
            dateTime.secondOfMinute().roundFloor();
            return print(dateTime);
          };
          _millisDateTimeTruncate = (dateTime) -> {
            dateTime.secondOfMinute().roundFloor();
            return dateTime.getMillis();
          };
        }
        break;
      case MINUTES:
        if (size != 1) {
          _sdfDateTimeTruncate = (dateTime) -> {
            dateTime.setMinuteOfHour((dateTime.getMinuteOfHour() / size) * size);
            dateTime.minuteOfHour().roundFloor();
            return print(dateTime);
          };
          _millisDateTimeTruncate = (dateTime) -> {
            dateTime.setMinuteOfHour((dateTime.getMinuteOfHour() / size) * size);
            dateTime.minuteOfHour().roundFloor();
            return dateTime.getMillis();
          };
        } else {
          _sdfDateTimeTruncate = (dateTime) -> {
            dateTime.minuteOfHour().roundFloor();
            return print(dateTime);
          };
          _millisDateTimeTruncate = (dateTime) -> {
            dateTime.minuteOfHour().roundFloor();
            return dateTime.getMillis();
          };
        }
        break;
      case HOURS:
        if (size != 1) {
          _sdfDateTimeTruncate = (dateTime) -> {
            dateTime.setHourOfDay((dateTime.getHourOfDay() / size) * size);
            dateTime.hourOfDay().roundFloor();
            return print(dateTime);
          };
          _millisDateTimeTruncate = (dateTime) -> {
            dateTime.setHourOfDay((dateTime.getHourOfDay() / size) * size);
            dateTime.hourOfDay().roundFloor();
            return dateTime.getMillis();
          };
        } else {
          _sdfDateTimeTruncate = (dateTime) -> {
            dateTime.hourOfDay().roundFloor();
            return print(dateTime);
          };
          _millisDateTimeTruncate = (dateTime) -> {
            dateTime.hourOfDay().roundFloor();
            return dateTime.getMillis();
          };
        }
        break;
      case DAYS:
        if (size != 1) {
          _sdfDateTimeTruncate = (dateTime) -> {
            dateTime.setDayOfMonth(((dateTime.getDayOfMonth() - 1) / size) * size + 1);
            dateTime.dayOfMonth().roundFloor();
            return print(dateTime);
          };
          _millisDateTimeTruncate = (dateTime) -> {
            dateTime.setDayOfMonth(((dateTime.getDayOfMonth() - 1) / size) * size + 1);
            dateTime.dayOfMonth().roundFloor();
            return dateTime.getMillis();
          };
        } else {
          _sdfDateTimeTruncate = (dateTime) -> {
            dateTime.dayOfMonth().roundFloor();
            return print(dateTime);
          };
          _millisDateTimeTruncate = (dateTime) -> {
            dateTime.dayOfMonth().roundFloor();
            return dateTime.getMillis();
          };
        }
        break;
      default:
        // truncating to MICROSECONDS or NANOSECONDS doesn't do anything because timestamps are stored
        // in milliseconds at most
        _sdfDateTimeTruncate = _outputDateTimeFormatter::print;
        _millisDateTimeTruncate = (dateTime) -> dateTime.getMillis();
        break;
    }
  }

  private String print(MutableDateTime dateTime) {
    _printBuffer.setLength(0);
    _outputDateTimeFormatter.printTo(_printBuffer, dateTime);
    return _printBuffer.toString();
  }

  protected long transformEpochToMillis(long epochTime) {
    return _inputTimeUnit.toMillis(epochTime * _inputTimeSize);
  }

  protected MutableDateTime toDateWithTZ(long millis) {
    _dateTime.setMillis(millis);
    _dateTime.setChronology(_bucketingChronology);
    return _dateTime;
  }

  protected long transformSDFToMillis(@Nonnull String sdfTime) {
    return _inputDateTimeFormatter.parseMillis(sdfTime);
  }

  protected long transformMillisToEpoch(long millisSinceEpoch) {
    return _outputTimeUnit.fromMillis(millisSinceEpoch) / _outputTimeSize;
  }

  protected String transformMillisToSDF(long millisSinceEpoch) {
    // convert to date time (with timezone), then truncate/bucket to the desired output granularity
    _dateTime.setZone(_outputDateTimeFormatter.getZone());
    _dateTime.setMillis(millisSinceEpoch);
    return _sdfDateTimeTruncate.truncate(_dateTime);
  }

  protected String truncateDateToSDF(MutableDateTime dateTime) {
    return _sdfDateTimeTruncate.truncate(dateTime);
  }

  protected long truncateToMillis(MutableDateTime dateTime) {
    return _millisDateTimeTruncate.truncate(dateTime);
  }

  protected long transformToOutputGranularity(long millisSinceEpoch) {
    return (millisSinceEpoch / _outputGranularityMillis) * _outputGranularityMillis;
  }

  protected boolean useCustomBucketingTimeZone() {
    return _bucketingChronology != null;
  }
}
