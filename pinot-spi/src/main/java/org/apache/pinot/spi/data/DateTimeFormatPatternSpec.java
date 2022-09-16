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
package org.apache.pinot.spi.data;

import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DateTimeFormatPatternSpec {
  public static final Logger LOGGER = LoggerFactory.getLogger(DateTimeFormatPatternSpec.class);

  public static final DateTimeZone DEFAULT_DATE_TIME_ZONE = DateTimeZone.UTC;
  public static final Locale DEFAULT_LOCALE = Locale.ENGLISH;

  public static final DateTimeFormatPatternSpec EPOCH = new DateTimeFormatPatternSpec(TimeFormat.EPOCH);
  public static final DateTimeFormatPatternSpec TIMESTAMP = new DateTimeFormatPatternSpec(TimeFormat.TIMESTAMP);

  /** eg: yyyyMMdd tz(CST) or yyyyMMdd HH tz(GMT+0700) or yyyyMMddHH tz(America/Chicago) **/
  private static final Pattern SDF_PATTERN_WITH_TIMEZONE = Pattern.compile("^(.+)( tz[ ]*\\((.+)\\))[ ]*");
  private static final int SDF_PATTERN_GROUP = 1;
  private static final int TIME_ZONE_GROUP = 3;

  private final TimeFormat _timeFormat;
  private final String _sdfPattern;
  private final DateTimeZone _dateTimeZone;
  private transient final DateTimeFormatter _dateTimeFormatter;

  public DateTimeFormatPatternSpec(TimeFormat timeFormat) {
    this(timeFormat, null);
  }

  public DateTimeFormatPatternSpec(TimeFormat timeFormat, @Nullable String sdfPatternWithTz) {
    _timeFormat = timeFormat;
    if (timeFormat == TimeFormat.SIMPLE_DATE_FORMAT) {
      if (sdfPatternWithTz != null) {
        Matcher m = SDF_PATTERN_WITH_TIMEZONE.matcher(sdfPatternWithTz);
        if (m.find()) {
          _sdfPattern = m.group(SDF_PATTERN_GROUP).trim();
          String timeZone = m.group(TIME_ZONE_GROUP).trim();
          try {
            _dateTimeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZone));
          } catch (Exception e) {
            throw new IllegalArgumentException("Invalid time zone: " + timeZone);
          }
        } else {
          _sdfPattern = sdfPatternWithTz;
          _dateTimeZone = DEFAULT_DATE_TIME_ZONE;
        }
      } else {
        _sdfPattern = null;
        _dateTimeZone = DEFAULT_DATE_TIME_ZONE;
      }
      try {
        if (_sdfPattern == null) {
          LOGGER.debug("SIMPLE_DATE_FORMAT pattern was found to be null. Using ISODateTimeFormat as default");
          _dateTimeFormatter =
              ISODateTimeFormat.dateOptionalTimeParser().withZone(_dateTimeZone).withLocale(DEFAULT_LOCALE);
        } else {
          _dateTimeFormatter =
              DateTimeFormat.forPattern(_sdfPattern).withZone(_dateTimeZone).withLocale(DEFAULT_LOCALE);
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid SIMPLE_DATE_FORMAT pattern: " + _sdfPattern);
      }
    } else {
      _sdfPattern = null;
      _dateTimeZone = DEFAULT_DATE_TIME_ZONE;
      _dateTimeFormatter = null;
    }
  }

  public DateTimeFormatPatternSpec(TimeFormat timeFormat, @Nullable String sdfPattern, @Nullable String timeZone) {
    _timeFormat = timeFormat;
    if (_timeFormat == TimeFormat.SIMPLE_DATE_FORMAT) {
      _sdfPattern = sdfPattern;
      if (timeZone != null) {
        try {
          _dateTimeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZone));
        } catch (Exception e) {
          throw new IllegalArgumentException("Invalid time zone: " + timeZone);
        }
      } else {
        _dateTimeZone = DEFAULT_DATE_TIME_ZONE;
      }
      try {
        if (_sdfPattern == null) {
          LOGGER.debug("SIMPLE_DATE_FORMAT pattern was found to be null. Using ISODateTimeFormat as default");
          _dateTimeFormatter =
              ISODateTimeFormat.dateOptionalTimeParser().withZone(_dateTimeZone).withLocale(DEFAULT_LOCALE);
        } else {
          _dateTimeFormatter =
              DateTimeFormat.forPattern(_sdfPattern).withZone(_dateTimeZone).withLocale(DEFAULT_LOCALE);
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid SIMPLE_DATE_FORMAT pattern: " + _sdfPattern);
      }
    } else {
      _sdfPattern = null;
      _dateTimeZone = DEFAULT_DATE_TIME_ZONE;
      _dateTimeFormatter = null;
    }
  }

  public TimeFormat getTimeFormat() {
    return _timeFormat;
  }

  public String getSdfPattern() {
    return _sdfPattern;
  }

  public DateTimeZone getDateTimeZone() {
    return _dateTimeZone;
  }

  public DateTimeFormatter getDateTimeFormatter() {
    return _dateTimeFormatter;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DateTimeFormatPatternSpec that = (DateTimeFormatPatternSpec) o;
    return _timeFormat == that._timeFormat && Objects.equals(_sdfPattern, that._sdfPattern) && _dateTimeZone.equals(
        that._dateTimeZone);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_timeFormat, _sdfPattern, _dateTimeZone);
  }

  @Override
  public String toString() {
    return "DateTimeFormatPatternSpec{" + "_timeFormat=" + _timeFormat + ", _sdfPattern='" + _sdfPattern + '\''
        + ", _dateTimeZone=" + _dateTimeZone + '}';
  }
}
