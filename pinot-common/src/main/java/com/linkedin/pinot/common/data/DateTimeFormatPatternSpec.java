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
package com.linkedin.pinot.common.data;

import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static com.linkedin.pinot.common.data.DateTimeFieldSpec.*;


public class DateTimeFormatPatternSpec {

  /** eg: yyyyMMdd tz(CST) or yyyyMMdd HH tz(GMT+0700) or yyyyMMddHH tz(America/Chicago) **/
  private static final Pattern SDF_PATTERN_WITH_TIMEZONE = Pattern.compile("^(.+)( tz[ ]*\\((.+)\\))[ ]*");
  private static final int SDF_PATTERN_GROUP = 1;
  private static final int TIMEZONE_GROUP = 3;
  public static final DateTimeZone DEFAULT_DATETIMEZONE = DateTimeZone.UTC;

  private TimeFormat _timeFormat;
  private String _sdfPattern = null;
  private DateTimeZone _dateTimeZone = DEFAULT_DATETIMEZONE;
  private transient DateTimeFormatter _dateTimeFormatter;

  public DateTimeFormatPatternSpec(String timeFormat, String sdfPatternWithTz) {
    _timeFormat = TimeFormat.valueOf(timeFormat);
    if (_timeFormat.equals(TimeFormat.SIMPLE_DATE_FORMAT)) {
      Matcher m = SDF_PATTERN_WITH_TIMEZONE.matcher(sdfPatternWithTz);
      _sdfPattern = sdfPatternWithTz;
      if (m.find()) {
        _sdfPattern = m.group(SDF_PATTERN_GROUP).trim();
        String timezoneString = m.group(TIMEZONE_GROUP).trim();
        _dateTimeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timezoneString));
      }
      _dateTimeFormatter = DateTimeFormat.forPattern(_sdfPattern).withZone(_dateTimeZone);
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
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    DateTimeFormatPatternSpec that = (DateTimeFormatPatternSpec) o;

    return EqualityUtils.isEqual(_timeFormat, that._timeFormat) &&
        EqualityUtils.isEqual(_sdfPattern, that._sdfPattern) &&
        EqualityUtils.isEqual(_dateTimeZone, that._dateTimeZone);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_timeFormat);
    result = EqualityUtils.hashCodeOf(result, _sdfPattern);
    result = EqualityUtils.hashCodeOf(result, _dateTimeZone);
    return result;
  }

  @Override
  public String toString() {
    return "DateTimeFormatPatternSpec{" + "_timeFormat=" + _timeFormat + ", _sdfPattern='" + _sdfPattern + '\''
        + ", _dateTimeZone=" + _dateTimeZone + ", _dateTimeFormatter=" + _dateTimeFormatter + '}';
  }
}
