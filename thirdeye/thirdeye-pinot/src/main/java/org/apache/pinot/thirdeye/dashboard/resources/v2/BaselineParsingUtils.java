/*
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

package org.apache.pinot.thirdeye.dashboard.resources.v2;

import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregate;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregateType;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineNone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.DateTimeZone;


public class BaselineParsingUtils {
  private static final Pattern PATTERN_NONE = Pattern.compile("none");
  private static final Pattern PATTERN_PREDICTED = Pattern.compile("predicted");
  private static final Pattern PATTERN_CURRENT = Pattern.compile("current");
  private static final Pattern PATTERN_HOUR_OVER_HOUR = Pattern.compile("ho([1-9][0-9]*)h");
  private static final Pattern PATTERN_DAY_OVER_DAY = Pattern.compile("do([1-9][0-9]*)d");
  private static final Pattern PATTERN_WEEK_OVER_WEEK = Pattern.compile("wo([1-9][0-9]*)w");
  private static final Pattern PATTERN_MONTH_OVER_MONTH = Pattern.compile("mo([1-9][0-9]*)m");
  private static final Pattern PATTERN_YEAR_OVER_YEAR = Pattern.compile("yo([1-9][0-9]*)y");
  private static final Pattern PATTERN_MEAN = Pattern.compile("mean([1-9][0-9]*)(h|d|w|m)");
  private static final Pattern PATTERN_MEDIAN = Pattern.compile("median([1-9][0-9]*)(h|d|w|m)");
  private static final Pattern PATTERN_MIN = Pattern.compile("min([1-9][0-9]*)(h|d|w|m)");
  private static final Pattern PATTERN_MAX = Pattern.compile("max([1-9][0-9]*)(h|d|w|m)");

  private enum Unit {
    HOUR("h"),
    DAY("d"),
    WEEK("w"),
    MONTH("m"),
    YEAR("y");

    final String unit;

    Unit(String unit) {
      this.unit = unit;
    }

    static Unit fromUnit(String unit) {
      for (Unit u : Unit.values()) {
        if (u.unit.equals(unit)) {
          return u;
        }
      }
      throw new IllegalArgumentException(String.format("Unknown unit '%s'", unit));
    }
  }

  /**
   * Returns a configured instance of Baseline for the given, named offset. The method uses slice and
   * timezone information to adjust for daylight savings time.
   *
   * <p>Supported offsets:</p>
   * <pre>
   *   current   the time range as specified by start and end)
   *   none      empty time range
   *   hoXh      hour-over-hour data points with a lag of X hours)
   *   doXd      day-over-day data points with a lag of X days)
   *   woXw      week-over-week data points with a lag of X weeks)
   *   moXm      month-over-month data points with a lag of X months)
   *   yoXy      year-over-year data points with a lag of X years)
   *   meanXU    average of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)
   *   medianXU  median of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)
   *   minXU     minimum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)
   *   maxXU     maximum of data points from the the past X units (hour, day, month, week), with a lag of 1 unit)
   * </pre>
   *
   * @param offset offset identifier
   * @param timeZoneString timezone identifier (location long format)
   * @return Baseline instance
   * @throws IllegalArgumentException if the offset cannot be parsed
   */
  public static Baseline parseOffset(String offset, String timeZoneString) {
    DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);

    Matcher mCurrent = PATTERN_CURRENT.matcher(offset);
    if (mCurrent.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.SUM, 1, 0, timeZone);
    }

    Matcher mNone = PATTERN_NONE.matcher(offset);
    if (mNone.find()) {
      return new BaselineNone();
    }

    // TODO link with generic metric baseline prediction when available
    Matcher mPredicted = PATTERN_PREDICTED.matcher(offset);
    if (mPredicted.find()) {
      return new BaselineNone();
    }

    BaselineAggregateType agg = null;
    int shift = Integer.MIN_VALUE;
    int count = Integer.MIN_VALUE;
    Unit unit = null;

    Matcher mHourOverHour = PATTERN_HOUR_OVER_HOUR.matcher(offset);
    if (mHourOverHour.find()) {
      agg = BaselineAggregateType.SUM;
      shift = Integer.valueOf(mHourOverHour.group(1));
      count = 1;
      unit = Unit.HOUR;
    }

    Matcher mDayOverDay = PATTERN_DAY_OVER_DAY.matcher(offset);
    if (mDayOverDay.find()) {
      agg = BaselineAggregateType.SUM;
      shift = Integer.valueOf(mDayOverDay.group(1));
      count = 1;
      unit = Unit.DAY;
    }

    Matcher mWeekOverWeek = PATTERN_WEEK_OVER_WEEK.matcher(offset);
    if (mWeekOverWeek.find()) {
      agg = BaselineAggregateType.SUM;
      shift = Integer.valueOf(mWeekOverWeek.group(1));
      count = 1;
      unit = Unit.WEEK;
    }

    Matcher mMonthOverMonth = PATTERN_MONTH_OVER_MONTH.matcher(offset);
    if (mMonthOverMonth.find()) {
      agg = BaselineAggregateType.SUM;
      shift = Integer.valueOf(mMonthOverMonth.group(1));
      count = 1;
      unit = Unit.MONTH;
    }

    Matcher mYearOverYear = PATTERN_YEAR_OVER_YEAR.matcher(offset);
    if (mYearOverYear.find()) {
      agg = BaselineAggregateType.SUM;
      shift = Integer.valueOf(mYearOverYear.group(1));
      count = 1;
      unit = Unit.YEAR;
    }

    Matcher mMean = PATTERN_MEAN.matcher(offset);
    if (mMean.find()) {
      agg = BaselineAggregateType.MEAN;
      shift = 1;
      count = Integer.valueOf(mMean.group(1));
      unit = Unit.fromUnit(mMean.group(2));
    }

    Matcher mMedian = PATTERN_MEDIAN.matcher(offset);
    if (mMedian.find()) {
      agg = BaselineAggregateType.MEDIAN;
      shift = 1;
      count = Integer.valueOf(mMedian.group(1));
      unit = Unit.fromUnit(mMedian.group(2));
    }

    Matcher mMin = PATTERN_MIN.matcher(offset);
    if (mMin.find()) {
      agg = BaselineAggregateType.MIN;
      shift = 1;
      count = Integer.valueOf(mMin.group(1));
      unit = Unit.fromUnit(mMin.group(2));
    }

    Matcher mMax = PATTERN_MAX.matcher(offset);
    if (mMax.find()) {
      agg = BaselineAggregateType.MAX;
      shift = 1;
      count = Integer.valueOf(mMax.group(1));
      unit = Unit.fromUnit(mMax.group(2));
    }

    if (agg == null || unit == null || shift == Integer.MIN_VALUE || count == Integer.MIN_VALUE ) {
      throw new IllegalArgumentException(String.format("Unsupported offset '%s'", offset));
    }

    switch (unit) {
      case HOUR:
        return BaselineAggregate.fromHourOverHour(agg, count, shift, timeZone);
      case DAY:
        return BaselineAggregate.fromDayOverDay(agg, count, shift, timeZone);
      case WEEK:
        return BaselineAggregate.fromWeekOverWeek(agg, count, shift, timeZone);
      case MONTH:
        return BaselineAggregate.fromMonthOverMonth(agg, count, shift, timeZone);
      case YEAR:
        return BaselineAggregate.fromYearOverYear(agg, count, shift, timeZone);
      default:
        throw new IllegalArgumentException(String.format("Unsupported unit '%s'", unit));
    }
  }
}
