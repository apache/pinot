/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.dashboard.resources.v2;

import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineNone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.DateTimeZone;


public class BaselineParsingUtils {
  private static final Pattern PATTERN_NONE = Pattern.compile("none");
  private static final Pattern PATTERN_PREDICTED = Pattern.compile("predicted");
  private static final Pattern PATTERN_CURRENT = Pattern.compile("current");
  private static final Pattern PATTERN_WEEK_OVER_WEEK = Pattern.compile("wo([1-9][0-9]*)w");
  private static final Pattern PATTERN_MEAN = Pattern.compile("mean([1-9][0-9]*)w");
  private static final Pattern PATTERN_MEDIAN = Pattern.compile("median([1-9][0-9]*)w");
  private static final Pattern PATTERN_MIN = Pattern.compile("min([1-9][0-9]*)w");
  private static final Pattern PATTERN_MAX = Pattern.compile("max([1-9][0-9]*)w");

  /**
   * Returns a configured instance of Baseline for the given, named offset. The method uses slice and
   * timezone information to adjust for daylight savings time.
   *
   * <p>Supported offsets:</p>
   * <pre>
   *   current   the time range as specified by start and end)
   *   none      empty time range
   *   woXw      week-over-week data points with a lag of X weeks)
   *   meanXw    average of data points from the the past X weeks, with a lag of 1 week)
   *   medianXw  median of data points from the the past X weeks, with a lag of 1 week)
   *   minXw     minimum of data points from the the past X weeks, with a lag of 1 week)
   *   maxXw     maximum of data points from the the past X weeks, with a lag of 1 week)
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

    Matcher mWeekOverWeek = PATTERN_WEEK_OVER_WEEK.matcher(offset);
    if (mWeekOverWeek.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.SUM, 1, Integer.valueOf(mWeekOverWeek.group(1)), timeZone);
    }

    Matcher mMean = PATTERN_MEAN.matcher(offset);
    if (mMean.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MEAN, Integer.valueOf(mMean.group(1)), 1, timeZone);
    }

    Matcher mMedian = PATTERN_MEDIAN.matcher(offset);
    if (mMedian.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MEDIAN, Integer.valueOf(mMedian.group(1)), 1, timeZone);
    }

    Matcher mMin = PATTERN_MIN.matcher(offset);
    if (mMin.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MIN, Integer.valueOf(mMin.group(1)), 1, timeZone);
    }

    Matcher mMax = PATTERN_MAX.matcher(offset);
    if (mMax.find()) {
      return BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MAX, Integer.valueOf(mMax.group(1)), 1, timeZone);
    }

    throw new IllegalArgumentException(String.format("Unknown offset '%s'", offset));
  }
}
