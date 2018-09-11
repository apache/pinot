/**
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

package com.linkedin.thirdeye.anomalydetection.model.data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.joda.time.Duration;
import org.joda.time.Interval;

public class SeasonalDataModel extends AbstractDataModel {
  public static final String SEASONAL_PERIOD = "seasonalPeriod";
  public static final String SEASONAL_SIZE = "seasonalSize";
  public static final String SEASONAL_UNIT = "seasonalUnit";

  @Override
  public List<Interval> getAllDataIntervals(long monitoringWindowStartTime,
      long monitoringWindowEndTime) {
    List<Interval> intervals =
        getTrainingDataIntervals(monitoringWindowStartTime, monitoringWindowEndTime);
    Interval currentInterval = new Interval(monitoringWindowStartTime, monitoringWindowEndTime);
    intervals.add(0, currentInterval);
    return intervals;
  }

  @Override
  public List<Interval> getTrainingDataIntervals(long monitoringWindowStartTime,
      long monitoringWindowEndTime) {
    Interval currentInterval = new Interval(monitoringWindowStartTime, monitoringWindowEndTime);
    int baselineCount = Integer.valueOf(getProperties().getProperty("seasonalPeriod"));
    // Gap between each season
    int seasonalSize = Integer.valueOf(getProperties().getProperty("seasonalSize"));
    TimeUnit seasonalUnit = TimeUnit.valueOf(getProperties().getProperty("seasonalUnit"));
    Duration gap = new Duration(seasonalUnit.toMillis(seasonalSize));
    // Compute the baseline intervals
    return getBaselineIntervals(currentInterval, baselineCount, gap);
  }

  /**
   * Given the interval of current time series, returns a list of baseline time series based on the
   * specified baseline count and the gap between each time series.
   *
   * Examples:
   * 1. Week-Over-Week: baselineCount = 1, gap = 7-DAYS
   * 2. Week-Over-2Week: baselineCount = 1, gap = 14-DAYS
   * 3. Week-Over-2Weeks: baselineCount = 2, gap = 7-DAYS
   *
   * @param currentInterval the interval of the current time series
   * @param baselineCount the desired number of baselines
   * @param gap the gap between the end of a time series to the begin of the subsequent time series
   * @return a list of baseline time series
   */
  private List<Interval> getBaselineIntervals(Interval currentInterval, int baselineCount, Duration gap) {
    List<Interval> baselineIntervals = new ArrayList<>();
    long currentStart = currentInterval.getStartMillis();
    long currentEnd = currentInterval.getEndMillis();
    for (int i = 0; i < baselineCount; ++i) {
      long baselineStart = currentStart - gap.getMillis() * (i + 1);
      long baselineEnd = currentEnd - gap.getMillis() * (i + 1);
      Interval baselineInterval = new Interval(baselineStart, baselineEnd);
      baselineIntervals.add(baselineInterval);
    }
    return baselineIntervals;
  }
}
