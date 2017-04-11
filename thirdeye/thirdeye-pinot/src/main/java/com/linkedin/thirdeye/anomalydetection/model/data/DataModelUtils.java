package com.linkedin.thirdeye.anomalydetection.model.data;

import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;


public class DataModelUtils {
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
  public static List<Interval> getBaselineIntervals(Interval currentInterval, int baselineCount, Duration gap) {
    List<Interval> baselineIntervals = new ArrayList<>();
    DateTime currentStart = currentInterval.getStart();
    DateTime currentEnd = currentInterval.getEnd();
    for (int i = 0; i < baselineCount; ++i) {
      DateTime baselineStart = currentStart.minus(gap.multipliedBy(i+1).toPeriod());
      DateTime baselineEnd = currentEnd.minus(gap.multipliedBy(i+1).toPeriod());
      Interval baselineInterval = new Interval(baselineStart, baselineEnd);
      baselineIntervals.add(baselineInterval);
    }
    return baselineIntervals;
  }
}
