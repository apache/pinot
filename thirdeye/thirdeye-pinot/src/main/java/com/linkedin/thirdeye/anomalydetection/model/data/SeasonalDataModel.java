package com.linkedin.thirdeye.anomalydetection.model.data;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

public class SeasonalDataModel extends AbstractDataModel {
  public static final String SEASONAL_PERIOD = "seasonalPeriod";
  public static final String SEASONAL_SIZE = "seasonalSize";
  public static final String SEASONAL_UNIT = "seasonalUnit";
  public static final String TIMEZONE = "timezone";
  public static final String DEFAULT_TIMEZONE = "America/Los_Angeles";

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
    DateTimeZone timezone = DateTimeZone.forID(getProperties().getProperty(TIMEZONE, DEFAULT_TIMEZONE));
    Interval currentInterval = new Interval(monitoringWindowStartTime, monitoringWindowEndTime, timezone);
    int baselineCount = Integer.valueOf(getProperties().getProperty("seasonalPeriod"));
    // Gap between each season
    int seasonalSize = Integer.valueOf(getProperties().getProperty("seasonalSize"));
    TimeUnit seasonalUnit = TimeUnit.valueOf(getProperties().getProperty("seasonalUnit"));
    Duration gap = new Duration(seasonalUnit.toMillis(seasonalSize));
    // Compute the baseline intervals
    return DataModelUtils.getBaselineIntervals(currentInterval, baselineCount, gap);
  }
}
