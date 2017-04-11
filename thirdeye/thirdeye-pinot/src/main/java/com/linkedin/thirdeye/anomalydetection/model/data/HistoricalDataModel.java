package com.linkedin.thirdeye.anomalydetection.model.data;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;


public class HistoricalDataModel extends AbstractDataModel {
  public static final String HISTORICAL_DATA_LENGTH = "historicalDataLength";
  public static final String BUCKET_LENGTH = "bucketSize";
  public static final String BUCKET_UNIT = "bucketUnit";
  public static final String TIMEZONE = "timezone";
  public static final String DEFAULT_TIMEZONE = "America/Los_Angeles";

  @Override
  public List<Interval> getAllDataIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime) {
    List<Interval> intervals =
        getTrainingDataIntervals(monitoringWindowStartTime, monitoringWindowEndTime);
    Interval currentInterval = new Interval(monitoringWindowStartTime, monitoringWindowEndTime);
    intervals.add(0, currentInterval);
    return intervals;
  }

  @Override
  public List<Interval> getTrainingDataIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime) {
    DateTimeZone timezone = DateTimeZone.forID(getProperties().getProperty(TIMEZONE, DEFAULT_TIMEZONE));
    Interval currentInterval = new Interval(monitoringWindowStartTime, monitoringWindowEndTime, timezone);
    int historicalDataLength = Integer.valueOf(getProperties().getProperty(HISTORICAL_DATA_LENGTH));
    int bucketSize = Integer.valueOf(getProperties().getProperty(BUCKET_LENGTH));
    TimeUnit bucketUnit = TimeUnit.valueOf(getProperties().getProperty(BUCKET_UNIT));
    Duration gap = new Duration(bucketUnit.toMillis(bucketSize));
    // Compute the baseline intervals
    return DataModelUtils.getBaselineIntervals(currentInterval, historicalDataLength, gap);
  }
}
