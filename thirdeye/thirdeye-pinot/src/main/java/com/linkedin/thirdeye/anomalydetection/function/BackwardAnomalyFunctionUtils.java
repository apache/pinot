package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomalydetection.AnomalyDetectionUtils;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeriesKey;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.Interval;

public class BackwardAnomalyFunctionUtils {
  private static final Double NULL_DOUBLE = Double.NaN;

  public static List<Pair<Long, Long>> toBackwardCompatibleDataRanges(
      List<Interval> timeSeriesIntervals) {
    List<Pair<Long, Long>> dataRanges = new ArrayList<>();
    for (Interval interval : timeSeriesIntervals) {
      Pair<Long, Long> dataRange = new Pair<>(interval.getStartMillis(), interval.getEndMillis());
      dataRanges.add(dataRange);
    }
    return dataRanges;
  }

  /**
   * Splits a MetricTimeSeries to current (observed) time series and baselines. The list of time
   * series is sorted by the start time of their interval in the reversed natural order. Therefore,
   * the current time series is located at the beginning of the returned list.
   *
   * @param metricTimeSeries the metric time series that contains current and baseline time series.
   * @param metricName the metric name to retrieve the value from the given metric time series.
   * @param timeSeriesIntervals the intervals of the split time series.
   * @return a list of time series, which are split from the metric time series.
   */
  public static List<TimeSeries> splitSetsOfTimeSeries(MetricTimeSeries metricTimeSeries,
      String metricName, List<Interval> timeSeriesIntervals) {
    List<TimeSeries> timeSeriesList = new ArrayList<>(timeSeriesIntervals.size());
    for (Interval interval : timeSeriesIntervals) {
      TimeSeries timeSeries = new TimeSeries();
      timeSeries.setTimeSeriesInterval(interval);
      timeSeriesList.add(timeSeries);
    }
    // Sort time series by their start time in reversed natural order, i.e., the latest time series
    // is arranged in the front of the list
    Collections.sort(timeSeriesList, Collections.reverseOrder(new TimeSeriesStartTimeComparator()));

    // If a timestamp is contained in the interval of a time series, then the timestamp and its
    // value is inserted into that time series. If the intervals of time series have overlap, then
    // the timestamp and its value could be inserted to multiple time series.
    for (long timestamp : metricTimeSeries.getTimeWindowSet()) {
      for (TimeSeries timeSeries : timeSeriesList) {
        if (timeSeries.getTimeSeriesInterval().contains(timestamp)) {
          double value = metricTimeSeries.getOrDefault(timestamp, metricName, NULL_DOUBLE).doubleValue();
          if (Double.compare(value, NULL_DOUBLE) != 0) {
            timeSeries.set(timestamp, value);
          }
        }
      }
    }
    return timeSeriesList;
  }

  /**
   * Returns an anomaly detection context from the given information.
   *
   * @param anomalyFunction the anomaly function for anomaly detection.
   * @param timeSeries the given time series.
   * @param metric the metric name of the given time series.
   * @param exploredDimensions the dimension map of the given time series.
   * @param windowStart the start of the interval of the time series.
   * @param windowEnd the end of the interval of the time series.
   * @param knownAnomalies the list of historical merged anomalies.
   *
   * @return an anomaly detection context from the given information.
   */
  public static AnomalyDetectionContext buildAnomalyDetectionContext(
      AnomalyDetectionFunction anomalyFunction, MetricTimeSeries timeSeries, String metric,
      DimensionMap exploredDimensions, int bucketSize, TimeUnit bucketUnit, DateTime windowStart,
      DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies) {
    // Create the anomaly detection context for the new modularized anomaly function
    AnomalyDetectionContext anomalyDetectionContext = new AnomalyDetectionContext();
    anomalyDetectionContext
        .setBucketSizeInMS(AnomalyDetectionUtils.getBucketInMillis(bucketSize, bucketUnit));
    anomalyDetectionContext.setAnomalyDetectionFunction(anomalyFunction);

    // Construct TimeSeriesKey
    TimeSeriesKey timeSeriesKey = new TimeSeriesKey();
    timeSeriesKey.setDimensionMap(exploredDimensions);
    timeSeriesKey.setMetricName(metric);
    anomalyDetectionContext.setTimeSeriesKey(timeSeriesKey);

    // set historical anomalies
    anomalyDetectionContext.setHistoricalAnomalies(knownAnomalies);

    // Split time series to observed time series and baselines for each metric
    for (String metricName : anomalyFunction.getSpec().getMetrics()) {
      List<Interval> intervals =
          anomalyFunction.getTimeSeriesIntervals(windowStart.getMillis(), windowEnd.getMillis());
      List<TimeSeries> timeSeriesList =
          BackwardAnomalyFunctionUtils.splitSetsOfTimeSeries(timeSeries, metricName, intervals);
      anomalyDetectionContext.setCurrent(metricName, timeSeriesList.get(0));
      timeSeriesList.remove(0);
      anomalyDetectionContext.setBaselines(metricName, timeSeriesList);
    }


    return anomalyDetectionContext;
  }

  /**
   * Compares Time Series by the start time of their interval.
   */
  private static class TimeSeriesStartTimeComparator implements Comparator<TimeSeries> {
    @Override public int compare(TimeSeries ts1, TimeSeries ts2) {
      long startTime1 = ts1.getTimeSeriesInterval().getStartMillis();
      long startTime2 = ts2.getTimeSeriesInterval().getStartMillis();
      return Long.compare(startTime1, startTime2);
    }
  }
}
