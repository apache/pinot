package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.joda.time.Interval;

public class BackwardAnomalyFunctionUtils {

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
    Collections.sort(timeSeriesList, new TimeSeriesStartTimeComparator().reversed());

    // If a timestamp is contained in the interval of a time series, then the timestamp and its
    // value is inserted into that time series. If the intervals of time series have overlap, then
    // the timestamp and its value could be inserted to multiple time series.
    for (long timestamp : metricTimeSeries.getTimeWindowSet()) {
      for (TimeSeries timeSeries : timeSeriesList) {
        if (timeSeries.getTimeSeriesInterval().contains(timestamp)) {
          double value = metricTimeSeries.get(timestamp, metricName).doubleValue();
          timeSeries.set(timestamp, value);
        }
      }
    }
    return timeSeriesList;
  }

  /**
   * Compares Time Series by the start time of their interval.
   */
  private static class TimeSeriesStartTimeComparator implements Comparator<TimeSeries> {
    @Override public int compare(TimeSeries ts1, TimeSeries ts2) {
      long startTime1 = ts1.getTimeSeriesInterval().getStartMillis();
      long startTime2 = ts2.getTimeSeriesInterval().getStartMillis();
      return (int) (startTime1 - startTime2);
    }
  }
}
