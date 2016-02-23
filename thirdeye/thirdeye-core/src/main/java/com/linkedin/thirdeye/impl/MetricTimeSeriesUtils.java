package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricTimeSeriesUtils {
  public static MetricTimeSeries convertTimeToMillis(MetricTimeSeries original, int size,
      TimeUnit unit) {
    MetricTimeSeries timeSeries = new MetricTimeSeries(original.getSchema());

    for (Long time : original.getTimeWindowSet()) {
      long millis = TimeUnit.MILLISECONDS.convert(time, unit) * size;

      for (int i = 0; i < original.getSchema().getNumMetrics(); i++) {
        String metricName = original.getSchema().getMetricName(i);
        timeSeries.set(millis, metricName, original.get(time, metricName));
      }
    }

    return timeSeries;
  }

  /**
   * @param original
   *          Some metric time series
   * @return
   *         A metric time series where each metric is normalized to its first non-zero value
   */
  public static MetricTimeSeries normalize(MetricTimeSeries original) {
    MetricType[] metricTypes = new MetricType[original.getSchema().getNumMetrics()];
    Arrays.fill(metricTypes, MetricType.DOUBLE);
    MetricTimeSeries normalized = new MetricTimeSeries(
        new MetricSchema(original.getSchema().getNames(), Arrays.asList(metricTypes)));

    // Find first non-zero value for all metrics
    Number[] firstNonZero = new Number[original.getSchema().getNumMetrics()];
    Arrays.fill(firstNonZero, 0);
    List<Long> sortedTimes = new ArrayList<Long>(original.getTimeWindowSet());
    Collections.sort(sortedTimes);
    for (int i = 0; i < original.getSchema().getNumMetrics(); i++) {
      String metricName = original.getSchema().getMetricName(i);

      for (Long time : sortedTimes) {
        Number value = original.get(time, metricName);

        if (value != null && value.doubleValue() > 0) {
          firstNonZero[i] = value;
          break;
        }
      }
    }

    // Normalize to those values
    for (Long time : sortedTimes) {
      for (int i = 0; i < original.getSchema().getNumMetrics(); i++) {
        String metricName = original.getSchema().getMetricName(i);
        MetricType metricType = original.getSchema().getMetricType(i);
        Number value = original.get(time, metricName);

        if (NumberUtils.isZero(firstNonZero[i], metricType)) {
          normalized.set(time, metricName, 0);
        } else {
          normalized.set(time, metricName, value.doubleValue() / firstNonZero[i].doubleValue());
        }
      }
    }

    return normalized;
  }

  /**
   * @param original
   *          Some metric time series
   * @param baselineMetricName
   *          The name of the metric we should normalize all metrics to
   * @return
   *         A metric time series whose values are all normalized to one metric's values
   */
  public static MetricTimeSeries normalize(MetricTimeSeries original, String baselineMetricName) {
    MetricType[] metricTypes = new MetricType[original.getSchema().getNumMetrics()];
    Arrays.fill(metricTypes, MetricType.DOUBLE);
    MetricTimeSeries normalized = new MetricTimeSeries(
        new MetricSchema(original.getSchema().getNames(), Arrays.asList(metricTypes)));

    MetricType baselineMetricType = original.getSchema().getMetricType(baselineMetricName);

    List<Long> sortedTimes = new ArrayList<Long>(original.getTimeWindowSet());

    Collections.sort(sortedTimes);

    for (Long time : sortedTimes) {
      for (int i = 0; i < original.getSchema().getNumMetrics(); i++) {
        String metricName = original.getSchema().getMetricName(i);
        Number value = original.get(time, metricName);
        Number baseline = original.get(time, baselineMetricName);

        if (NumberUtils.isZero(baseline, baselineMetricType)) {
          normalized.set(time, metricName, 0);
        } else {
          normalized.set(time, metricName, value.doubleValue() / baseline.doubleValue());
        }
      }
    }

    return normalized;
  }

  /**
   * @param original
   *          A metric time series with some granularity
   * @param timeWindow
   *          A coarser granularity
   * @return
   *         A new metric time series with aggregates at the new coarser granularity
   */
  public static MetricTimeSeries aggregate(MetricTimeSeries original, long timeWindow, long limit) {
    MetricTimeSeries aggregated = new MetricTimeSeries(original.getSchema());

    for (Long time : original.getTimeWindowSet()) {
      long bucket = (time / timeWindow) * timeWindow;

      for (int i = 0; i < original.getSchema().getNumMetrics(); i++) {
        String metricName = original.getSchema().getMetricName(i);
        Number metricValue = original.get(time, metricName);

        if (metricValue != null && bucket < limit) {
          aggregated.increment(bucket, metricName, metricValue);
        }
      }
    }

    return aggregated;
  }

  /**
   * @param original
   *          The metric time series with values starting at (startTime - timeWindow)
   * @param startTime
   *          The first time in the result moving average (time series must contain startTime -
   *          timeWindow)
   * @param movingAverageWindow
   *          The interval over which the average should be computed
   * @return
   *         A new metric time series from (startTime, max(original.getTimeWindowSet()))
   */
  public static MetricTimeSeries getSimpleMovingAverage(MetricTimeSeries original, long startTime,
      long endTime, long movingAverageWindow) {
    if (movingAverageWindow < 1) {
      throw new IllegalArgumentException("Must provide non-zero positive value for time window");
    }

    // Bootstrap the moving average with (startTime - timeWindow, startTime)
    double[] sums = new double[original.getSchema().getNumMetrics()];
    for (long time = startTime - movingAverageWindow; time < startTime; time++) {
      for (int i = 0; i < original.getSchema().getNumMetrics(); i++) {
        Number value = original.get(time, original.getSchema().getMetricName(i));
        if (value != null) {
          sums[i] += value.doubleValue();
        }
      }
    }

    // Compute moving average
    MetricType[] metricTypes = new MetricType[original.getSchema().getNumMetrics()];
    Arrays.fill(metricTypes, MetricType.DOUBLE);
    MetricTimeSeries timeSeries = new MetricTimeSeries(
        new MetricSchema(original.getSchema().getNames(), Arrays.asList(metricTypes)));
    for (long time = startTime; time <= endTime; time++) {
      for (int i = 0; i < original.getSchema().getNumMetrics(); i++) {
        Number baseline =
            original.get(time - movingAverageWindow, original.getSchema().getMetricName(i));
        if (baseline != null) {
          sums[i] -= baseline.doubleValue();
        }

        Number current = original.get(time, original.getSchema().getMetricName(i));
        if (current != null) {
          sums[i] += current.doubleValue();
        }

        timeSeries.set(time, original.getSchema().getMetricName(i), sums[i] / movingAverageWindow);
      }
    }

    return timeSeries;
  }
}
