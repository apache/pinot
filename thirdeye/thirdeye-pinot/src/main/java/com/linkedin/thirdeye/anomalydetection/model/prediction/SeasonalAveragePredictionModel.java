package com.linkedin.thirdeye.anomalydetection.model.prediction;

import com.linkedin.thirdeye.anomalydetection.Utils;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.joda.time.Interval;

public class SeasonalAveragePredictionModel extends ExpectedTimeSeriesPredictionModel {
  public static final String BUCKET_SIZE = "bucketSize";
  public static final String BUCKET_UNIT = "bucketUnit";

  TimeSeries expectedTimeSeries;

  @Override
  public void train(List<TimeSeries> baselineTimeSeries) {
    expectedTimeSeries = new TimeSeries();

    if (CollectionUtils.isNotEmpty(baselineTimeSeries)) {
      TimeSeries baseTimeSeries = getLatestTimeSeries(baselineTimeSeries);
      Interval baseInterval = baseTimeSeries.getTimeSeriesInterval();

      long bucketSizeInMillis = Utils.getBucketInMillis(BUCKET_SIZE, BUCKET_UNIT, getProperties());

      long baseStart = baseInterval.getStartMillis();
      long baseEnd = baseInterval.getEndMillis();
      int bucketCount = (int) ((baseEnd - baseStart) / bucketSizeInMillis);
      expectedTimeSeries.setTimeSeriesInterval(baseInterval);

      if (baselineTimeSeries.size() > 1) {
        for (int i = 0; i < bucketCount; ++i) {
          double sum = 0d;
          int count = 0;
          long timeOffset = i * bucketSizeInMillis;
          for (TimeSeries ts : baselineTimeSeries) {
            long timestamp = ts.getTimeSeriesInterval().getStartMillis() + timeOffset;
            Double value = ts.get(timestamp);
            if (value != null) {
              sum += value;
              ++count;
            }
          }
          if (count != 0) {
            long timestamp = baseStart + timeOffset;
            double avgValue = sum / (double) count;
            expectedTimeSeries.set(timestamp, avgValue);
          }
        }
      } else {
        for (int i = 0; i < bucketCount; ++i) {
          long timestamp = baseStart + i * bucketSizeInMillis;
          expectedTimeSeries.set(timestamp, baseTimeSeries.get(timestamp));
        }
      }
    }
  }

  @Override
  public TimeSeries getExpectedTimeSeries() {
    return expectedTimeSeries;
  }

  /**
   * Returns the time series that has the largest start millis.
   *
   * @param baselineTimeSeries the set of baselines
   * @return the time series that has the largest start millis.
   */
  private TimeSeries getLatestTimeSeries(List<TimeSeries> baselineTimeSeries) {
    if (CollectionUtils.isNotEmpty(baselineTimeSeries)) {
      if (baselineTimeSeries.size() > 1) {
        TimeSeries latestTimeSeries = baselineTimeSeries.get(0);
        Interval latestInterval = latestTimeSeries.getTimeSeriesInterval();
        for (TimeSeries ts : baselineTimeSeries) {
          Interval currentInterval = ts.getTimeSeriesInterval();
          if (latestInterval.getStartMillis() < currentInterval.getStartMillis()) {
            latestTimeSeries = ts;
            latestInterval = currentInterval;
          }
        }
        return latestTimeSeries;
      } else {
        return baselineTimeSeries.get(0);
      }
    } else {
      return null;
    }
  }
}
