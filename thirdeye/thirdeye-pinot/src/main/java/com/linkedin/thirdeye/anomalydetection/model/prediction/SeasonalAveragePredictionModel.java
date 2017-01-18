package com.linkedin.thirdeye.anomalydetection.model.prediction;

import com.linkedin.thirdeye.anomalydetection.data.TimeSeries;
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
      int bucketSize = Integer.valueOf(this.properties.getProperty(BUCKET_SIZE));
      TimeUnit bucketUnit = TimeUnit.valueOf(this.properties.getProperty(BUCKET_UNIT));
      long bucketSizeInMillis = bucketUnit.toMillis(bucketSize);

      TimeSeries baseTimeSeries = baselineTimeSeries.get(0);
      Interval baseInteval = baseTimeSeries.getTimeSeriesInterval();
      long baseStart = baseInteval.getStartMillis();
      long baseEnd = baseInteval.getEndMillis();
      int bucketCount = (int) ((baseEnd - baseStart) / bucketSizeInMillis);
      expectedTimeSeries.setTimeSeriesInterval(baseInteval);

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
          double avgValue = sum / count;
          expectedTimeSeries.set(timestamp, avgValue);
        }
      }
    }
  }

  @Override
  public TimeSeries getExpectedTimeSeries() {
    return null;
  }
}
