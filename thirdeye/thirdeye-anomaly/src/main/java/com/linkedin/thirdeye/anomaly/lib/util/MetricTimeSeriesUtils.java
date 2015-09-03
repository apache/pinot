package com.linkedin.thirdeye.anomaly.lib.util;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.MetricTimeSeries;

/**
 *
 */
public class MetricTimeSeriesUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricTimeSeriesUtils.class);

  /**
   * @param series
   *  The MetricTimeSeries
   * @param metric
   *  The metric to extract
   * @param bucketWidthInMillis
   *  The width of the bucket in milliseconds
   * @param missingTimeStamps
   *  If this is not null, then any missing timestamps will be added to this set
   * @param defaultValue
   *  The default value to use if the data is missing
   * @return
   *  A pair containing the timestamps and values
   */
  public static Pair<long[], double[]> toArray(MetricTimeSeries series, String metric, long bucketWidthInMillis,
      Set<Long> missingTimeStamps, double defaultValue) {
    long startTime = Collections.min(series.getTimeWindowSet());
    long endTime = Collections.max(series.getTimeWindowSet());

    int numValues = (int) (1 + ((endTime - startTime) / bucketWidthInMillis));
    double[] values = new double[numValues];
    long[] timestamps = new long[numValues];

    for (int i = 0; i < numValues; i++) {
      long timeWindow = startTime + (i * bucketWidthInMillis);
      timestamps[i] = timeWindow;
      if (series.getTimeWindowSet().contains(timeWindow)) {
        values[i] = series.get(timeWindow, metric).doubleValue();
      } else {
        values[i] = defaultValue;
        if (missingTimeStamps != null) {
          missingTimeStamps.add(timeWindow); // do not use these datapoints
        }
      }
    }

    if (numValues != series.getTimeWindowSet().size()) {
      LOGGER.warn("looks like there are holes in the data: expected {} timestamps, found {}", numValues,
          series.getTimeWindowSet().size());
    }

    return new Pair<>(timestamps, values);
  }

}
