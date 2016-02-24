package com.linkedin.thirdeye.function;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;

/**
 * See params for property configuration
 * @param baseline baseline comparison period. Value should be one of 'w/w', 'w/2w',
 *          'w/3w'. The anomaly function spec should also be configured to provide a sufficient
 *          window size.
 * @param changeThreshold detection threshold for percent change relative to baseline, defined as
 *          (current - baseline) / baseline. Positive values detect an increase, while negative
 *          values detect a decrease. This value should be a decimal, eg a 50% increase would have a
 *          threshold of 0.50.
 * @param averageVolumeThreshold minimum average threshold across the entire input window. If the
 *          average value does not meet the
 *          threshold, no anomaly results will be generated. This value should be a double.
 */
public class UserRuleAnomalyFunction extends BaseAnomalyFunction {
  public static String BASELINE = "baseline";
  public static String CHANGE_THRESHOLD = "changeThreshold";
  public static String AVERAGE_VOLUME_THRESHOLD = "averageVolumeThreshold";
  private static final Joiner CSV = Joiner.on(",");

  @Override
  public List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<AnomalyResult> knownAnomalies)
          throws Exception {
    List<AnomalyResult> anomalyResults = new ArrayList<>();

    // Parse function properties
    Properties props = getProperties();

    // Metric
    String metric = getSpec().getMetric();

    // Get thresholds
    double changeThreshold = Double.valueOf(props.getProperty(CHANGE_THRESHOLD));
    double volumeThreshold = 0;
    if (props.containsKey(AVERAGE_VOLUME_THRESHOLD)) {
      volumeThreshold = Double.valueOf(props.getProperty(AVERAGE_VOLUME_THRESHOLD));
    }

    // Compute baseline for comparison
    // TODO: Expand options here and use regex to extract numeric parameter
    long baselineMillis;
    String baselineProp = props.getProperty(BASELINE);
    if ("w/w".equals(baselineProp)) {
      baselineMillis = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
    } else if ("w/2w".equals(baselineProp)) {
      baselineMillis = TimeUnit.MILLISECONDS.convert(14, TimeUnit.DAYS);
    } else if ("w/3w".equals(baselineProp)) {
      baselineMillis = TimeUnit.MILLISECONDS.convert(21, TimeUnit.DAYS);
    } else {
      throw new IllegalArgumentException("Unsupported baseline " + baselineProp);
    }

    // Compute the bucket size, so we can iterate in those steps
    long bucketMillis =
        TimeUnit.MILLISECONDS.convert(getSpec().getBucketSize(), getSpec().getBucketUnit());

    // Compute the weight of this time series (average across whole)
    double averageValue = 0;
    for (Long time : timeSeries.getTimeWindowSet()) {
      averageValue += timeSeries.get(time, metric).doubleValue();
    }
    long numBuckets = (windowEnd.getMillis() - windowStart.getMillis()) / bucketMillis;
    averageValue /= numBuckets;

    // Check if this time series even meets our volume threshold
    if (averageValue < volumeThreshold) {
      return anomalyResults; // empty list
    }

    // Start at current time and iterate backwards until baseline is window start
    DateTime current = windowEnd;
    DateTime baseline = current.minus(baselineMillis);
    while (baseline.compareTo(windowStart) >= 0) {
      double currentValue = timeSeries.get(current.getMillis(), metric).doubleValue();
      double baselineValue = timeSeries.get(baseline.getMillis(), metric).doubleValue();

      if (baselineValue > 0) {
        double percentChange = (currentValue - baselineValue) / baselineValue;

        if (changeThreshold > 0 && percentChange > changeThreshold
            || changeThreshold < 0 && percentChange < changeThreshold) {
          AnomalyResult anomalyResult = new AnomalyResult();
          anomalyResult.setCollection(getSpec().getCollection());
          anomalyResult.setMetric(metric);
          anomalyResult.setDimensions(CSV.join(dimensionKey.getDimensionValues()));
          anomalyResult.setFunctionId(getSpec().getId());
          anomalyResult.setProperties(getSpec().getProperties());
          anomalyResult.setStartTimeUtc(current.getMillis());
          anomalyResult.setEndTimeUtc(null); // point-in-time
          anomalyResult.setScore(percentChange);
          anomalyResult.setWeight(averageValue);
          anomalyResults.add(anomalyResult);
        }
      }

      current = current.minus(bucketMillis);
      baseline = baseline.minus(bucketMillis);
    }

    return anomalyResults;
  }
}
