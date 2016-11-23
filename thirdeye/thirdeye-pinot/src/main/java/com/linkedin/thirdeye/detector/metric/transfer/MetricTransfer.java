package com.linkedin.thirdeye.detector.metric.transfer;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricTransfer {
  private static final Logger LOG = LoggerFactory.getLogger(MetricTransfer.class);
  private static final boolean DEBUG = true;

  /**
   * Use the scaling factor to normalize the input time series
   *
   * @param metricsToModify the data MetricTimeSeries to be modified by scaling factor
   * @param metricName the metricName withing the timeseries to be modified
   * @param scalingFactorList list of scaling factors
   * @return
   */
  public static void rescaleMetric(MetricTimeSeries metricsToModify, String metricName,
      List<ScalingFactor> scalingFactorList) {
    if (CollectionUtils.isEmpty(scalingFactorList)) {
      return;  // no transformation if there is no scaling factor in
    }

    List<ScaledValue> scaledValues = null;
    if (DEBUG) {
      scaledValues = new ArrayList<>();
    }

    // Went over the List of Scaling Factors and Rescale the timestamps with in the time range
    // get the metric name
    for (long ts : metricsToModify.getTimeWindowSet()) {
      for (ScalingFactor sf: scalingFactorList) {
        if (sf.isInTimeWindow(ts)) {
          double originalValue = metricsToModify.get(ts, metricName).doubleValue();
          double scaledValue = originalValue * sf.getScalingFactor();
          metricsToModify.set(ts, metricName, scaledValue);
          if (DEBUG) {
            scaledValues.add(new ScaledValue(ts, originalValue, scaledValue));
          }
        }
      }
    }
    if (DEBUG) {
      if (CollectionUtils.isNotEmpty(scaledValues)) {
        Collections.sort(scaledValues);
        StringBuilder sb = new StringBuilder();
        String separator = "";
        for (ScaledValue scaledValue : scaledValues) {
          sb.append(separator).append(scaledValue.toString());
          separator = ", ";
        }
        LOG.info("Transformed values: {}", sb.toString());
      }
    }
  }

  /**
   * This class is used to store debugging information, which is used to show the status of the
   * transformed time series.
   */
  private static class ScaledValue implements Comparable<ScaledValue> {
    long timestamp;
    double originalValue;
    double scaledValue;

    public ScaledValue(long timestamp, double originalValue, double scaledValue) {
      this.timestamp = timestamp;
      this.originalValue = originalValue;
      this.scaledValue = scaledValue;
    }

    /**
     * Used to sort ScaledValue by the natural order of their timestamp
     */
    @Override
    public int compareTo(ScaledValue o) {
      return Long.compare(timestamp, o.timestamp);
    }

    @Override
    public String toString() {
      return "ScaledValue{" + "timestamp=" + timestamp + ", originalValue=" + originalValue
          + ", scaledValue=" + scaledValue + ", scale=" + scaledValue / originalValue + '}';
    }
  }
}
