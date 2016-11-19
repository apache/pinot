package com.linkedin.thirdeye.detector.metric.transfer;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import java.util.List;


public class MetricTransfer {

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
    if (scalingFactorList == null || scalingFactorList.size() < 1) {
      return;  // no transformation if there is no scaling factor in
    }
    // Went over the List of Scaling Factors and Rescale the timestamps with in the time range
    // get the metric name
    for (long ts : metricsToModify.getTimeWindowSet()) {
      for (ScalingFactor sf: scalingFactorList) {
        if (sf.getTimeWindow().contains(ts)) {
          //update the metric value
          metricsToModify.set(ts, metricName, metricsToModify.get(ts, metricName).doubleValue() * sf.getScalingFactor());
        }
      }
    }

  }
}
