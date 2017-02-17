package com.linkedin.thirdeye.anomalydetection.model.transform;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import org.apache.commons.lang3.StringUtils;

public class TotalCountThresholdRemovalFunction extends AbstractTransformationFunction {
  public static final String TOTAL_COUNT_METRIC_NAME = "totalCountName";
  public static final String TOTAL_COUNT_THRESHOLD = "totalCountThreshold";

  /**
   * Returns an empty time series if the sum of the total count metric does not exceed the
   * threshold.
   *
   * @param timeSeries              the time series that provides the data points to be
   *                                transformed.
   * @param anomalyDetectionContext the anomaly detection context that could provide additional
   *                                information for the transformation. Specifically, the time
   *                                series that provide the values to compute the total count. Note
   *                                that this function simply sum up the values of the specified
   *                                time series and hence the timestamp of that time series does not
   *                                matter. Moreover, this metric has to be put in the set of
   *                                current time series.
   *
   * @return the original time series the sum of the values in total count time series exceeds the
   * threshold.
   */
  @Override public TimeSeries transform(TimeSeries timeSeries,
      AnomalyDetectionContext anomalyDetectionContext) {
    String totalCountMetricName = getProperties().getProperty(TOTAL_COUNT_METRIC_NAME);
    if (StringUtils.isBlank(totalCountMetricName)) {
      return timeSeries;
    }

    double totalCountThreshold =
        Double.valueOf(getProperties().getProperty(TOTAL_COUNT_THRESHOLD, "0"));

    TimeSeries totalCountTS = anomalyDetectionContext.getCurrent(totalCountMetricName);
    double sum = 0d;
    for (long timestamp : totalCountTS.timestampSet()) {
      sum += totalCountTS.get(timestamp);
    }
    if (Double.compare(sum, totalCountThreshold) < 0) {
      TimeSeries emptyTimeSeries = new TimeSeries();
      emptyTimeSeries.setTimeSeriesInterval(timeSeries.getTimeSeriesInterval());
      return emptyTimeSeries;
    } else {
      return timeSeries;
    }
  }
}
