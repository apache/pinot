package com.linkedin.thirdeye.anomalydetection.model.transform;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import org.joda.time.Interval;

public class ZeroRemovalFunction extends AbstractTransformationFunction {
  /**
   * Removes value 0.0 from the time series. The reason to apply this transformation function is
   * that ThirdEye currently returns empty values as 0.0. Therefore, we need to remove those values.
   *
   * @param timeSeries the time series that provides the data points to be transformed.
   * @param anomalyDetectionContext the anomaly detection context that could provide additional
   *                                information for the transformation.
   * @return a time series that have value 0.0 removed.
   */
  @Override public TimeSeries transform(TimeSeries timeSeries,
      AnomalyDetectionContext anomalyDetectionContext) {

    TimeSeries transformedTimeSeries = new TimeSeries();

    Interval timeSeriesInterval = timeSeries.getTimeSeriesInterval();
    transformedTimeSeries.setTimeSeriesInterval(timeSeriesInterval);

    for (long timestamp : timeSeries.timestampSet()) {
      double value = timeSeries.get(timestamp);
      if (value != 0d && timeSeriesInterval.contains(timestamp)) {
        transformedTimeSeries.set(timestamp, value);
      }
    }

    return transformedTimeSeries;
  }
}
