package com.linkedin.thirdeye.anomalydetection.model.prediction;

import com.linkedin.thirdeye.anomalydetection.data.TimeSeries;
import java.util.List;
import org.joda.time.Interval;

public interface PredictionModel {
  /**
   * Given the interval of the observed (current) time series, returns the intervals of time series
   * that are used by this prediction model for training purpose.
   *
   * @param monitoringWindowStartTime inclusive
   * @param monitoringWindowEndTime exclusive
   *
   * @return intervals of time series that are used for training.
   */
  List<Interval> getTrainingTimeSeriesIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime);

  /**
   * Compute parameters for this model using the given time series. The parameters could be a
   * dynamic threshold that is computed using the given baselines.
   *
   * Note: The input time series could be a set of time series that is different from or the same
   * as the baseline time series. For instance, the training data could be the time series in the
   * past 8 weeks and the baselines are the time series in the past 4 weeks.
   *
   * @param timeSeries the history time series for training this model.
   */
  void train(List<TimeSeries> timeSeries);

  /**
   * Returns the expected time series that is computed using the given baselines and the trained
   * parameters.
   *
   * @return the expected time series that is computed using the given baselines.
   */
  TimeSeries getExpectedTimeSeries(List<TimeSeries> baselines);
}
