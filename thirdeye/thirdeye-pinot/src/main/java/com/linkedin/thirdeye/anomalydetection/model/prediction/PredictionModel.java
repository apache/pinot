package com.linkedin.thirdeye.anomalydetection.model.prediction;

import com.linkedin.thirdeye.anomalydetection.data.TimeSeries;
import java.util.List;
import java.util.Properties;

public interface PredictionModel {
  /**
   * Initializes this model with the given properties.
   * @param properties the given properties.
   */
  void init(Properties properties);

  /**
   * Compute parameters for this model using the given time series. The parameters could be a
   * dynamic threshold that is computed using the given baselines.
   *
   * @param historicalTimeSeries the history time series for training this model.
   */
  void train(List<TimeSeries> historicalTimeSeries);
}
