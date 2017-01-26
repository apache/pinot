package com.linkedin.thirdeye.anomalydetection.model.prediction;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import java.util.List;
import java.util.Properties;

public interface PredictionModel {
  /**
   * Initializes this model with the given properties.
   * @param properties the given properties.
   */
  void init(Properties properties);

  /**
   * Returns the properties of this model.
   */
  Properties getProperties();

  /**
   * Compute parameters for this model using the given time series. The parameters could be a
   * dynamic threshold that is computed using the given baselines.
   *
   * @param historicalTimeSeries the history time series for training this model.
   * @param anomalyDetectionContext
   */
  void train(List<TimeSeries> historicalTimeSeries, AnomalyDetectionContext anomalyDetectionContext);
}
