package com.linkedin.thirdeye.anomalydetection.model.prediction;

import com.linkedin.thirdeye.anomalydetection.data.TimeSeries;

public abstract class ExpectedTimeSeriesPredictionModel extends AbstractPredictionModel {
  /**
   * Returns the expected (predicted) time series from the prediction model.
   *
   * @return the expected (predicted) time series of this prediction model.
   */
  abstract TimeSeries getExpectedTimeSeries();
}
