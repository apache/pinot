package com.linkedin.thirdeye.anomalydetection.model.prediction;

import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;

public abstract class ExpectedTimeSeriesPredictionModel extends AbstractPredictionModel {
  /**
   * Returns the expected (predicted) time series from the prediction model.
   *
   * @return the expected (predicted) time series of this prediction model.
   */
  public abstract TimeSeries getExpectedTimeSeries();
}
