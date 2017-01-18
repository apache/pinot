package com.linkedin.thirdeye.anomalydetection.model.prediction;

import java.util.Properties;

public abstract class AbstractPredictionModel implements PredictionModel{
  protected Properties properties;

  @Override
  public void init(Properties properties) {
    this.properties = properties;
  }
}
