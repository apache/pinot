package com.linkedin.thirdeye.anomalydetection.model.transform;

import java.util.Properties;

public abstract class AbstractTransformationFunction implements TransformationFunction {
  protected Properties properties;

  @Override
  public void init(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Properties getProperties() {
    return this.properties;
  }
}
