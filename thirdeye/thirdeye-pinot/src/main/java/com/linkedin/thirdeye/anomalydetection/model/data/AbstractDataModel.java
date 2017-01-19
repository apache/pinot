package com.linkedin.thirdeye.anomalydetection.model.data;

import java.util.Properties;

public abstract class AbstractDataModel implements DataModel {
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
