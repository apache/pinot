package com.linkedin.thirdeye.anomalydetection.model.merge;

import java.util.Properties;

public abstract class AbstractMergeModel implements MergeModel {
  protected Properties properties;

  protected double weight = 0d;

  protected double score = 0d;

  @Override
  public void init(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Properties getProperties() {
    return this.properties;
  }

  @Override
  public double getWeight() {
    return weight;
  }

  @Override
  public double getScore() {
    return score;
  }
}
