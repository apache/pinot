package com.linkedin.thirdeye.detector.function;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;

public abstract class BaseAnomalyFunction implements AnomalyFunction {
  private AnomalyFunctionSpec spec;

  @Override
  public void init(AnomalyFunctionSpec spec) throws Exception {
    this.spec = spec;
  }

  @Override
  public AnomalyFunctionSpec getSpec() {
    return spec;
  }

  protected Properties getProperties() throws IOException {
    Properties props = new Properties();
    if (spec.getProperties() != null) {
      String[] tokens = spec.getProperties().split(";");
      for (String token : tokens) {
        props.load(new ByteArrayInputStream(token.getBytes()));
      }
    }
    return props;
  }

  /**
   * Returns unit change from baseline value
   * @param currentValue
   * @param baselineValue
   * @return
   */
  protected double calculateChange(double currentValue, double baselineValue) {
    return (currentValue - baselineValue) / baselineValue;
  }
}
