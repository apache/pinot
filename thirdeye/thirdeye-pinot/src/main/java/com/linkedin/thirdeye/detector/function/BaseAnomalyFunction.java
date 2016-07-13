package com.linkedin.thirdeye.detector.function;

import static com.linkedin.thirdeye.client.MetricFunction.Function;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;

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

  @Override
  public Function getFunction() {
    // default
    return Function.SUM;
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
}
