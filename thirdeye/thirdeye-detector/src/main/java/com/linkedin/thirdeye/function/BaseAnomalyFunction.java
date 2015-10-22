package com.linkedin.thirdeye.function;

import com.linkedin.thirdeye.api.AnomalyFunctionSpec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

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
}
