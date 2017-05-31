package com.linkedin.thirdeye.anomaly.classification.classifier;

import java.util.Collections;
import java.util.Map;

public abstract class BaseAnomalyClassifier implements AnomalyClassifier {
  protected Map<String, String> props = Collections.emptyMap();

  @Override
  public void setParameters(Map<String, String> props) {
    this.props = props;
  }
}
