package com.linkedin.thirdeye.anomaly.alert.grouping.filter;

import java.util.Collections;
import java.util.Map;

public abstract class BaseAlertGroupFilter implements AlertGroupFilter {

  Map<String, String> props = Collections.emptyMap();

  @Override
  public void setParameters(Map<String, String> props) {
    this.props = props;
  }
}
