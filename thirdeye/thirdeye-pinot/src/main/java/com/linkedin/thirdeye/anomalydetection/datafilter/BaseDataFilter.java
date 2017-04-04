package com.linkedin.thirdeye.anomalydetection.datafilter;

import java.util.Collections;
import java.util.Map;

abstract class BaseDataFilter implements DataFilter {
  protected Map<String, String> props = Collections.emptyMap();

  @Override
  public void setParameters(Map<String, String> props) {
    this.props = props;
  }
}
