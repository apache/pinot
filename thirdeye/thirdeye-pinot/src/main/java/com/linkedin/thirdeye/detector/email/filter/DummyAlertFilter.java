package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class DummyAlertFilter extends BaseAlertFilter {
  @Override
  public List<String> getPropertyNames() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void setParameters(Map<String, String> props) {
    // Does nothing
  }

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    return true;
  }

  @Override
  public String toString() {
    return "DummyFilter";
  }
}
