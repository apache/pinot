package com.linkedin.thirdeye.anomaly.alert.grouping.filter;

import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import java.util.Map;

public class DummyAlertGroupFilter extends BaseAlertGroupFilter {
  @Override
  public void setParameters(Map<String, String> props) {
  }

  @Override
  public boolean isQualified(GroupedAnomalyResultsDTO groupedAnomaly) {
    return true;
  }
}
