package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Map;


public interface AlertFilter {
  List<String> getPropertyNames();

  void setParameters(Map<String, String> props);

  boolean isQualified(MergedAnomalyResultDTO anomaly);
}
