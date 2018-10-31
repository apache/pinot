package com.linkedin.thirdeye.anomalydetection.model.detection;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import java.util.Collections;
import java.util.List;

public class NoopDetectionModel extends AbstractDetectionModel {
  @Override
  public List<AnomalyResult> detect(String metricName, AnomalyDetectionContext anomalyDetectionContext) {
    return Collections.emptyList();
  }
}
