package com.linkedin.thirdeye.anomalydetection.model.detection;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.Collections;
import java.util.List;

public class NoopDetectionModel extends AbstractDetectionModel {
  @Override
  public List<RawAnomalyResultDTO> detect(String metricName, AnomalyDetectionContext anomalyDetectionContext) {
    return Collections.emptyList();
  }
}
