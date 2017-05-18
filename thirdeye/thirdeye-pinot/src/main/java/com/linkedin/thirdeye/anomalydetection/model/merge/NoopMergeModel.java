package com.linkedin.thirdeye.anomalydetection.model.merge;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public class NoopMergeModel extends AbstractMergeModel implements NoPredictionMergeModel {
  @Override
  public void update(AnomalyDetectionContext anomalyDetectionContext,
      MergedAnomalyResultDTO anomalyToUpdated) {
    // Does nothing
  }

  @Override
  public Boolean isMergeable(MergedAnomalyResultDTO anomaly1, MergedAnomalyResultDTO anomaly2) {
    return true;
  }
}
