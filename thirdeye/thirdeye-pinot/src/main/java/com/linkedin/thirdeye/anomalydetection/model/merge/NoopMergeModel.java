package com.linkedin.thirdeye.anomalydetection.model.merge;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public class NoopMergeModel extends AbstractMergeModel implements NoPredictionMergeModel {
  @Override
  public void update(AnomalyDetectionContext anomalyDetectionContext,
      MergedAnomalyResultDTO anomalyToUpdated) {
    this.weight = anomalyToUpdated.getWeight();
    this.score = anomalyToUpdated.getScore();
  }
}
