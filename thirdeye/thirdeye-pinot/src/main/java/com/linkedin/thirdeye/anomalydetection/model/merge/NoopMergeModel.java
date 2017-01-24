package com.linkedin.thirdeye.anomalydetection.model.merge;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public class NoopMergeModel extends AbstractMergeModel implements NoPredictionMergeModel {
  private static final String DEFAULT_MESSAGE_TEMPLATE = "weight: %.2f, score: %.2f";

  @Override
  public void update(AnomalyDetectionContext anomalyDetectionContext,
      MergedAnomalyResultDTO anomalyToUpdated) {
    double weight = anomalyToUpdated.getWeight();
    double score = anomalyToUpdated.getScore();

    anomalyToUpdated.setWeight(weight);
    anomalyToUpdated.setScore(score);
    anomalyToUpdated.setMessage(String.format(DEFAULT_MESSAGE_TEMPLATE, weight, score));
  }
}
