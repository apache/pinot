package com.linkedin.thirdeye.detection.finetune;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * The F1 score function based on counting anomaly.
 */
public class F1ScoreFunction implements ScoreFunction {

  /**
   * Calculate F1 score of the result against the anomalies.
   */
  @Override
  public double calculateScore(DetectionPipelineResult result, Collection<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> trueTestAnomalies = new ArrayList<>();
    int labeledAnomalies = 0;

    for (MergedAnomalyResultDTO testAnomaly : anomalies) {
      if (testAnomaly.getAnomalyFeedbackId() != null) {
        labeledAnomalies++;
        if (testAnomaly.getFeedback().getFeedbackType() == AnomalyFeedbackType.ANOMALY) {
          trueTestAnomalies.add(testAnomaly);
        }
      }
    }

    int truePositives = 0;
    for (MergedAnomalyResultDTO anomaly : result.getAnomalies()) {
      for (MergedAnomalyResultDTO testAnomaly : trueTestAnomalies) {
        if (testAnomaly.getFeedback().getFeedbackType() == AnomalyFeedbackType.ANOMALY && isOverlap(anomaly,
            testAnomaly)) {
          truePositives += 1;
          break;
        }
      }
    }

    double precision = truePositives / (double) (result.getAnomalies().size() - truePositives);
    double recall = truePositives / (double) labeledAnomalies;
    return 2 * precision * recall / (precision + recall);
  }

  private boolean isOverlap(MergedAnomalyResultDTO anomaly, MergedAnomalyResultDTO testAnomaly) {
    return (anomaly.getStartTime() > testAnomaly.getStartTime() && anomaly.getStartTime() < testAnomaly.getEndTime())
        || (anomaly.getEndTime() > testAnomaly.getStartTime() && anomaly.getEndTime() < testAnomaly.getEndTime());
  }
}
