package com.linkedin.thirdeye.detection.finetune;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;


/**
 * The F1 score function based on time bucket counting.
 */
public class TimeBucketF1ScoreFunction implements ScoreFunction {

  private long bucketSize = TimeUnit.MINUTES.toMillis(1);

  @Override
  public double calculateScore(DetectionPipelineResult detectionResult, Collection<MergedAnomalyResultDTO> testAnomalies) {
    List<MergedAnomalyResultDTO> anomalyResults = detectionResult.getAnomalies();
    Set<Long> resultAnomalyTimes = new HashSet<>();
    for (MergedAnomalyResultDTO anomaly : anomalyResults) {
      for (long time = anomaly.getStartTime(); time < anomaly.getEndTime(); time += bucketSize) {
        resultAnomalyTimes.add(time / bucketSize);
      }
    }

    Collection<MergedAnomalyResultDTO> labeledAnomalies =
        Collections2.filter(testAnomalies, new Predicate<MergedAnomalyResultDTO>() {
          @Override
          public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
            return mergedAnomalyResultDTO.getAnomalyFeedbackId() != null;
          }
        });

    long totalLabeledTrueAnomaliesTime = 0;
    long totalTruePositiveTime = 0;
    long totalOverlappedTime = 0;

    for (MergedAnomalyResultDTO labeledAnomaly : labeledAnomalies) {
      if (labeledAnomaly.getFeedback().getFeedbackType() == AnomalyFeedbackType.ANOMALY) {
        totalLabeledTrueAnomaliesTime += (labeledAnomaly.getEndTime() - labeledAnomaly.getStartTime());
      }
      for (long time = labeledAnomaly.getStartTime(); time < labeledAnomaly.getEndTime(); time += bucketSize) {
        if (resultAnomalyTimes.contains(time / bucketSize)) {
          totalOverlappedTime += bucketSize;
          if (labeledAnomaly.getFeedback().getFeedbackType() == AnomalyFeedbackType.ANOMALY) {
            totalTruePositiveTime += bucketSize;
          }
        }
      }
    }

    double precision = totalTruePositiveTime / (double) totalOverlappedTime;
    double recall = totalTruePositiveTime / (double) totalLabeledTrueAnomaliesTime;
    return 2 * precision * recall / (precision + recall);
  }
}
