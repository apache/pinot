package com.linkedin.thirdeye.detection.finetune;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.linkedin.thirdeye.api.DimensionMap;
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

    Collection<MergedAnomalyResultDTO> allLabeledAnomalies =
        Collections2.filter(testAnomalies, new Predicate<MergedAnomalyResultDTO>() {
          @Override
          public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
            return mergedAnomalyResultDTO.getAnomalyFeedbackId() != null;
          }
        });

    long totalLabeledTrueAnomaliesTime = 0;
    long totalTruePositiveTime = 0;
    long totalOverlappedTime = 0;

    // TODO: make this O(NlogN) solution to minimize space consumption if necessary

    Multimap<DimensionMap, MergedAnomalyResultDTO> groupedLabeled = Multimaps.index(allLabeledAnomalies, new Function<MergedAnomalyResultDTO, DimensionMap>() {
      @Override
      public DimensionMap apply(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
        return mergedAnomalyResultDTO.getDimensions();
      }
    });

    Multimap<DimensionMap, MergedAnomalyResultDTO> groupedResults = Multimaps.index(anomalyResults, new Function<MergedAnomalyResultDTO, DimensionMap>() {
      @Override
      public DimensionMap apply(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
        return mergedAnomalyResultDTO.getDimensions();
      }
    });

    for (DimensionMap key : groupedLabeled.keySet()) {
      Set<Long> resultAnomalyTimes = new HashSet<>();
      for (MergedAnomalyResultDTO anomaly : groupedResults.get(key)) {
        for (long time = anomaly.getStartTime(); time < anomaly.getEndTime(); time += bucketSize) {
          resultAnomalyTimes.add(time / bucketSize);
        }
      }

      for (MergedAnomalyResultDTO labeledAnomaly : groupedLabeled.get(key)) {
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
    }

    double precision = totalTruePositiveTime / (double) totalOverlappedTime;
    double recall = totalTruePositiveTime / (double) totalLabeledTrueAnomaliesTime;
    return 2 * precision * recall / (precision + recall);
  }
}
