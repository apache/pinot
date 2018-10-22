/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  private static final long BUCKET_SIZE = TimeUnit.MINUTES.toMillis(1);
  private static final double beta = 3;

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
        for (long time = anomaly.getStartTime(); time < anomaly.getEndTime(); time += BUCKET_SIZE) {
          resultAnomalyTimes.add(time / BUCKET_SIZE);
        }
      }

      for (MergedAnomalyResultDTO labeledAnomaly : groupedLabeled.get(key)) {
        if (labeledAnomaly.getFeedback().getFeedbackType().isAnomaly()) {
          totalLabeledTrueAnomaliesTime += (labeledAnomaly.getEndTime() - labeledAnomaly.getStartTime());
        }
        for (long time = labeledAnomaly.getStartTime(); time < labeledAnomaly.getEndTime(); time += BUCKET_SIZE) {
          if (resultAnomalyTimes.contains(time / BUCKET_SIZE)) {
            totalOverlappedTime += BUCKET_SIZE;
            if (labeledAnomaly.getFeedback().getFeedbackType().isAnomaly()) {
              totalTruePositiveTime += BUCKET_SIZE;
            }
          }
        }
      }
    }

    double precision = totalTruePositiveTime / (double) totalOverlappedTime;
    double recall = totalTruePositiveTime / (double) totalLabeledTrueAnomaliesTime;
    return (1 + beta * beta) * precision * recall / ((beta * beta) * precision + recall);
  }
}
