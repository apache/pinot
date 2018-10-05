/*
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

package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * The Child keeping Merge Wrapper. Merge anomalies and the anomalies before merging in the merged anomaly children set.
 * Useful when merging anomalies from different source, e.g, different algorithms/rules, this merger allows tracing back to anomalies before merging.
 * Will not merge anomalies if potential merged anomaly is beyond max duration.
 */
public class ChildKeepingMergeWrapper extends MergeWrapper {
  public ChildKeepingMergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);
  }

  @Override
  protected List<MergedAnomalyResultDTO> merge(Collection<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> input = new ArrayList<>(anomalies);
    Collections.sort(input, COMPARATOR);

    List<MergedAnomalyResultDTO> output = new ArrayList<>();

    Map<AnomalyKey, MergedAnomalyResultDTO> parents = new HashMap<>();
    for (MergedAnomalyResultDTO anomaly : input) {
      if (anomaly.isChild()) {
        continue;
      }

      AnomalyKey key = AnomalyKey.from(anomaly);
      MergedAnomalyResultDTO parent = parents.get(key);

      if (parent == null || anomaly.getStartTime() - parent.getEndTime() > this.maxGap) {
        // no parent, too far away
        parents.put(key, anomaly);
        output.add(anomaly);
      } else if (anomaly.getEndTime() <= parent.getEndTime()
          || anomaly.getEndTime() - parent.getStartTime() <= this.maxDuration) {
        // fully merge into existing
        if (parent.getChildren().isEmpty()){
          parent.getChildren().add(copyAnomaly(parent));
        }
        parent.setEndTime(Math.max(parent.getEndTime(), anomaly.getEndTime()));

        if (anomaly.getChildren().isEmpty()) {
          parent.getChildren().add(anomaly);
        } else {
          parent.getChildren().addAll(anomaly.getChildren());
        }
      } else {
        // partially overlap but potential merged anomaly is beyond max duration or merge not possible, do not merge
        parents.put(key, anomaly);
        output.add(anomaly);
      }
    }

    return output;
  }

  MergedAnomalyResultDTO copyAnomaly(MergedAnomalyResultDTO anomaly) {
    MergedAnomalyResultDTO newAnomaly = new MergedAnomalyResultDTO();
    newAnomaly.setStartTime(anomaly.getStartTime());
    newAnomaly.setEndTime(anomaly.getEndTime());
    newAnomaly.setMetric(anomaly.getMetric());
    newAnomaly.setMetricUrn(anomaly.getMetricUrn());
    newAnomaly.setCollection(anomaly.getCollection());
    newAnomaly.setDimensions(anomaly.getDimensions());
    newAnomaly.setDetectionConfigId(anomaly.getDetectionConfigId());
    newAnomaly.setAnomalyResultSource(anomaly.getAnomalyResultSource());
    return newAnomaly;
  }
}
