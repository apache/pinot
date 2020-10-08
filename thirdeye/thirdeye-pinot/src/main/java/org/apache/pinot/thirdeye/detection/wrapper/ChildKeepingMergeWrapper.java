/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.detection.wrapper;

import com.google.common.collect.Collections2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.algorithm.MergeWrapper;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;


/**
 * The Child keeping Merge Wrapper. Merge anomalies and the anomalies before merging in the merged anomaly children set.
 * Useful when merging anomalies from different source, e.g, different algorithms/rules, this merger allows tracing back to anomalies before merging.
 * Will not merge anomalies if potential merged anomaly is beyond max duration. Will be able to fill in current and baseline value if configured.
 * Merge anomalies regardless of anomaly merge key.
 */
public class ChildKeepingMergeWrapper extends BaselineFillingMergeWrapper {
  private static final String PROP_GROUP_KEY = "groupKey";
  private static final String PROP_PATTERN_KEY = "pattern";

  public ChildKeepingMergeWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);
  }

  @Override
  // retrieve the anomalies that are detected by multiple detectors
  protected List<MergedAnomalyResultDTO> retrieveAnomaliesFromDatabase(List<MergedAnomalyResultDTO> generated) {
    AnomalySlice effectiveSlice = this.slice.withDetectionId(this.config.getId())
        .withStart(this.getStartTime(generated) - this.maxGap - 1)
        .withEnd(this.getEndTime(generated) + this.maxGap + 1);

    Collection<MergedAnomalyResultDTO> anomalies =
        this.provider.fetchAnomalies(Collections.singleton(effectiveSlice)).get(effectiveSlice);

    return anomalies.stream().filter(anomaly -> !anomaly.isChild()
        && anomaly.getAnomalyResultSource().equals(AnomalyResultSource.DEFAULT_ANOMALY_DETECTION)
        && ThirdEyeUtils.isDetectedByMultipleComponents(anomaly))
        .collect(Collectors.toList());
  }

  @Override
  protected List<MergedAnomalyResultDTO> merge(Collection<MergedAnomalyResultDTO> anomalies) {
    List<MergedAnomalyResultDTO> input = new ArrayList<>(anomalies);
    Map<Long, MergedAnomalyResultDTO> existingParentAnomalies = new HashMap<>();
    for (MergedAnomalyResultDTO anomaly : input) {
      if (anomaly.getId() != null && !anomaly.getChildren().isEmpty()) {
        existingParentAnomalies.put(anomaly.getId(), copyAnomalyInfo(anomaly, new MergedAnomalyResultDTO()));
      }
    }

    Collections.sort(input, MergeWrapper.COMPARATOR);

    List<MergedAnomalyResultDTO> output = new ArrayList<>();

    Map<MergeWrapper.AnomalyKey, MergedAnomalyResultDTO> parents = new HashMap<>();
    for (MergedAnomalyResultDTO anomaly : input) {
      if (anomaly.isChild()) {
        continue;
      }

      // Prevent merging of grouped anomalies
      String groupKey = "";
      if (anomaly.getProperties().containsKey(PROP_GROUP_KEY)) {
        groupKey = anomaly.getProperties().get(PROP_GROUP_KEY);
      }
      String patternKey = "";
      if (anomaly.getProperties().containsKey(PROP_PATTERN_KEY)) {
        patternKey = anomaly.getProperties().get(PROP_PATTERN_KEY);
      } else if (!Double.isNaN(anomaly.getAvgBaselineVal()) && !Double.isNaN(anomaly.getAvgCurrentVal())) {
        patternKey = (anomaly.getAvgCurrentVal() > anomaly.getAvgBaselineVal()) ? "UP" : "DOWN";
      }
      MergeWrapper.AnomalyKey key =
          new MergeWrapper.AnomalyKey(anomaly.getMetric(), anomaly.getCollection(), anomaly.getDimensions(),
              StringUtils.join(Arrays.asList(groupKey, patternKey), ","), "", anomaly.getType());
      MergedAnomalyResultDTO parent = parents.get(key);

      if (parent == null || anomaly.getStartTime() - parent.getEndTime() > this.maxGap) {
        // no parent, too far away
        parents.put(key, anomaly);
        output.add(anomaly);
      } else if (anomaly.getEndTime() <= parent.getEndTime()
          || anomaly.getEndTime() - parent.getStartTime() <= this.maxDuration) {
        // fully merge into existing
        if (parent.getChildren().isEmpty()) {
          parent.getChildren().add(copyAnomalyInfo(parent, new MergedAnomalyResultDTO()));
        }
        parent.setEndTime(Math.max(parent.getEndTime(), anomaly.getEndTime()));

        // merge the anomaly's properties into parent
        ThirdEyeUtils.mergeAnomalyProperties(parent.getProperties(), anomaly.getProperties());
        // merge the anomaly severity
        if (parent.getSeverityLabel().compareTo(anomaly.getSeverityLabel()) > 0) {
          // set the highest severity
          parent.setSeverityLabel(anomaly.getSeverityLabel());
        }
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

    // Refill current and baseline values for qualified parent anomalies
    // Ignore filling baselines for exiting parent anomalies and grouped anomalies
    Collection<MergedAnomalyResultDTO> parentAnomalies = Collections2.filter(output,
        mergedAnomaly -> mergedAnomaly != null && !mergedAnomaly.getChildren().isEmpty() && !isExistingAnomaly(
            existingParentAnomalies, mergedAnomaly) && !StringUtils.isBlank(mergedAnomaly.getMetricUrn()));
    super.fillCurrentAndBaselineValue(new ArrayList<>(parentAnomalies));
    return output;
  }
}
