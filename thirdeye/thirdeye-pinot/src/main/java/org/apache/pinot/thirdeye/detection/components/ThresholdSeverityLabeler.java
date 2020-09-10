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

package org.apache.pinot.thirdeye.detection.components;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.thirdeye.anomaly.AnomalySeverity;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.spec.SeverityThresholdLabelerSpec;
import org.apache.pinot.thirdeye.detection.spi.components.Labeler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.detection.spec.SeverityThresholdLabelerSpec.Threshold;

/**
 * Threshold-based severity labeler, which labels anomalies with severity based on deviation from baseline and duration
 * of the anomalies. It tries to label anomalies from highest to lowest if deviation or duration exceeds the threshold
 */
@Components(title = "ThresholdSeverityLabeler", type = "THRESHOLD_SEVERITY_LABELER",
    tags = {DetectionTag.LABELER}, description = "An threshold-based labeler for anomaly severity")
public class ThresholdSeverityLabeler implements Labeler<SeverityThresholdLabelerSpec> {
  private final static Logger LOG = LoggerFactory.getLogger(ThresholdSeverityLabeler.class);
  // severity map ordered by priority from top to bottom
  private TreeMap<AnomalySeverity, Threshold> severityMap;

  @Override
  public Map<MergedAnomalyResultDTO, AnomalySeverity> label(List<MergedAnomalyResultDTO> anomalies) {
    Map<MergedAnomalyResultDTO, AnomalySeverity> res = new HashMap<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      double currVal = anomaly.getAvgCurrentVal();
      double baseVal = anomaly.getAvgBaselineVal();
      if (Double.isNaN(currVal) || Double.isNaN(baseVal)) {
        LOG.warn("Unable to label anomaly for detection {} from {} to {}, so skipping labeling...",
            anomaly.getDetectionConfigId(), anomaly.getStartTime(), anomaly.getEndTime());
        continue;
      }
      double deviation = Math.abs(currVal - baseVal) / baseVal;
      long duration = anomaly.getEndTime() - anomaly.getStartTime();
      for (Map.Entry<AnomalySeverity, Threshold> entry : severityMap.entrySet()) {
        if (deviation >= entry.getValue().change || duration >= entry.getValue().duration) {
          res.put(anomaly, entry.getKey());
          break;
        }
      }
    }
    return res;
  }

  @Override
  public void init(SeverityThresholdLabelerSpec spec, InputDataFetcher dataFetcher) {
    this.severityMap = new TreeMap<>();
    for (String key : spec.getSeverity().keySet()) {
      try {
        AnomalySeverity severity = AnomalySeverity.valueOf(key);
        this.severityMap.put(severity, spec.getSeverity().get(key));
      } catch (IllegalArgumentException e) {
        LOG.error("Cannot find valid anomaly severity, so ignoring...", e);
      }
    }
  }
}
