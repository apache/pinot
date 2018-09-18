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

package com.linkedin.thirdeye.detection.algorithm.stage;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;


/**
 * Anomaly Detection Stage Wrapper. This wrapper runs a anomaly detection stage and return the anomalies.
 */
public class AnomalyDetectionStageWrapper extends DetectionPipeline {
  private static final String PROP_STAGE_CLASSNAME = "stageClassName";
  private static final String PROP_SPECS = "specs";

  private final AnomalyDetectionStage anomalyDetectionStage;

  public AnomalyDetectionStageWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);

    Map<String, Object> properties = config.getProperties();
    Preconditions.checkArgument(properties.containsKey(PROP_STAGE_CLASSNAME), "Missing " + PROP_STAGE_CLASSNAME);

    this.anomalyDetectionStage = loadAnomalyDetectorStage(MapUtils.getString(properties, PROP_STAGE_CLASSNAME));
    this.anomalyDetectionStage.init(MapUtils.getMap(properties, PROP_SPECS));
  }

  @Override
  public DetectionPipelineResult run() {
    List<MergedAnomalyResultDTO> anomalies = this.anomalyDetectionStage.runDetection(this.startTime, this.endTime, this.provider);
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      anomaly.setDetectionConfigId(this.config.getId());
    }
    return new DetectionPipelineResult(anomalies);
  }

  private AnomalyDetectionStage loadAnomalyDetectorStage(String className) throws Exception {
    return (AnomalyDetectionStage) Class.forName(className).newInstance();
  }
}
