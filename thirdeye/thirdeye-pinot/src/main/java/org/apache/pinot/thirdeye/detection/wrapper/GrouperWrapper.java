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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.GrouperWrapperConstants;
import org.apache.pinot.thirdeye.detection.PredictionResult;
import org.apache.pinot.thirdeye.detection.spi.components.Grouper;

import static org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator.*;


/**
 * A group wrapper which triggers the configured grouper. The actual grouper
 * must implement the {@link Grouper} interface.
 *
 * A grouper must always return grouped (parent) anomalies - no child anomalies
 */
public class GrouperWrapper extends DetectionPipeline {
  private static final String PROP_NESTED = "nested";
  private static final String PROP_GROUPER = "grouper";

  private final List<Map<String, Object>> nestedProperties;

  private final Grouper grouper;
  private final String grouperName;
  private final String entityName;

  public GrouperWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);
    Map<String, Object> properties = config.getProperties();
    this.nestedProperties = ConfigUtils.getList(properties.get(PROP_NESTED));

    // Get the configured grouper from the components
    Preconditions.checkArgument(this.config.getProperties().containsKey(PROP_GROUPER));
    this.grouperName = DetectionUtils.getComponentKey(MapUtils.getString(config.getProperties(), PROP_GROUPER));
    Preconditions.checkArgument(this.config.getComponents().containsKey(this.grouperName));
    this.grouper = (Grouper) this.config.getComponents().get(this.grouperName);

    Preconditions.checkArgument(this.config.getProperties().containsKey(PROP_SUB_ENTITY_NAME));
    this.entityName = MapUtils.getString(config.getProperties(), PROP_SUB_ENTITY_NAME);
  }

  /**
   * Runs the nested pipelines and calls the group method to group the anomalies accordingly.
   * @return the detection pipeline result
   */
  @Override
  public final DetectionPipelineResult run() throws Exception {
    List<MergedAnomalyResultDTO> candidates = new ArrayList<>();
    Map<String, Object> diagnostics = new HashMap<>();
    List<PredictionResult> predictionResults = new ArrayList<>();
    List<EvaluationDTO> evaluations = new ArrayList<>();

    Set<Long> lastTimeStamps = new HashSet<>();
    for (Map<String, Object> properties : this.nestedProperties) {
      DetectionPipelineResult intermediate = this.runNested(properties, this.startTime, this.endTime);
      lastTimeStamps.add(intermediate.getLastTimestamp());
      predictionResults.addAll(intermediate.getPredictions());
      evaluations.addAll(intermediate.getEvaluations());
      diagnostics.putAll(intermediate.getDiagnostics());
      candidates.addAll(intermediate.getAnomalies());
    }

    List<MergedAnomalyResultDTO> anomalies = this.grouper.group(candidates);

    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomaly.isChild()) {
        throw new RuntimeException("Child anomalies returned by grouper. It should always return parent anomalies"
            + " with child mapping. Detection id: " + this.config.getId() + ", grouper name: " + this.grouperName);
      }

      anomaly.setDetectionConfigId(this.config.getId());
      if (anomaly.getProperties() == null) {
        anomaly.setProperties(new HashMap<>());
      }
      anomaly.getProperties().put(GrouperWrapperConstants.PROP_DETECTOR_COMPONENT_NAME, this.grouperName);
      anomaly.getProperties().put(PROP_SUB_ENTITY_NAME, this.entityName);
    }

    return new DetectionPipelineResult(anomalies, DetectionUtils.consolidateNestedLastTimeStamps(lastTimeStamps),
        predictionResults, evaluations).setDiagnostics(diagnostics);
  }
}
