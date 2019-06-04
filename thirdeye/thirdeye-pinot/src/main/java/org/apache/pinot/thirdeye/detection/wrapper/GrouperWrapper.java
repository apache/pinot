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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.PredictionResult;
import org.apache.pinot.thirdeye.detection.spi.components.Grouper;


/**
 * A group wrapper which triggers the configured grouper. The actual grouper
 * must implement the {@link Grouper} interface.
 */
public class GrouperWrapper extends DetectionPipeline {
  private static final String PROP_NESTED = "nested";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_GROUPER = "grouper";

  private final List<Map<String, Object>> nestedProperties;

  private final Grouper grouper;
  private final String grouperName;

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
  }

  /**
   * Runs the nested pipelines and calls the group method to group the anomalies accordingly.
   * @return the detection pipeline result
   */
  @Override
  public final DetectionPipelineResult run() throws Exception {
    List<MergedAnomalyResultDTO> candidates = new ArrayList<>();

    Set<Long> lastTimeStamps = new HashSet<>();
    List<PredictionResult> predictionResults = new ArrayList<>();
    for (Map<String, Object> properties : this.nestedProperties) {
      DetectionConfigDTO nestedConfig = new DetectionConfigDTO();

      Preconditions.checkArgument(properties.containsKey(PROP_CLASS_NAME), "Nested missing " + PROP_CLASS_NAME);
      nestedConfig.setId(this.config.getId());
      nestedConfig.setName(this.config.getName());
      nestedConfig.setDescription(this.config.getDescription());
      nestedConfig.setProperties(properties);

      DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

      DetectionPipelineResult intermediate = pipeline.run();
      lastTimeStamps.add(intermediate.getLastTimestamp());
      predictionResults.addAll(intermediate.getPredictions());

      candidates.addAll(intermediate.getAnomalies());
    }

    List<MergedAnomalyResultDTO> anomalies = this.grouper.group(candidates);

    return new DetectionPipelineResult(anomalies, DetectionUtils.consolidateNestedLastTimeStamps(lastTimeStamps), predictionResults);
  }
}
