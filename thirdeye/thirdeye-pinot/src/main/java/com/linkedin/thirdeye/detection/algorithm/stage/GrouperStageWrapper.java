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
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;


/**
 * The grouper stage wrapper runs the grouper stage and return a list of anomalies has the dimensions grouped.
 */
public class GrouperStageWrapper extends DetectionPipeline {
  private static final String PROP_NESTED = "nested";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_STAGE_CLASSNAME = "stageClassName";
  private static final String PROP_SPECS = "specs";
  private final List<Map<String, Object>> nestedProperties;
  private GrouperStage grouperStage;

  public GrouperStageWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);
    Map<String, Object> properties = config.getProperties();
    this.nestedProperties = ConfigUtils.getList(properties.get(PROP_NESTED));
    Preconditions.checkArgument(properties.containsKey(PROP_STAGE_CLASSNAME), "Missing " + PROP_STAGE_CLASSNAME);

    this.grouperStage = loadGroupingStage(MapUtils.getString(properties, PROP_STAGE_CLASSNAME));
    this.grouperStage.init(MapUtils.getMap(properties, PROP_SPECS), config.getId(), startTime, endTime);
  }

  /**
   * Runs the nested pipelines and calls the group method to groups the anomalies by dimension.
   * @return the detection pipeline result
   * @throws Exception
   */
  @Override
  public final DetectionPipelineResult run() throws Exception {
    List<MergedAnomalyResultDTO> candidates = new ArrayList<>();
    for (Map<String, Object> properties : this.nestedProperties) {
      DetectionConfigDTO nestedConfig = new DetectionConfigDTO();

      Preconditions.checkArgument(properties.containsKey(PROP_CLASS_NAME), "Nested missing " + PROP_CLASS_NAME);
      nestedConfig.setId(this.config.getId());
      nestedConfig.setName(this.config.getName());
      nestedConfig.setProperties(properties);

      DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

      DetectionPipelineResult intermediate = pipeline.run();
      candidates.addAll(intermediate.getAnomalies());
    }

    Collection<MergedAnomalyResultDTO> anomalies = this.grouperStage.group(candidates, this.provider);

    return new DetectionPipelineResult(new ArrayList<>(anomalies));
  }

  private GrouperStage loadGroupingStage(String className) throws Exception {
    return (GrouperStage) Class.forName(className).newInstance();
  }
}
