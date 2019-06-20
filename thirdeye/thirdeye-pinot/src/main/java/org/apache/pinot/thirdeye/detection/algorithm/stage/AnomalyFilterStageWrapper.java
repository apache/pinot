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

package org.apache.pinot.thirdeye.detection.algorithm.stage;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.MapUtils;


/**
 * This anomaly filter wrapper allows user to plug in filter rules in the detection pipeline.
 */
public class AnomalyFilterStageWrapper extends DetectionPipeline {
  private static final String PROP_NESTED = "nested";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_STAGE_CLASSNAME = "stageClassName";
  private static final String PROP_SPECS = "specs";
  private static final String PROP_METRIC_URN = "metricUrn";

  private final List<Map<String, Object>> nestedProperties;
  private final AnomalyFilterStage anomalyFilter;
  private String metricUrn;

  public AnomalyFilterStageWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);
    Map<String, Object> properties = config.getProperties();
    this.nestedProperties = ConfigUtils.getList(properties.get(PROP_NESTED));
    Preconditions.checkArgument(properties.containsKey(PROP_STAGE_CLASSNAME), "Missing " + PROP_STAGE_CLASSNAME);

    this.anomalyFilter = loadAnomalyFilterStage(MapUtils.getString(properties, PROP_STAGE_CLASSNAME));
    this.anomalyFilter.init(ConfigUtils.getMap(properties.get(PROP_SPECS)), config.getId(), startTime, endTime);
    this.metricUrn = MapUtils.getString(properties, PROP_METRIC_URN);
  }

  /**
   * Runs the nested pipelines and calls the isQualified method in the anomaly filter stage to check if an anomaly passes the filter.
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
      nestedConfig.setDescription(this.config.getDescription());
      nestedConfig.setProperties(properties);
      if (this.metricUrn != null){
        properties.put(PROP_METRIC_URN, this.metricUrn);
      }
      DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

      DetectionPipelineResult intermediate = pipeline.run();
      candidates.addAll(intermediate.getAnomalies());
    }

    Collection<MergedAnomalyResultDTO> anomalies =
        Collections2.filter(candidates, new Predicate<MergedAnomalyResultDTO>() {
          @Override
          public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomaly) {
            return mergedAnomaly != null && !mergedAnomaly.isChild() && anomalyFilter.isQualified(mergedAnomaly,
                provider);
          }
        });

    return new DetectionPipelineResult(new ArrayList<>(anomalies));
  }

  private AnomalyFilterStage loadAnomalyFilterStage(String className) throws Exception {
    return (AnomalyFilterStage) Class.forName(className).newInstance();
  }
}
