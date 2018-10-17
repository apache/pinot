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

package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
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
import javax.annotation.Nullable;

/**
 * This abstract filter wrapper allows user to plug in exclusion filter business rules in the detection pipeline.
 */
public abstract class RuleBasedFilterWrapper extends DetectionPipeline {
  private static final String PROP_NESTED = "nested";
  private static final String PROP_CLASS_NAME = "className";

  private final List<Map<String, Object>> nestedProperties;

  public RuleBasedFilterWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);
    this.nestedProperties = ConfigUtils.getList(config.getProperties().get(PROP_NESTED));
  }

  /**
   * Runs the nested pipelines and calls the isQualified method to check if an anomaly passes the rule filter.
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

    Collection<MergedAnomalyResultDTO> anomalies =
        Collections2.filter(candidates, new Predicate<MergedAnomalyResultDTO>() {
          @Override
          public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomaly) {
            return mergedAnomaly != null && !mergedAnomaly.isChild() && isQualified(mergedAnomaly);
          }
        });

    return new DetectionPipelineResult(new ArrayList<>(anomalies));
  }

  /**
   * Sub-classes override this method to check if anomaly passes the filter wrapper.
   * @param anomaly
   * @return if the anomaly passes the filter
   */
  abstract boolean isQualified(MergedAnomalyResultDTO anomaly);
}
