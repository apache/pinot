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

package com.linkedin.thirdeye.detection.tune;

import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.detection.baseline.RuleBaselineProvider;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.eclipse.jetty.util.StringUtil;


/**
 * The rule detection training module. Parameters of the stage is defined by the user.
 */
public class RuleDetectionTrainingModule extends StaticStageTrainingModule {
  private static final String PROP_OFFSET = "offset";
  private static final String PROP_TYPE = "type";

  private Map<String, Object> yamlConfigs = new HashMap<>();
  private Map<String, Object> baselineProviderSpecs = new HashMap<>();

  @Override
  public void init(Map<String, Object> properties, long startTime, long endTime) {
    this.yamlConfigs = MapUtils.getMap(properties, PROP_YAML_CONFIG);
    this.baselineProviderSpecs.put(PROP_CLASS_NAME, RuleBaselineProvider.class.getName());
    String offset = MapUtils.getString(properties, PROP_OFFSET);
    if (StringUtil.isBlank(offset)) {
      offset = "wo1w";
    }
    this.baselineProviderSpecs.put(PROP_OFFSET, offset);
  }

  @Override
  TrainingResult fit(InputData data) {
    Map<String, Object> stageSpecs = new HashMap<>();
    for (Map.Entry<String, Object> entry : this.yamlConfigs.entrySet()) {
      if (!entry.getKey().equals(PROP_TYPE)){
        stageSpecs.put(entry.getKey(), entry.getValue());
      }
    }
    return new TrainingResult().withStageSpecs(stageSpecs).withBaselineSpecs(this.baselineProviderSpecs);
  }

  @Override
  InputDataSpec getInputDataSpec() {
    // empty input data spec, don't fetch data
    return new InputDataSpec();
  }
}
