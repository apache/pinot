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

package org.apache.pinot.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;


/**
 * The YAML config translator converts the yaml config into a detection config.
 * Calls training module for each stage.
 */
public abstract class YamlDetectionConfigTranslator {
  protected static final Logger LOG = LoggerFactory.getLogger(YamlDetectionConfigTranslator.class);
  private static final String PROP_NAME = "detectionName";


  protected Map<String, Object> yamlConfig;
  protected long startTime;
  protected long endTime;
  protected DataProvider dataProvider;
  protected DetectionConfigDTO existingConfig;
  protected Map<String, Object> existingComponentSpecs;

  public YamlDetectionConfigTranslator(Map<String, Object> yamlConfig, DataProvider provider) {
    this.yamlConfig = yamlConfig;
    this.dataProvider = provider;
    this.existingComponentSpecs = new HashMap<>();
  }

  public YamlDetectionConfigTranslator withTrainingWindow(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    return this;
  }

  public YamlDetectionConfigTranslator withExistingDetectionConfig(DetectionConfigDTO existingDTO) {
    this.existingConfig = existingDTO;
    if(existingDTO != null) this.existingComponentSpecs = existingDTO.getComponentSpecs();
    return this;
  }

  /**
   * Convert Yaml configurations into detection properties. Can be customized and override by different detection flow.
   * @return properties of the detection pipeline
   */
  abstract YamlTranslationResult translateYaml();

  /**
   * Fill in common fields of detection config. Properties of the pipeline is filled by the subclass.
   */
  DetectionConfigDTO generateDetectionConfig() {
    validateYAML(yamlConfig);

    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName(MapUtils.getString(yamlConfig, PROP_NAME));
    config.setLastTimestamp(System.currentTimeMillis());
    config.setActive(true);
    YamlTranslationResult translationResult = translateYaml();
    Preconditions.checkArgument(!translationResult.getProperties().isEmpty(), "Empty detection property");
    config.setProperties(translationResult.getProperties());
    config.setComponentSpecs(translationResult.getComponents());
    config.setCron(translationResult.getCron());

    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    options.setPrettyFlow(true);
    config.setYaml(new Yaml(options).dump(yamlConfig));

    if (existingConfig != null) {
      config.setId(existingConfig.getId());
      config.setLastTimestamp(existingConfig.getLastTimestamp());
    }

    return config;
  }

  /**
   * Check the yaml configuration is semantically valid. Throws an IllegalArgumentException if not.
   * @param yamlConfig yamlConfiguration to be checked
   */
  protected void validateYAML(Map<String, Object> yamlConfig) {
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_NAME), "Property missing " + PROP_NAME);
  }
}
