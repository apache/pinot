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

package org.apache.pinot.thirdeye.detection.yaml.translator;

import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.thirdeye.detection.validators.DetectionConfigValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The YAML config translator converts the yaml config into a detection config.
 * Calls training module for each stage.
 */
public abstract class DetectionConfigTranslator extends ConfigTranslator<DetectionConfigDTO, DetectionConfigValidator> {
  protected static final Logger LOG = LoggerFactory.getLogger(DetectionConfigTranslator.class);
  private static final String PROP_NAME = "detectionName";
  private static final String PROP_DESC_NAME = "description";
  private static final String PROP_ACTIVE = "active";

  protected DataProvider dataProvider;

  DetectionConfigTranslator(String yamlConfig, DataProvider provider, DetectionConfigValidator validator) {
    super(yamlConfig, validator);
    this.dataProvider = provider;
  }

  /**
   * Fill in common fields of detection config. Properties of the pipeline is filled by the subclass.
   */
  DetectionConfigDTO generateDetectionConfig(Map<String, Object> properties, Map<String, Object> components, String cron) {
    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName(MapUtils.getString(yamlConfigMap, PROP_NAME));
    config.setDescription(MapUtils.getString(yamlConfigMap, PROP_DESC_NAME));
    config.setLastTimestamp(System.currentTimeMillis());

    config.setProperties(properties);
    config.setComponentSpecs(components);
    config.setCron(cron);
    config.setActive(MapUtils.getBooleanValue(yamlConfigMap, PROP_ACTIVE, true));
    config.setYaml(yamlConfig);

    return config;
  }
}
