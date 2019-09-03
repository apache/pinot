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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.dto.AbstractDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.validators.ConfigValidator;
import org.yaml.snakeyaml.Yaml;


/**
 * Translate a raw yaml config and map it to a config object
 */
public abstract class ConfigTranslator<T extends AbstractDTO, V extends ConfigValidator> {

  protected final String yamlConfig;
  protected final ConfigValidator validator;
  protected final Yaml yaml;

  ConfigTranslator(String yamlConfig, V validator) {
    this.yamlConfig = yamlConfig;
    this.validator = validator;
    this.yaml = new Yaml();
  }

  List<String> filterOwners(List<String> configuredOwners) {
    List<String> owners = new ArrayList<>();
    for (String configuredOwner : configuredOwners) {
      // TODO: check if configured owner is a valid account
      owners.add(configuredOwner.trim());
    }
    // Return after removing duplicates
    return new ArrayList<>(new HashSet<>(owners));
  }

  abstract T translateConfig(Map<String, Object> yamlConfigMap) throws IllegalArgumentException;

  /**
   * Convert raw yaml configuration into config object with pre and post validation
   */
  public T translate() throws IllegalArgumentException {
    Map<String, Object> yamlConfigMap = new HashMap<>(ConfigUtils.getMap(this.yaml.load(yamlConfig)));
    validator.validateYaml(yamlConfigMap);
    return this.translateConfig(yamlConfigMap);
  }
}
