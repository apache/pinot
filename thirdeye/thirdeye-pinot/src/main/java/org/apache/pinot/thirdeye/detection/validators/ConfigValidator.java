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

package org.apache.pinot.thirdeye.detection.validators;

import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.Yaml;


public abstract class ConfigValidator {

  protected static final Yaml YAML = new Yaml();

  final AlertConfigManager alertDAO;
  final ApplicationManager applicationDAO;

  ConfigValidator() {
    this.alertDAO = DAORegistry.getInstance().getAlertConfigDAO();
    this.applicationDAO = DAORegistry.getInstance().getApplicationDAO();
  }

  /**
   * Perform basic validations on a yaml file like verifying if
   * the yaml exists and is parsable
   */
  @SuppressWarnings("unchecked")
  public boolean validateYAMLConfig(String yamlConfig, Map<String, String> responseMessage) {
    // Check if YAML is empty or not
    if (StringUtils.isEmpty(yamlConfig)) {
      responseMessage.put("message", "The config file cannot be blank.");
      responseMessage.put("more-info", "Payload in the request is empty");
      return false;
    }

    // Check if the YAML is parsable
    try {
      Map<String, Object> yamlConfigMap = (Map<String, Object>) YAML.load(yamlConfig);
    } catch (Exception e) {
      responseMessage.put("message", "There was an error parsing the yaml file. Check for syntax issues.");
      responseMessage.put("more-info", "Error parsing YAML" + e);
      return false;
    }

    return true;
  }

}
