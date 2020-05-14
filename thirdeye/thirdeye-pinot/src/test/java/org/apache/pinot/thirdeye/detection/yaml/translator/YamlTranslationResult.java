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

import java.util.Map;


public class YamlTranslationResult {
  private Map<String, Object> properties;
  private Map<String, Object> dataQualityProperties;
  private Map<String, Object> components;
  private String cron;

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setDataQualityProperties(Map<String, Object> dataQualityProperties) {
    this.dataQualityProperties = dataQualityProperties;
  }

  public Map<String, Object> getDataQualityProperties() {
    return dataQualityProperties;
  }

  public void setComponents(Map<String, Object> components) {
    this.components = components;
  }

  public Map<String, Object> getComponents() {
    return components;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public String getCron() {
    return cron;
  }

}
