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
 *
 */

package org.apache.pinot.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.thirdeye.detection.health.DetectionHealth;


/**
 * ConfigBean holds namespaced key-value configuration values.  Values are serialized into the
 * database using the default object mapper.  ConfigBean serves as a light-weight
 * alternative to existing configuration mechanisms to (a) allow at-runtime changes to configuration
 * traditionally stored in config files, and (b) alleviate the need for introducing new bean classes
 * to handle simple configuration tasks.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DetectionConfigBean extends AbstractBean {
  String cron;
  String name;
  String description;
  long lastTimestamp;
  Map<String, Object> properties;
  boolean active;
  String yaml;
  Map<String, Object> componentSpecs;
  long lastTuningTimestamp;
  List<String> owners;

  // Stores properties related to data SLA rules for every metric
  Map<String, Object> dataQualityProperties;

  boolean isDataAvailabilitySchedule;
  long taskTriggerFallBackTimeInSec;
  DetectionHealth health;

  public List<String> getOwners() {
    return owners;
  }

  public void setOwners(List<String> owners) {
    this.owners = owners;
  }

  public Map<String, Object> getComponentSpecs() {
    return componentSpecs;
  }

  public void setComponentSpecs(Map<String, Object> componentSpecs) {
    this.componentSpecs = componentSpecs;
  }

  public String getYaml() {
    return yaml;
  }

  public void setYaml(String yaml) {
    this.yaml = yaml;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getCron() {
    return cron;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public long getLastTimestamp() {
    return lastTimestamp;
  }

  public void setLastTimestamp(long lastTimestamp) {
    this.lastTimestamp = lastTimestamp;
  }

  public long getLastTuningTimestamp() {
    return lastTuningTimestamp;
  }

  public void setLastTuningTimestamp(long lastTuningTimestamp) {
    this.lastTuningTimestamp = lastTuningTimestamp;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public Map<String, Object> getDataQualityProperties() {
    return dataQualityProperties;
  }

  public void setDataQualityProperties(Map<String, Object> dataQualityProperties) {
    this.dataQualityProperties = dataQualityProperties;
  }

  public boolean isDataAvailabilitySchedule() {
    return isDataAvailabilitySchedule;
  }

  public void setDataAvailabilitySchedule(boolean dataAvailabilitySchedule) {
    isDataAvailabilitySchedule = dataAvailabilitySchedule;
  }

  public long getTaskTriggerFallBackTimeInSec() {
    return taskTriggerFallBackTimeInSec;
  }

  public void setTaskTriggerFallBackTimeInSec(long taskTriggerFallBackTimeInSec) {
    this.taskTriggerFallBackTimeInSec = taskTriggerFallBackTimeInSec;
  }

  public DetectionHealth getHealth() {
    return health;
  }

  public void setHealth(DetectionHealth health) {
    this.health = health;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DetectionConfigBean)) {
      return false;
    }
    DetectionConfigBean that = (DetectionConfigBean) o;
    return lastTimestamp == that.lastTimestamp && active == that.active && Objects.equals(cron, that.cron)
        && Objects.equals(name, that.name) && Objects.equals(properties, that.properties) && Objects.equals(yaml,
        that.yaml) && Objects.equals(dataQualityProperties, that.dataQualityProperties)
        && Objects.equals(isDataAvailabilitySchedule, that.isDataAvailabilitySchedule) && Objects
        .equals(taskTriggerFallBackTimeInSec, that.taskTriggerFallBackTimeInSec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cron, name, lastTimestamp, properties, active, yaml);
  }
}
