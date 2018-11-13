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

package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * ConfigBean holds namespaced key-value configuration values.  Values are serialized into the
 * database using the default object mapper.  ConfigBean serves as a light-weight
 * alternative to existing configuration mechanisms to (a) allow at-runtime changes to configuration
 * traditionally stored in config files, and (b) alleviate the need for introducing new bean classes
 * to handle simple configuration tasks.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DetectionAlertConfigBean extends AbstractBean {
  boolean active;
  String name;
  String from;
  String cronExpression;
  String application;

  Map<String, Map<String, Object>> alertSchemes;
  Map<String, Map<String, Object>> alertSuppressors;
  AlertConfigBean.SubjectType subjectType = AlertConfigBean.SubjectType.ALERT;

  Map<Long, Long> vectorClocks;
  Long highWaterMark;

  Map<String, Object> properties;

  Map<String, String> refLinks;

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCronExpression() {
    return cronExpression;
  }

  public void setCronExpression(String cronExpression) {
    this.cronExpression = cronExpression;
  }

  public String getFrom() {
    return from;
  }

  public void setFrom(String fromAddress) {
    this.from = fromAddress;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public Map<Long, Long> getVectorClocks() {
    return vectorClocks;
  }

  public void setVectorClocks(Map<Long, Long> vectorClocks) {
    this.vectorClocks = vectorClocks;
  }

  public String getApplication() {
    return application;
  }

  public void setApplication(String application) {
    this.application = application;
  }

  public Long getHighWaterMark() {
    return highWaterMark;
  }

  public void setHighWaterMark(Long highWaterMark) {
    this.highWaterMark = highWaterMark;
  }

  public AlertConfigBean.SubjectType getSubjectType() {
    return subjectType;
  }

  public void setSubjectType(AlertConfigBean.SubjectType subjectType) {
    this.subjectType = subjectType;
  }

  public Map<String, Map<String, Object>> getAlertSchemes() {
    return alertSchemes;
  }

  public void setAlertSchemes(Map<String, Map<String, Object>> alertSchemes) {
    this.alertSchemes = alertSchemes;
  }

  public Map<String, Map<String, Object>> getAlertSuppressors() {
    return alertSuppressors;
  }

  public void setAlertSuppressors(Map<String, Map<String, Object>> alertSuppressors) {
    this.alertSuppressors = alertSuppressors;
  }

  public Map<String, String> getReferenceLinks() {
    return refLinks;
  }

  public void setReferenceLinks(Map<String, String> refLinks) {
    this.refLinks = refLinks;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DetectionAlertConfigBean that = (DetectionAlertConfigBean) o;
    return active == that.active && Objects.equals(name, that.name) && Objects.equals(from, that.from)
        && Objects.equals(cronExpression, that.cronExpression) && Objects.equals(application, that.application)
        && subjectType == that.subjectType && Objects.equals(vectorClocks, that.vectorClocks) && Objects.equals(
        highWaterMark, that.highWaterMark) && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(active, name, from, cronExpression, application, subjectType, vectorClocks,
        highWaterMark, properties);
  }
}
