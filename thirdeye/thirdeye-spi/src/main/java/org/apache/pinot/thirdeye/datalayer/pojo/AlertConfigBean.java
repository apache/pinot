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
import org.apache.pinot.thirdeye.alert.commons.AnomalyFeedConfig;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.eclipse.jetty.util.StringUtil;


@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertConfigBean extends AbstractBean {
  public enum SubjectType {
    ALERT,
    METRICS,
    DATASETS
  }

  String name;
  String application;
  String cronExpression;
  String holidayCronExpression;
  boolean active;
  AnomalyFeedConfig anomalyFeedConfig;
  EmailConfig emailConfig;
  ReportConfigCollection reportConfigCollection;
  AlertGroupConfig alertGroupConfig;
  EmailFormatterConfig emailFormatterConfig;
  DetectionAlertFilterRecipients receiverAddresses;
  String fromAddress;
  SubjectType subjectType = SubjectType.ALERT;
  Map<String, String> refLinks;

  public String getApplication() {
    return application;
  }

  public void setApplication(String application) {
    this.application = application;
  }

  public String getCronExpression() {
    return cronExpression;
  }

  public String getFromAddress() {
    return fromAddress;
  }

  public void setFromAddress(String fromAddress) {
    this.fromAddress = fromAddress;
  }

  public DetectionAlertFilterRecipients getReceiverAddresses() {
    return receiverAddresses;
  }

  public void setReceiverAddresses(DetectionAlertFilterRecipients receiverAddresses) {
    this.receiverAddresses = receiverAddresses;
  }

  public void setCronExpression(String cronExpression) {
    this.cronExpression = cronExpression;
  }

  public AnomalyFeedConfig getAnomalyFeedConfig() {
    return anomalyFeedConfig;
  }

  public void setAnomalyFeedConfig(AnomalyFeedConfig anomalyFeedConfig) {
    this.anomalyFeedConfig = anomalyFeedConfig;
  }

  public String getHolidayCronExpression() {
    return holidayCronExpression;
  }

  public void setHolidayCronExpression(String holidayCronExpression) {
    this.holidayCronExpression = holidayCronExpression;
  }

  public EmailConfig getEmailConfig() {
    return emailConfig;
  }

  public void setEmailConfig(EmailConfig emailConfig) {
    this.emailConfig = emailConfig;
  }

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

  public ReportConfigCollection getReportConfigCollection() {
    return reportConfigCollection;
  }

  public void setReportConfigCollection(ReportConfigCollection reportConfigCollection) {
    this.reportConfigCollection = reportConfigCollection;
  }

  public AlertGroupConfig getAlertGroupConfig() {
    return alertGroupConfig;
  }

  public void setAlertGroupConfig(AlertGroupConfig alertGroupConfig) {
    this.alertGroupConfig = alertGroupConfig;
  }

  public EmailFormatterConfig getEmailFormatterConfig() {
    return emailFormatterConfig;
  }

  public void setEmailFormatterConfig(EmailFormatterConfig emailFormatterConfig) {
    this.emailFormatterConfig = emailFormatterConfig;
  }

  public SubjectType getSubjectType() {
    return subjectType;
  }

  public void setSubjectType(SubjectType subjectType) {
    this.subjectType = subjectType;
  }

  public Map<String, String> getReferenceLinks() {
    return refLinks;
  }

  public void setReferenceLinks(Map<String, String> refLinks) {
    this.refLinks = refLinks;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class EmailConfig {
    long anomalyWatermark = 0l;
    List<Long> functionIds = new ArrayList<>();
    List<Long> detectionConfigIds = new ArrayList<>();

    public List<Long> getDetectionConfigIds() {
      return detectionConfigIds;
    }

    public void setDetectionConfigIds(List<Long> detectionConfigIds) {
      this.detectionConfigIds = detectionConfigIds;
    }

    public List<Long> getFunctionIds() {
      return functionIds;
    }

    public void setFunctionIds(List<Long> functionIds) {
      this.functionIds = functionIds;
    }

    public long getAnomalyWatermark() {
      return anomalyWatermark;
    }

    public void setAnomalyWatermark(long lastNotifiedAnomalyId) {
      this.anomalyWatermark = lastNotifiedAnomalyId;
    }

    @Override
    public String toString() {
      return "EmailConfig{" +
          "functionIds=" + functionIds +
          ", anomalyWatermark=" + anomalyWatermark +
          '}';
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class EmailFormatterConfig {
    String type;
    String properties;

    public String getType() {
      if (StringUtil.isBlank(type)) {
        return "";
      }
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getProperties() {
      if (StringUtil.isBlank(properties)) {
        return "";
      }
      return properties;
    }

    public void setProperties(String properties) {
      this.properties = properties;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ReportMetricConfig {
    COMPARE_MODE compareMode = COMPARE_MODE.Wo2W;
    Long metricId;
    List<String> dimensions = new ArrayList<>();
    Map<String, Collection<String>> filters = new HashMap<>();

    public COMPARE_MODE getCompareMode() {
      return compareMode;
    }

    public void setCompareMode(COMPARE_MODE compareMode) {
      this.compareMode = compareMode;
    }

    public List<String> getDimensions() {
      return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
      this.dimensions = dimensions;
    }

    public Long getMetricId() {
      return metricId;
    }

    public void setMetricId(Long metricId) {
      this.metricId = metricId;
    }

    public Map<String, Collection<String>> getFilters() {
      return filters;
    }

    public void setFilters(Map<String, Collection<String>> filters) {
      this.filters = filters;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ReportConfigCollection {
    boolean enabled;
    boolean intraDay;
    long delayOffsetMillis = 2 * 36_00_000; // 2 hours
    List<ReportMetricConfig> reportMetricConfigs = new ArrayList<>();
    String contactEmail;

    public boolean isIntraDay() {
      return intraDay;
    }

    public void setIntraDay(boolean intraDay) {
      this.intraDay = intraDay;
    }

    public long getDelayOffsetMillis() {
      return delayOffsetMillis;
    }

    public void setDelayOffsetMillis(long delayOffsetMillis) {
      this.delayOffsetMillis = delayOffsetMillis;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public String getContactEmail() {
      return contactEmail;
    }

    public List<ReportMetricConfig> getReportMetricConfigs() {
      return reportMetricConfigs;
    }

    public void setReportMetricConfigs(List<ReportMetricConfig> reportMetricConfigs) {
      this.reportMetricConfigs = reportMetricConfigs;
    }

    public void setContactEmail(String contactEmail) {
      this.contactEmail = contactEmail;
    }

    @Override public String toString() {
      return "ReportConfigCollection{" +
          "contactEmail='" + contactEmail + '\'' +
          ", enabled=" + enabled +
          ", reportMetricConfigs=" + reportMetricConfigs +
          '}';
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AlertGroupConfig {
    Map<String, String> groupByConfig = new HashMap<>();
    Map<String, String> groupFilterConfig = new HashMap<>();
    Map<String, String> groupTimeBasedMergeConfig = new HashMap<>();
    Map<String, String> groupAuxiliaryEmailProvider = new HashMap<>();

    public Map<String, String> getGroupByConfig() {
      return groupByConfig;
    }

    public void setGroupByConfig(Map<String, String> groupByConfig) {
      this.groupByConfig = groupByConfig;
    }

    public Map<String, String> getGroupFilterConfig() {
      return groupFilterConfig;
    }

    public void setGroupFilterConfig(Map<String, String> groupFilterConfig) {
      this.groupFilterConfig = groupFilterConfig;
    }

    public Map<String, String> getGroupTimeBasedMergeConfig() {
      return groupTimeBasedMergeConfig;
    }

    public void setGroupTimeBasedMergeConfig(Map<String, String> groupTimeBasedMergeConfig) {
      this.groupTimeBasedMergeConfig = groupTimeBasedMergeConfig;
    }

    public Map<String, String> getGroupAuxiliaryEmailProvider() {
      return groupAuxiliaryEmailProvider;
    }

    public void setGroupAuxiliaryEmailProvider(Map<String, String> groupAuxiliaryEmailProvider) {
      this.groupAuxiliaryEmailProvider = groupAuxiliaryEmailProvider;
    }
  }

  public enum COMPARE_MODE {
    WoW, Wo2W, Wo3W, Wo4W
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AlertConfigBean that = (AlertConfigBean) o;
    return isActive() == that.isActive() && Objects.equals(getName(), that.getName()) && Objects
        .equals(getApplication(), that.getApplication()) && Objects
        .equals(getCronExpression(), that.getCronExpression()) && Objects
        .equals(getHolidayCronExpression(), that.getHolidayCronExpression()) && Objects
        .equals(getAnomalyFeedConfig(), that.getAnomalyFeedConfig()) && Objects
        .equals(getEmailConfig(), that.getEmailConfig()) && Objects
        .equals(getReportConfigCollection(), that.getReportConfigCollection()) && Objects
        .equals(getAlertGroupConfig(), that.getAlertGroupConfig()) && Objects
        .equals(getEmailFormatterConfig(), that.getEmailFormatterConfig()) && Objects
        .equals(getReceiverAddresses(), that.getReceiverAddresses()) && Objects.equals(getFromAddress(), that.getFromAddress());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getApplication(), getCronExpression(), getHolidayCronExpression(), isActive(),
        getAnomalyFeedConfig(), getEmailConfig(), getReportConfigCollection(), getAlertGroupConfig(),
        getEmailFormatterConfig(), getReceiverAddresses(), getFromAddress());
  }
}
