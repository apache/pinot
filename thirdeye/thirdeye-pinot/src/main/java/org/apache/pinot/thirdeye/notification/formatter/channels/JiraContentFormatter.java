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

package org.apache.pinot.thirdeye.notification.formatter.channels;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.notification.commons.JiraConfiguration;
import org.apache.pinot.thirdeye.notification.commons.JiraEntity;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.notification.content.templates.MetricAnomaliesContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.notification.commons.ThirdEyeJiraClient.*;


/**
 * This class formats the content for jira alerts
 */
public class JiraContentFormatter extends AlertContentFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(JiraContentFormatter.class);

  private JiraConfiguration jiraAdminConfig;

  private static final String CHARSET = "UTF-8";
  static final String PROP_DEFAULT_LABEL = "thirdeye";

  public static final int MAX_JIRA_SUMMARY_LENGTH = 255;

  private static final Map<String, String> alertContentToTemplateMap;

  static {
    Map<String, String> aMap = new HashMap<>();
    aMap.put(MetricAnomaliesContent.class.getSimpleName(), "jira-metric-anomalies-template.ftl");
    alertContentToTemplateMap = Collections.unmodifiableMap(aMap);
  }

  public JiraContentFormatter(JiraConfiguration jiraAdminConfig, Properties jiraClientConfig,
      BaseNotificationContent content, ThirdEyeAnomalyConfiguration teConfig, DetectionAlertConfigDTO subsConfig) {
    super(jiraClientConfig, content, teConfig, subsConfig);

    this.jiraAdminConfig = jiraAdminConfig;
    validateJiraConfigs(jiraAdminConfig);
  }

  /**
   * Make sure the base admin parameters are configured before proceeding
   */
  private void validateJiraConfigs(JiraConfiguration jiraAdminConfig) {
    Preconditions.checkNotNull(jiraAdminConfig.getJiraUser());
    Preconditions.checkNotNull(jiraAdminConfig.getJiraPassword());
    Preconditions.checkNotNull(jiraAdminConfig.getJiraHost());
  }

  /**
   * Format and construct a {@link JiraEntity} by rendering the anomalies and properties
   *
   * @param dimensionFilters dimensions configured in the multi-dimensions alerter
   * @param anomalies anomalies to be reported to recipients configured in (@link #jiraClientConfig}
   */
  public JiraEntity getJiraEntity(Multimap<String, String> dimensionFilters, Collection<AnomalyResult> anomalies) {
    Map<String, Object> templateData = notificationContent.format(anomalies, this.subsConfig);
    templateData.put("dashboardHost", teConfig.getDashboardHost());
    return buildJiraEntity(alertContentToTemplateMap.get(notificationContent.getTemplate()), templateData,
        dimensionFilters);
  }

  private String buildSummary(Map<String, Object> templateValues, Multimap<String, String> dimensionFilters) {
    String issueSummary =
        BaseNotificationContent.makeSubject(super.getSubjectType(alertClientConfig), this.subsConfig, templateValues);

    // Append dimensional info to summary
    StringBuilder dimensions = new StringBuilder();
    for (Map.Entry<String, Collection<String>> dimFilter : dimensionFilters.asMap().entrySet()) {
      dimensions.append(", ").append(dimFilter.getKey()).append("=").append(String.join(",", dimFilter.getValue()));
    }
    issueSummary = issueSummary + dimensions.toString();

    // Truncate summary due to jira character limit
    return StringUtils.abbreviate(issueSummary, MAX_JIRA_SUMMARY_LENGTH);
  }

  private List<String> buildLabels(Multimap<String, String> dimensionFilters) {
    List<String> labels = ConfigUtils.getList(alertClientConfig.get(PROP_LABELS));
    labels.add(PROP_DEFAULT_LABEL);
    labels.add("subsId=" + this.subsConfig.getId().toString());
    dimensionFilters.asMap().forEach((k, v) -> labels.add(k + "=" + String.join(",", v)));
    return labels;
  }

  private String buildDescription(String jiraTemplate, Map<String, Object> templateValues) {
    String description;

    // Render the values in templateValues map to the jira ftl template file
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/org/apache/pinot/thirdeye/detector");
      freemarkerConfig.setDefaultEncoding(CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Template template = freemarkerConfig.getTemplate(jiraTemplate);
      template.process(templateValues, out);

      description = new String(baos.toByteArray(), CHARSET);
    } catch (Exception e) {
      description = "Found an exception while constructing the description content. Pls report & reach out"
          + " to the Thirdeye team. Exception = " + e.getMessage();
    }

    return description;
  }

  private File buildSnapshot() {
    File snapshotFile = null;
    try {
      snapshotFile = new File(this.notificationContent.getSnaphotPath());
    } catch (Exception e) {
      LOG.error("Exception while loading snapshot {}", this.notificationContent.getSnaphotPath(), e);
    }
    return snapshotFile;
  }

  /**
   * Apply the parameter map to given jira template, and format it as JiraEntity
   */
  private JiraEntity buildJiraEntity(String jiraTemplate, Map<String, Object> templateValues,
      Multimap<String, String> dimensionFilters) {
    String jiraProject = MapUtils.getString(alertClientConfig, PROP_PROJECT, this.jiraAdminConfig.getJiraDefaultProjectKey());
    Long jiraIssueTypeId = MapUtils.getLong(alertClientConfig, PROP_ISSUE_TYPE, this.jiraAdminConfig.getJiraIssueTypeId());

    JiraEntity jiraEntity = new JiraEntity(jiraProject, jiraIssueTypeId, buildSummary(templateValues, dimensionFilters));
    jiraEntity.setAssignee(MapUtils.getString(alertClientConfig, PROP_ASSIGNEE, "")); // Default - Unassigned
    jiraEntity.setMergeGap(MapUtils.getLong(alertClientConfig, PROP_MERGE_GAP, -1L)); // Default - Always merge
    jiraEntity.setLabels(buildLabels(dimensionFilters));
    jiraEntity.setDescription(buildDescription(jiraTemplate, templateValues));
    jiraEntity.setComponents(ConfigUtils.getList(alertClientConfig.get(PROP_COMPONENTS)));
    jiraEntity.setSnapshot(buildSnapshot());

    return jiraEntity;
  }
}