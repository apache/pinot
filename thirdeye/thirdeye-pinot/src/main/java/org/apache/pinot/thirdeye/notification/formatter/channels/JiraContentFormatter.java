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

import com.atlassian.jira.rest.client.api.domain.input.IssueInput;
import com.atlassian.jira.rest.client.api.domain.input.IssueInputBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
import org.apache.commons.mail.HtmlEmail;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.notification.commons.JiraConfiguration;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.notification.content.templates.MetricAnomaliesContent;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class formats the content for jira alerts
 */
public class JiraContentFormatter extends AlertContentFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(JiraContentFormatter.class);

  private JiraConfiguration jiraAdminConfig;

  private static final String CHARSET = "UTF-8";
  static final String PROP_ISSUE_TYPE = "issuetype";
  static final String PROP_PROJECT = "project";
  static final String PROP_ASSIGNEE = "assignee";
  static final String PROP_LABELS = "labels";
  static final String PROP_SUMMARY = "summary";
  static final String PROP_DEFAULT_LABEL = "thirdeye";

  private static final Map<String, String> alertContentToTemplateMap;
  static {
    Map<String, String> aMap = new HashMap<>();
    aMap.put(MetricAnomaliesContent.class.getSimpleName(), "jira-metric-anomalies-template.ftl");
    alertContentToTemplateMap = Collections.unmodifiableMap(aMap);
  }

  public JiraContentFormatter(JiraConfiguration jiraAdminConfig, Properties jiraClientConfig,
      BaseNotificationContent content, ThirdEyeAnomalyConfiguration teConfig, ADContentFormatterContext adContext) {
    super(jiraClientConfig, content, teConfig, adContext);

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

  public IssueInput getJiraEntity(Collection<AnomalyResult> anomalies) {
    Map<String, Object> templateData = notificationContent.format(anomalies, adContext);
    templateData.put("dashboardHost", teConfig.getDashboardHost());
    return buildJiraEntity(alertContentToTemplateMap.get(notificationContent.getTemplate()), templateData);
  }

  /**
   * Apply the parameter map to given email template, and format it as EmailEntity
   */
  private IssueInput buildJiraEntity(String jiraTemplate, Map<String, Object> templateValues) {
    String issueSummary = BaseNotificationContent.makeSubject(getSubjectType(alertClientConfig), this.adContext.getNotificationConfig(), templateValues);

    // Fetch the jira project and issue type fields if overridden by user
    String jiraProject = MapUtils.getString(alertClientConfig, PROP_PROJECT, this.jiraAdminConfig.getJiraDefaultProjectKey());
    Long jiraIssueTypeId = MapUtils.getLong(alertClientConfig, PROP_ISSUE_TYPE, this.jiraAdminConfig.getJiraIssueTypeId());

    IssueInputBuilder issueBuilder = new IssueInputBuilder(jiraProject, jiraIssueTypeId, issueSummary);

    String assignee = MapUtils.getString(alertClientConfig, PROP_ASSIGNEE);
    if (StringUtils.isNotBlank(assignee)) {
      LOG.info("Assigning the jira to " + alertClientConfig.get(PROP_ASSIGNEE));
      issueBuilder.setAssigneeName(assignee);
    }

    List<String> labels = ConfigUtils.getList(alertClientConfig.get(PROP_LABELS));
    labels.add(PROP_DEFAULT_LABEL);
    issueBuilder.setFieldValue("labels", labels);

    HtmlEmail email = new HtmlEmail();
    String cid = "";
    try {
      if (StringUtils.isNotBlank(this.notificationContent.getSnaphotPath())) {
        cid = email.embed(new File(this.notificationContent.getSnaphotPath()));
      }
    } catch (Exception e) {
      LOG.error("Exception while embedding screenshot for anomaly", e);
    }
    templateValues.put("cid", cid);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/org/apache/pinot/thirdeye/detector");
      freemarkerConfig.setDefaultEncoding(CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Template template = freemarkerConfig.getTemplate(jiraTemplate);
      template.process(templateValues, out);

      String alertEmailHtml = new String(baos.toByteArray(), CHARSET);

      issueBuilder.setDescription(alertEmailHtml);
    } catch (Exception e) {
      Throwables.propagate(e);
    }

    return issueBuilder.build();
  }
}
