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

package org.apache.pinot.thirdeye.detection.alert.scheme;

import com.atlassian.jira.rest.client.api.domain.input.IssueInput;
import com.atlassian.jira.rest.client.internal.async.AsynchronousJiraRestClientFactory;
import com.google.common.base.Preconditions;
import com.atlassian.jira.rest.client.api.JiraRestClient;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.detection.annotation.AlertScheme;
import org.apache.pinot.thirdeye.notification.commons.JiraConfiguration;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;
import org.apache.pinot.thirdeye.notification.formatter.channels.JiraContentFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.notification.commons.JiraConfiguration.*;


/**
 * This class is responsible for creating the jira alert tickets
 *
 * detector.yml
 * alertSchemes:
 * - type: JIRA
 *   params:
 *     project: THIRDEYE    # optional, default - create jira under THIRDEYE project
 *     issuetype: 19        # optional, default - create a jira TASK
 *     subject: METRICS     # optional, default - follows SubjectType.METRICS format
 *     assignee: user       # optional, default - unassigned
 *     labels:              # optional, default - thirdeye label is always appended
 *       - test-label-1
 *       - test-label-2
 */
@AlertScheme(type = "JIRA")
public class DetectionJiraAlerter extends DetectionAlertScheme {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionJiraAlerter.class);

  private ThirdEyeAnomalyConfiguration teConfig;
  private JiraRestClient jiraRestClient;
  private JiraConfiguration jiraAdminConfig;

  public static final String PROP_JIRA_SCHEME = "jiraScheme";

  public DetectionJiraAlerter(ADContentFormatterContext adContext, ThirdEyeAnomalyConfiguration thirdeyeConfig,
      DetectionAlertFilterResult result) throws Exception {
    super(adContext, result);
    this.teConfig = thirdeyeConfig;

    init();
  }

  private void init() {
    this.jiraAdminConfig = JiraConfiguration.createFromProperties(this.teConfig.getAlerterConfiguration().get(JIRA_CONFIG_KEY));
    this.jiraRestClient = new AsynchronousJiraRestClientFactory().createWithBasicHttpAuthentication(
        URI.create(jiraAdminConfig.getJiraHost()),
        jiraAdminConfig.getJiraUser(),
        jiraAdminConfig.getJiraPassword());
  }

  private String createIssue(IssueInput issueInput) {
    return jiraRestClient.getIssueClient().createIssue(issueInput).claim().getKey();
  }

  private void createJiraTickets(DetectionAlertFilterResult results) {
    LOG.info("Preparing a jira alert for subscription group id {}", this.adContext.getNotificationConfig().getId());
    Preconditions.checkNotNull(results.getResult());
    for (Map.Entry<DetectionAlertFilterNotification, Set<MergedAnomalyResultDTO>> result : results.getResult().entrySet()) {
      try {
        Map<String, Object> notificationSchemeProps = result.getKey().getNotificationSchemeProps();
        if (notificationSchemeProps == null || notificationSchemeProps.get(PROP_JIRA_SCHEME) == null) {
          throw new IllegalArgumentException("Invalid jira settings in subscription group " + this.adContext.getNotificationConfig().getId());
        }

        Properties jiraClientConfig = new Properties();
        jiraClientConfig.putAll(ConfigUtils.getMap(notificationSchemeProps.get(PROP_JIRA_SCHEME)));

        List<AnomalyResult> anomalyResultListOfGroup = new ArrayList<>(result.getValue());
        anomalyResultListOfGroup.sort(COMPARATOR_DESC);

        BaseNotificationContent content = super.buildNotificationContent(jiraClientConfig);
        IssueInput jiraIssueInput = new JiraContentFormatter(this.jiraAdminConfig, jiraClientConfig, content, this.teConfig, adContext)
            .getJiraEntity(anomalyResultListOfGroup);

        String issueKey = createIssue(jiraIssueInput);
        LOG.info("Jira created/updated with issue key {}", issueKey);
      } catch (IllegalArgumentException e) {
        LOG.warn("Skipping! Found illegal arguments while sending {} anomalies for alert {}."
            + " Exception message: ", result.getValue().size(), this.adContext.getNotificationConfig().getId(), e);
      }
    }
  }

  @Override
  public void run() throws Exception {
    Preconditions.checkNotNull(result);
    if (result.getAllAnomalies().size() == 0) {
      LOG.info("Zero anomalies found, skipping creation of jira alert for {}", this.adContext.getNotificationConfig().getId());
      return;
    }

    createJiraTickets(result);
  }
}
