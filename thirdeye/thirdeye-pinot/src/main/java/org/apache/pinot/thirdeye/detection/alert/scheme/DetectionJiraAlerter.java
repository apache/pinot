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

import com.atlassian.jira.rest.client.api.domain.Issue;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyResult;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterNotification;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.detection.annotation.AlertScheme;
import org.apache.pinot.thirdeye.notification.commons.JiraConfiguration;
import org.apache.pinot.thirdeye.notification.commons.JiraEntity;
import org.apache.pinot.thirdeye.notification.commons.ThirdEyeJiraClient;
import org.apache.pinot.thirdeye.notification.content.BaseNotificationContent;
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
 *     custom:
 *       test1: value1
 *       test2: value2
 */
@AlertScheme(type = "JIRA")
public class DetectionJiraAlerter extends DetectionAlertScheme {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionJiraAlerter.class);

  private ThirdEyeAnomalyConfiguration teConfig;
  private ThirdEyeJiraClient jiraClient;
  private JiraConfiguration jiraAdminConfig;

  public static final String PROP_JIRA_SCHEME = "jiraScheme";
  public static final int JIRA_DESCRIPTION_MAX_LENGTH = 100000;
  public static final int JIRA_ONE_LINE_COMMENT_LENGTH = 250;

  public DetectionJiraAlerter(DetectionAlertConfigDTO subsConfig, ThirdEyeAnomalyConfiguration thirdeyeConfig,
      DetectionAlertFilterResult result, ThirdEyeJiraClient jiraClient) {
    super(subsConfig, result);
    this.teConfig = thirdeyeConfig;

    this.jiraAdminConfig = JiraConfiguration.createFromProperties(this.teConfig.getAlerterConfiguration().get(JIRA_CONFIG_KEY));
    this.jiraClient = jiraClient;
  }

  public DetectionJiraAlerter(DetectionAlertConfigDTO subsConfig, ThirdEyeAnomalyConfiguration thirdeyeConfig,
      DetectionAlertFilterResult result) throws Exception {
    this(subsConfig, thirdeyeConfig, result, new ThirdEyeJiraClient(JiraConfiguration
        .createFromProperties(thirdeyeConfig.getAlerterConfiguration().get(JIRA_CONFIG_KEY))));
  }

  private void updateJiraAlert(Issue issue, JiraEntity jiraEntity) {
    // Append labels - do not remove existing labels
    jiraEntity.getLabels().addAll(issue.getLabels());
    jiraEntity.setLabels(jiraEntity.getLabels().stream().distinct().collect(Collectors.toList()));

    jiraClient.reopenIssue(issue);
    jiraClient.updateIssue(issue, jiraEntity);

    try {
      // Safeguard check from ThirdEye side
      if (jiraEntity.getDescription().length() > JIRA_DESCRIPTION_MAX_LENGTH) {
        throw new RuntimeException("Exceeded jira description character limit of {}" + JIRA_DESCRIPTION_MAX_LENGTH);
      }

      jiraClient.addComment(issue, jiraEntity.getDescription());
    } catch (Exception e) {
      // Jira has a upper limit on the number of characters in description. In such cases we will only
      // share a link in the comment.
      StringBuilder sb = new StringBuilder();
      sb.append("*<Truncating details due to jira limit! Please use the below link to view all the anomalies.>*");
      sb.append(System.getProperty("line.separator"));

      // Print only the first line with the redirection link to ThirdEye
      String desc = jiraEntity.getDescription();
      int newLineIndex = desc.indexOf("\n");
      if (newLineIndex < 0 || newLineIndex > JIRA_ONE_LINE_COMMENT_LENGTH) {
        sb.append(desc, 0, JIRA_ONE_LINE_COMMENT_LENGTH);
        sb.append("...");
      } else {
        sb.append(desc, 0, newLineIndex);
      }

      jiraClient.addComment(issue, sb.toString());
    }
  }

  private JiraEntity buildJiraEntity(DetectionAlertFilterNotification notification, Set<MergedAnomalyResultDTO> anomalies) {
    DetectionAlertConfigDTO subsetSubsConfig = notification.getSubscriptionConfig();
    if (subsetSubsConfig.getAlertSchemes().get(PROP_JIRA_SCHEME) == null) {
      throw new IllegalArgumentException("Jira not configured in subscription group " + this.subsConfig.getId());
    }

    Properties jiraClientConfig = new Properties();
    jiraClientConfig.putAll(ConfigUtils.getMap(subsetSubsConfig.getAlertSchemes().get(PROP_JIRA_SCHEME)));

    List<AnomalyResult> anomalyResultListOfGroup = new ArrayList<>(anomalies);
    anomalyResultListOfGroup.sort(COMPARATOR_DESC);

    BaseNotificationContent content = getNotificationContent(jiraClientConfig);

    return new JiraContentFormatter(this.jiraAdminConfig, jiraClientConfig, content, this.teConfig, subsetSubsConfig)
        .getJiraEntity(notification.getDimensionFilters(), anomalyResultListOfGroup);
  }

  private void createJiraTickets(DetectionAlertFilterResult results) throws Exception {
    LOG.info("Preparing a jira alert for subscription group id {}", this.subsConfig.getId());
    Preconditions.checkNotNull(results.getResult());
    for (Map.Entry<DetectionAlertFilterNotification, Set<MergedAnomalyResultDTO>> result : results.getResult().entrySet()) {
      try {
        JiraEntity jiraEntity = buildJiraEntity(result.getKey(), result.getValue());

        // Fetch the most recent reported issues within mergeGap by jira service account under the project
        List<Issue> issues = jiraClient.getIssues(jiraEntity.getJiraProject(), jiraEntity.getLabels(),
            this.jiraAdminConfig.getJiraUser(), jiraEntity.getMergeGap());
        Optional<Issue> latestJiraIssue = issues.stream().max(
              (o1, o2) -> o2.getCreationDate().compareTo(o1.getCreationDate()));

        if (!latestJiraIssue.isPresent()) {
          // No existing ticket found. Create a new jira ticket
          String issueKey = jiraClient.createIssue(jiraEntity);
          ThirdeyeMetricsUtil.jiraAlertsSuccessCounter.inc();
          ThirdeyeMetricsUtil.jiraAlertsNumTicketsCounter.inc();
          LOG.info("Jira created {}, anomalies reported {}", issueKey, result.getValue().size());
        } else {
          // Reopen recent existing ticket and add a comment
          updateJiraAlert(latestJiraIssue.get(), jiraEntity);
          ThirdeyeMetricsUtil.jiraAlertsSuccessCounter.inc();
          ThirdeyeMetricsUtil.jiraAlertsNumCommentsCounter.inc();
          LOG.info("Jira updated {}, anomalies reported = {}", latestJiraIssue.get().getKey(), result.getValue().size());
        }
      } catch (Exception e) {
        ThirdeyeMetricsUtil.jiraAlertsFailedCounter.inc();
        super.handleAlertFailure(result.getValue().size(), e);
      }
    }
  }

  @Override
  public void destroy() {
    this.jiraClient.close();
  }

  @Override
  public void run() throws Exception {
    Preconditions.checkNotNull(result);
    if (result.getAllAnomalies().size() == 0) {
      LOG.info("Zero anomalies found, skipping creation of jira alert for {}", this.subsConfig.getId());
      return;
    }

    createJiraTickets(result);
  }
}
