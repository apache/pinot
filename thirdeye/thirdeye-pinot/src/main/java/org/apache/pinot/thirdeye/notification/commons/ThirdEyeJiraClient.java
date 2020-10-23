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

package org.apache.pinot.thirdeye.notification.commons;

import com.atlassian.jira.rest.client.api.GetCreateIssueMetadataOptionsBuilder;
import com.atlassian.jira.rest.client.api.JiraRestClient;
import com.atlassian.jira.rest.client.api.domain.BasicIssue;
import com.atlassian.jira.rest.client.api.domain.CimFieldInfo;
import com.atlassian.jira.rest.client.api.domain.CimIssueType;
import com.atlassian.jira.rest.client.api.domain.CimProject;
import com.atlassian.jira.rest.client.api.domain.Comment;
import com.atlassian.jira.rest.client.api.domain.Issue;
import com.atlassian.jira.rest.client.api.domain.Transition;
import com.atlassian.jira.rest.client.api.domain.input.ComplexIssueInputFieldValue;
import com.atlassian.jira.rest.client.api.domain.input.IssueInput;
import com.atlassian.jira.rest.client.api.domain.input.IssueInputBuilder;
import com.atlassian.jira.rest.client.api.domain.input.TransitionInput;
import com.atlassian.jira.rest.client.internal.async.AsynchronousJiraRestClientFactory;
import com.google.common.base.Joiner;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A client to communicate with Jira
 */
public class ThirdEyeJiraClient {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeJiraClient.class);

  private JiraRestClient restClient;
  private static final String JIRA_REOPEN_TRANSITION = "Reopen";
  public static final String PROP_ISSUE_TYPE = "issuetype";
  public static final String PROP_PROJECT = "project";
  public static final String PROP_ASSIGNEE = "assignee";
  public static final String PROP_CUSTOM = "custom";
  public static final String PROP_MERGE_GAP = "mergeGap";
  public static final String PROP_LABELS = "labels";
  public static final String PROP_COMPONENTS = "components";

  public ThirdEyeJiraClient(JiraConfiguration jiraAdminConfig) {
    this.restClient = createJiraRestClient(jiraAdminConfig);
  }

  private JiraRestClient createJiraRestClient(JiraConfiguration jiraAdminConfig) {
    return new AsynchronousJiraRestClientFactory().createWithBasicHttpAuthentication(
        URI.create(jiraAdminConfig.getJiraHost()),
        jiraAdminConfig.getJiraUser(),
        jiraAdminConfig.getJiraPassword());
  }

  private String buildQueryOnCreatedBy(long lookBackMillis) {
    String createdByQuery = "created";
    if (lookBackMillis < 0) {
      createdByQuery += "<= -0m";
    } else {
      createdByQuery += ">= -" + TimeUnit.MILLISECONDS.toDays(lookBackMillis) + "d";
    }

    return createdByQuery;
  }

  private String buildQueryOnLabels(List<String> labels) {
    return labels.stream()
        .map(label -> "labels = \"" + label + "\"")
        .collect(Collectors.joining(" and "));
  }

  /**
   * Search for all the existing jira tickets based on the filters
   */
  public List<Issue> getIssues(String project, List<String> labels, String reporter, long lookBackMillis) {
    List<Issue> issues = new ArrayList<>();

    StringBuilder jiraQuery = new StringBuilder();
    // Query by project first as a jira optimization
    jiraQuery.append("project=").append(project);
    jiraQuery.append(" and ").append("reporter IN (\"").append(reporter).append("\")");
    jiraQuery.append(" and ").append(buildQueryOnLabels(labels));
    jiraQuery.append(" and ").append(buildQueryOnCreatedBy(lookBackMillis));

    Iterable<Issue> jiraIssuesIt = restClient.getSearchClient().searchJql(jiraQuery.toString()).claim().getIssues();
    jiraIssuesIt.forEach(issues::add);

    LOG.info("Fetched {} Jira tickets using query - {}", issues.size(), jiraQuery.toString());
    return issues;
  }

  /**
   * Adds the specified comment to the jira ticket
   */
  public void addComment(Issue issue, String comment) {
    Comment resComment = Comment.valueOf(comment);
    LOG.info("Commenting Jira {} with new anomalies", issue.getKey());
    restClient.getIssueClient().addComment(issue.getCommentsUri(), resComment).claim();
  }

  /**
   * Attaches the specified file to the issue
   */
  public void addAttachment(BasicIssue basicIssue, File snapshot) {
    if (snapshot != null) {
      LOG.info("Attaching Jira {} with snapshot", basicIssue.getKey());
      Issue issue = restClient.getIssueClient().getIssue(basicIssue.getKey()).claim();
      restClient.getIssueClient().addAttachments(issue.getAttachmentsUri(), snapshot).claim();
    }
  }

  /**
   * Reopens an existing issue irrespective of its current state
   */
  public void reopenIssue(Issue issue) {
    Iterable<Transition> issueTransIt = restClient.getIssueClient().getTransitions(issue).claim();
    LOG.debug("Supported transitions for {} are {}", issue.getKey(), Joiner.on(",").join(issueTransIt));
    Optional<Transition> openTransition = StreamSupport
        .stream(issueTransIt.spliterator(), false)
        .filter(issueTrans -> issueTrans.getName().contains(JIRA_REOPEN_TRANSITION))
        .findFirst();

    if (openTransition.isPresent()) {
      TransitionInput trans = new TransitionInput(openTransition.get().getId());
      LOG.info("Reopening Jira {} with [status={}]. Previous state [status={}]", issue.getKey(),
          openTransition.get().getName(), issue.getStatus().getName());
      restClient.getIssueClient().transition(issue, trans).claim();
    } else {
      LOG.warn("Unable to reopen issue {}! Cannot find a '{}' transition. Verify permissions!", issue.getKey(),
          JIRA_REOPEN_TRANSITION);
    }
  }

  /**
   * Updates existing issue with assignee and labels
   */
  public void updateIssue(Issue issue, JiraEntity jiraEntity) {
    IssueInputBuilder issueBuilder = new IssueInputBuilder();
    setJiraAlertUpdatableFields(issueBuilder, jiraEntity);
    IssueInput issueInput = issueBuilder.build();

    LOG.info("Updating Jira {} with {}", issue.getKey(), issueInput.toString());
    restClient.getIssueClient().updateIssue(issue.getKey(), issueInput).claim();
    if (jiraEntity.getSnapshot() != null && jiraEntity.getSnapshot().exists()) {
      restClient.getIssueClient().addAttachments(issue.getAttachmentsUri(), jiraEntity.getSnapshot()).claim();
    }
  }

  Iterable<CimProject> getProjectMetadata(JiraEntity jiraEntity) {
    return restClient.getIssueClient().getCreateIssueMetadata(new GetCreateIssueMetadataOptionsBuilder()
        .withProjectKeys(jiraEntity.getJiraProject())
        .withIssueTypeIds(jiraEntity.getJiraIssueTypeId())
        .withExpandedIssueTypesFields()
        .build()).claim();
  }

  private Map<String, CimFieldInfo> getIssueRequiredCreateFields(JiraEntity jiraEntity) {
    Map<String, CimFieldInfo> requiredCreateFields = new HashMap<>();
    Iterator<CimProject> projectIt = getProjectMetadata(jiraEntity).iterator();
    if (projectIt.hasNext()) {
      CimProject project = projectIt.next();
      Iterator<CimIssueType> issueTypeIt = project.getIssueTypes().iterator();
      if (issueTypeIt.hasNext()) {
        CimIssueType issueType = issueTypeIt.next();
        for (Map.Entry<String, CimFieldInfo> issueField : issueType.getFields().entrySet()) {
          if (issueField.getValue().isRequired()) {
            requiredCreateFields.put(issueField.getKey(), issueField.getValue());
          }
        }
      }
    }

    LOG.debug("Required fields found in project {} issue {} are {}", jiraEntity.getJiraProject(),
        jiraEntity.getJiraIssueTypeId(), requiredCreateFields.keySet());
    return requiredCreateFields;
  }

  /**
   * Creates a new jira ticket with specified settings
   */
  public String createIssue(JiraEntity jiraEntity) {
    LOG.info("Creating Jira with user settings {}", jiraEntity.toString());
    BasicIssue basicIssue = restClient.getIssueClient().createIssue(buildIssue(jiraEntity)).claim();

    try {
      addAttachment(basicIssue, jiraEntity.getSnapshot());
    } catch (Exception e) {
      throw new RuntimeException("Jira alert fired successfully but failed to attach snapshot.", e);
    }

    return basicIssue.getKey();
  }

  /**
   * Set jira fields which should be updated when a new alert is fired
   */
  private void setJiraAlertUpdatableFields(IssueInputBuilder issueBuilder, JiraEntity jiraEntity) {
    issueBuilder.setAssigneeName(jiraEntity.getAssignee());
    issueBuilder.setFieldValue(PROP_LABELS, jiraEntity.getLabels());

    if (jiraEntity.getComponents() != null && !jiraEntity.getComponents().isEmpty()) {
      issueBuilder.setComponentsNames(jiraEntity.getComponents());
    }

    if (jiraEntity.getCustomFieldsMap() != null) {
      for (Map.Entry<String, Object> customFieldEntry : jiraEntity.getCustomFieldsMap().entrySet()) {
        issueBuilder.setFieldValue(
            customFieldEntry.getKey(),
            ComplexIssueInputFieldValue.with("name", customFieldEntry.getValue().toString()));
      }
    }
  }

  IssueInput buildIssue(JiraEntity jiraEntity) {
    IssueInputBuilder issueBuilder = new IssueInputBuilder();

    // For required/compulsory jira fields, we will automatically set a value from the "allowed values" list.
    // If there are no allowed values configured for that field in jira, required field won't be set by ThirdEye.
    for (Map.Entry<String, CimFieldInfo> reqFieldToInfoMap : getIssueRequiredCreateFields(jiraEntity).entrySet()) {
      Iterable<Object> allowedValues = reqFieldToInfoMap.getValue().getAllowedValues();
      if (allowedValues != null && allowedValues.iterator().hasNext()) {
        if (reqFieldToInfoMap.getValue().getSchema().getType().equals("array")) {
          issueBuilder.setFieldValue(reqFieldToInfoMap.getKey(),
              Collections.singletonList(allowedValues.iterator().next()));
        } else {
          issueBuilder.setFieldValue(reqFieldToInfoMap.getKey(), allowedValues.iterator().next());
        }
      }
    }

    issueBuilder.setProjectKey(jiraEntity.getJiraProject());
    issueBuilder.setSummary(jiraEntity.getSummary());
    issueBuilder.setIssueTypeId(jiraEntity.getJiraIssueTypeId());
    issueBuilder.setDescription(jiraEntity.getDescription());

    setJiraAlertUpdatableFields(issueBuilder, jiraEntity);

    return issueBuilder.build();
  }

  public void close() {
    try {
      restClient.close();
    } catch (IOException e) {
      LOG.warn("Failed to close jira rest client", e);
    }
  }
}
