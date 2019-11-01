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

import com.atlassian.jira.rest.client.api.JiraRestClient;
import com.atlassian.jira.rest.client.api.domain.Comment;
import com.atlassian.jira.rest.client.api.domain.Issue;
import com.atlassian.jira.rest.client.api.domain.IssueFieldId;
import com.atlassian.jira.rest.client.api.domain.Transition;
import com.atlassian.jira.rest.client.api.domain.input.FieldInput;
import com.atlassian.jira.rest.client.api.domain.input.IssueInput;
import com.atlassian.jira.rest.client.api.domain.input.IssueInputBuilder;
import com.atlassian.jira.rest.client.api.domain.input.TransitionInput;
import com.atlassian.jira.rest.client.internal.async.AsynchronousJiraRestClientFactory;
import com.google.common.base.Joiner;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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

  public ThirdEyeJiraClient(JiraConfiguration jiraAdminConfig) {
    this.restClient = createJiraRestClient(jiraAdminConfig);
  }

  private JiraRestClient createJiraRestClient(JiraConfiguration jiraAdminConfig) {
    return new AsynchronousJiraRestClientFactory().createWithBasicHttpAuthentication(
        URI.create(jiraAdminConfig.getJiraHost()),
        jiraAdminConfig.getJiraUser(),
        jiraAdminConfig.getJiraPassword());
  }

  /**
   * Search for all the existing jira tickets based on the filters
   */
  public List<Issue> getIssues(String project, List<String> labels, String reporter, long lookBackMillis) {
    List<Issue> issues = new ArrayList<>();
    long lookBackDays = TimeUnit.MILLISECONDS.toDays(lookBackMillis);
    String andQueryOnLabels = labels.stream()
        .map(label -> "labels = \"" + label + "\"")
        .collect(Collectors.joining(" and "));

    StringBuilder jiraQuery = new StringBuilder();
    jiraQuery.append("project=").append(project);
    jiraQuery.append(" and ").append("reporter IN (").append(reporter).append(")");
    jiraQuery.append(" and ").append(andQueryOnLabels);
    jiraQuery.append(" and ").append("created>=").append("-").append(lookBackDays).append("d");

    LOG.info("Fetching Jira tickets using query - {}", jiraQuery.toString());
    Iterable<Issue> jiraIssuesIt = restClient.getSearchClient().searchJql(jiraQuery.toString()).claim().getIssues();
    jiraIssuesIt.forEach(issues::add);
    return issues;
  }

  /**
   * Adds the specified comment to the jira ticket
   */
  public void addComment(Issue issue, String comment) {
    // TODO: handle comments longer than X characters!
    Comment resComment = Comment.valueOf(comment);
    LOG.info("Commenting Jira {} with new anomalies", issue.getKey());
    restClient.getIssueClient().addComment(issue.getCommentsUri(), resComment).claim();
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
    IssueInput input = new IssueInputBuilder()
        .setAssigneeName(jiraEntity.getAssignee())
        .setFieldInput(new FieldInput(IssueFieldId.LABELS_FIELD, jiraEntity.getLabels()))
        .build();
    String prevAssignee = issue.getAssignee() == null ? "unassigned" : issue.getAssignee().getName();

    LOG.info("Updating Jira {} with [assignee={}, labels={}]. Previous state [assignee={}, labels={}]", issue.getKey(),
        jiraEntity.getAssignee(), jiraEntity.getLabels(), prevAssignee, issue.getLabels());
    restClient.getIssueClient().updateIssue(issue.getKey(), input).claim();
  }

  /**
   * Creates a new jira ticket with specified settings
   */
  public String createIssue(JiraEntity jiraEntity) {
    IssueInputBuilder issueBuilder = new IssueInputBuilder(
        jiraEntity.getJiraProject(), jiraEntity.getJiraIssueTypeId(), jiraEntity.getSummary());
    issueBuilder.setAssigneeName(jiraEntity.getAssignee());
    issueBuilder.setFieldInput(new FieldInput(IssueFieldId.LABELS_FIELD, jiraEntity.getLabels()));
    issueBuilder.setDescription(jiraEntity.getDescription());

    LOG.info("Creating Jira with settings {}", jiraEntity.toString());
    return restClient.getIssueClient().createIssue(issueBuilder.build()).claim().getKey();
  }
}
