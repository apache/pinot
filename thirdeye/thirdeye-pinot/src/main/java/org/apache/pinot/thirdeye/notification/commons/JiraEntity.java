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

import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;


/**
 * ThirdEye's Jira Settings Holder
 */
public class JiraEntity {
  private String jiraProject;
  private Long jiraIssueTypeId;
  private String summary;
  private String description;
  private String snapshot;
  private String assignee;
  private List<String> labels;
  private long mergeGap;

  public JiraEntity(String jiraProject, Long jiraIssueTypeId, String issueSummary) {
    this.jiraProject = jiraProject;
    this.jiraIssueTypeId = jiraIssueTypeId;
    this.summary = issueSummary;
  }

  public String getJiraProject() {
    return jiraProject;
  }

  public void setJiraProject(String jiraProject) {
    this.jiraProject = jiraProject;
  }

  public Long getJiraIssueTypeId() {
    return jiraIssueTypeId;
  }

  public void setJiraIssueTypeId(Long jiraIssueTypeId) {
    this.jiraIssueTypeId = jiraIssueTypeId;
  }

  public String getSummary() {
    return summary;
  }

  public void setSummary(String summary) {
    this.summary = summary;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getSnapshot() {
    return snapshot;
  }

  public void setSnapshot(String snapshot) {
    this.snapshot = snapshot;
  }

  public String getAssignee() {
    return assignee;
  }

  public void setAssignee(String assignee) {
    this.assignee = assignee;
  }

  public long getMergeGap() {
    return mergeGap;
  }

  public void setMergeGap(long mergeGap) {
    this.mergeGap = mergeGap;
  }

  public List<String> getLabels() {
    return labels;
  }

  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Jira{");
    sb.append("project='").append(jiraProject).append('\'');
    sb.append(", type='").append(jiraIssueTypeId).append('\'');
    sb.append(", assignee='").append(assignee).append('\'');
    sb.append(", summary='").append(summary).append('\'');
    sb.append(", labels='").append(labels).append('\'');
    sb.append(", mergeGap='").append(mergeGap).append('\'');
    sb.append(", snapshot='").append(StringUtils.isBlank(snapshot) ? "no-snapshot" : "has-snapshot").append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(jiraProject, jiraIssueTypeId, assignee, summary, labels, mergeGap, snapshot);
  }
}
