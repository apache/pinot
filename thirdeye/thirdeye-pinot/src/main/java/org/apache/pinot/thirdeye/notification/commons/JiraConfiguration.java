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

import com.google.common.base.MoreObjects;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections4.MapUtils;


public class JiraConfiguration {

  public static final String JIRA_CONFIG_KEY = "jiraConfiguration";
  public static final String JIRA_URL_KEY = "jiraUrl";
  public static final String JIRA_USER_KEY = "jiraUser";
  public static final String JIRA_PASSWD_KEY = "jiraPassword";
  public static final String JIRA_DEFAULT_PROJECT_KEY = "jiraDefaultProject";
  public static final String JIRA_ISSUE_TYPE_KEY = "jiraIssueTypeId";

  private String jiraUrl;
  private String jiraUser;
  private String jiraPassword;
  private String jiraDefaultProject;
  private Long jiraIssueTypeId;

  public String getJiraHost() {
    return jiraUrl;
  }

  public void setJiraHost(String jiraHost) {
    this.jiraUrl = jiraHost;
  }

  public String getJiraUser() {
    return jiraUser;
  }

  public void setJiraUser(String jiraUser) {
    this.jiraUser = jiraUser;
  }

  public String getJiraPassword() {
    return jiraPassword;
  }

  public void setJiraPassword(String jiraPassword) {
    this.jiraPassword = jiraPassword;
  }

  public void setJiraDefaultProjectKey(String jiraDefaultProject) {
    this.jiraDefaultProject = jiraDefaultProject;
  }

  public String getJiraDefaultProjectKey() {
    return jiraDefaultProject;
  }

  public void setJiraIssueTypeId(Long jiraIssueTypeId) {
    this.jiraIssueTypeId = jiraIssueTypeId;
  }

  public Long getJiraIssueTypeId() {
    return jiraIssueTypeId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof JiraConfiguration)) {
      return false;
    }
    JiraConfiguration at = (JiraConfiguration) o;
    return Objects.equals(jiraUrl, at.getJiraHost())
        && Objects.equals(jiraUser, at.getJiraUser())
        && Objects.equals(jiraPassword, at.getJiraPassword())
        && Objects.equals(jiraDefaultProject, at.getJiraDefaultProjectKey())
        && Objects.equals(jiraIssueTypeId, at.getJiraIssueTypeId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(jiraUrl, jiraUser, jiraPassword, jiraDefaultProject);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add(JIRA_URL_KEY, jiraUrl).add(JIRA_USER_KEY, jiraUser).toString();
  }

  public static JiraConfiguration createFromProperties(Map<String,Object> jiraConfiguration) {
    JiraConfiguration conf = new JiraConfiguration();
    try {
      conf.setJiraHost(MapUtils.getString(jiraConfiguration, JIRA_URL_KEY));
      conf.setJiraUser(MapUtils.getString(jiraConfiguration, JIRA_USER_KEY));
      conf.setJiraPassword(MapUtils.getString(jiraConfiguration, JIRA_PASSWD_KEY));
      conf.setJiraDefaultProjectKey(MapUtils.getString(jiraConfiguration, JIRA_DEFAULT_PROJECT_KEY, "THIRDEYE"));
      conf.setJiraIssueTypeId(MapUtils.getLong(jiraConfiguration, JIRA_ISSUE_TYPE_KEY, 19L));
    } catch (Exception e) {
      throw new RuntimeException("Error occurred while parsing jira configuration into object.", e);
    }
    return conf;
  }
}
