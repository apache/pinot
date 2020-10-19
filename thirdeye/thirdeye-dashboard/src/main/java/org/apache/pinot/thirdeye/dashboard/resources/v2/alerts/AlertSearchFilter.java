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

package org.apache.pinot.thirdeye.dashboard.resources.v2.alerts;

import java.util.Collections;
import java.util.List;


/**
 * The type Alert search filter.
 */
public class AlertSearchFilter {
  private final List<String> applications;
  private final List<String> subscriptionGroups;
  private final List<String> createdBy;
  private final List<String> subscribedBy;
  private final List<String> ruleTypes;
  private final List<String> metrics;
  private final List<String> datasets;
  private final List<String> names;
  private final Boolean active;

  public AlertSearchFilter() {
    this.applications = Collections.emptyList();
    this.subscriptionGroups = Collections.emptyList();
    this.createdBy = Collections.emptyList();
    this.subscribedBy = Collections.emptyList();
    this.ruleTypes = Collections.emptyList();
    this.datasets = Collections.emptyList();
    this.metrics = Collections.emptyList();
    this.names = Collections.emptyList();
    this.active = null;
  }

  /**
   * Instantiates a new Alert search filter.
   *
   * @param applications the applications
   * @param subscriptionGroups the subscription groups
   * @param names the names
   * @param createdBy the createdBy
   * @param subscribedBy the subscribed by
   * @param ruleTypes the rule types
   * @param metrics the metrics
   * @param datasets the datasets
   * @param active the active
   */
  public AlertSearchFilter(List<String> applications, List<String> subscriptionGroups, List<String> names,
      List<String> createdBy, List<String> subscribedBy, List<String> ruleTypes, List<String> metrics, List<String> datasets, Boolean active) {
    this.applications = applications;
    this.subscriptionGroups = subscriptionGroups;
    this.names = names;
    this.createdBy = createdBy;
    this.subscribedBy = subscribedBy;
    this.ruleTypes = ruleTypes;
    this.metrics = metrics;
    this.datasets = datasets;
    this.active = active;
  }

  /**
   * Gets applications.
   *
   * @return the applications
   */
  public List<String> getApplications() {
    return applications;
  }

  /**
   * Gets subscription groups.
   *
   * @return the subscription groups
   */
  public List<String> getSubscriptionGroups() {
    return subscriptionGroups;
  }

  /**
   * Gets createdBy.
   *
   * @return the owners
   */
  public List<String> getCreatedBy() {
    return createdBy;
  }

  /**
   * Gets subscribedBy.
   *
   * @return the subscribedBy
   */
  public List<String> getSubscribedBy() {
    return subscribedBy;
  }

  /**
   * Gets rule types.
   *
   * @return the rule types
   */
  public List<String> getRuleTypes() {
    return ruleTypes;
  }

  /**
   * Gets metrics.
   *
   * @return the metrics
   */
  public List<String> getMetrics() {
    return metrics;
  }

  /**
   * Gets datasets.
   *
   * @return the datasets
   */
  public List<String> getDatasets() {
    return datasets;
  }

  /**
   * Gets names.
   *
   * @return the names
   */
  public List<String> getNames() {
    return names;
  }

  /**
   * Gets active.
   *
   * @return the active
   */
  public Boolean getActive() {
    return active;
  }

  /**
   * If all the search filters are empty.
   *
   * @return the boolean value of the result
   */
  public boolean isEmpty() {
    return this.applications.isEmpty() && this.subscriptionGroups.isEmpty() && this.names.isEmpty()
        && this.createdBy.isEmpty() && this.subscribedBy.isEmpty() && this.ruleTypes.isEmpty() && this.metrics.isEmpty() && this.datasets.isEmpty()
        && active == null;
  }
}
