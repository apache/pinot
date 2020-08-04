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

package org.apache.pinot.thirdeye.dashboard.resources.v2.anomalies;

import java.util.Collections;
import java.util.List;


/**
 * The type Anomaly search filter.
 */
public class AnomalySearchFilter {
  private final List<String> feedbacks;
  private final List<String> subscriptionGroups;
  private final List<String> detectionNames;
  private final List<String> metrics;
  private final List<String> datasets;
  private final List<Long> anomalyIds;
  private final Long startTime;
  private final Long endTime;

  /**
   * Instantiates a new Anomaly search filter.
   *
   * @param startTime the start time
   * @param endTime the end time
   */
  public AnomalySearchFilter(Long startTime, Long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.feedbacks = Collections.emptyList();
    this.subscriptionGroups = Collections.emptyList();
    this.detectionNames = Collections.emptyList();
    this.metrics = Collections.emptyList();
    this.datasets = Collections.emptyList();
    this.anomalyIds = Collections.emptyList();
  }

  /**
   * Instantiates a new Anomaly search filter.
   *
   * @param startTime the start time
   * @param endTime the end time
   * @param feedbacks the feedbacks
   * @param subscriptionGroups the subscription groups
   * @param detectionNames the detection names
   * @param metrics the metrics
   * @param datasets the datasets
   * @param anomalyIds the anomaly ids
   */
  public AnomalySearchFilter(Long startTime, Long endTime, List<String> feedbacks, List<String> subscriptionGroups, List<String> detectionNames,
      List<String> metrics, List<String> datasets, List<Long> anomalyIds) {
    this.feedbacks = feedbacks;
    this.subscriptionGroups = subscriptionGroups;
    this.detectionNames = detectionNames;
    this.metrics = metrics;
    this.datasets = datasets;
    this.startTime = startTime;
    this.endTime = endTime;
    this.anomalyIds = anomalyIds;
  }

  /**
   * Gets start time.
   *
   * @return the start time
   */
  public Long getStartTime() {
    return startTime;
  }

  /**
   * Gets end time.
   *
   * @return the end time
   */
  public Long getEndTime() {
    return endTime;
  }

  /**
   * Gets feedbacks.
   *
   * @return the feedbacks
   */
  public List<String> getFeedbacks() {
    return feedbacks;
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
   * Gets detection names.
   *
   * @return the detection names
   */
  public List<String> getDetectionNames() {
    return detectionNames;
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
   * Gets anomaly ids.
   *
   * @return the anomaly ids
   */
  public List<Long> getAnomalyIds() {
    return anomalyIds;
  }
}
