/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.tracking;

import java.util.Map;


/**
 * Statistics container for data source request performance log
 */
public final class RequestStatistics {
  Map<String, Long> requestsPerDatasource;
  Map<String, Long> requestsPerDataset;
  Map<String, Long> requestsPerMetric;
  Map<String, Long> requestsPerPrincipal;
  long requestsTotal;

  Map<String, Long> successPerDatasource;
  Map<String, Long> successPerDataset;
  Map<String, Long> successPerMetric;
  Map<String, Long> successPerPrincipal;
  long successTotal;

  Map<String, Long> failurePerDatasource;
  Map<String, Long> failurePerDataset;
  Map<String, Long> failurePerMetric;
  Map<String, Long> failurePerPrincipal;
  long failureTotal;

  Map<String, Long> durationPerDatasource;
  Map<String, Long> durationPerDataset;
  Map<String, Long> durationPerMetric;
  Map<String, Long> durationPerPrincipal;
  long durationTotal;

  public Map<String, Long> getRequestsPerDatasource() {
    return requestsPerDatasource;
  }

  public void setRequestsPerDatasource(Map<String, Long> requestsPerDatasource) {
    this.requestsPerDatasource = requestsPerDatasource;
  }

  public Map<String, Long> getRequestsPerDataset() {
    return requestsPerDataset;
  }

  public void setRequestsPerDataset(Map<String, Long> requestsPerDataset) {
    this.requestsPerDataset = requestsPerDataset;
  }

  public Map<String, Long> getRequestsPerMetric() {
    return requestsPerMetric;
  }

  public void setRequestsPerMetric(Map<String, Long> requestsPerMetric) {
    this.requestsPerMetric = requestsPerMetric;
  }

  public Map<String, Long> getRequestsPerPrincipal() {
    return requestsPerPrincipal;
  }

  public void setRequestsPerPrincipal(Map<String, Long> requestsPerPrincipal) {
    this.requestsPerPrincipal = requestsPerPrincipal;
  }

  public long getRequestsTotal() {
    return requestsTotal;
  }

  public void setRequestsTotal(long requestsTotal) {
    this.requestsTotal = requestsTotal;
  }

  public Map<String, Long> getSuccessPerDatasource() {
    return successPerDatasource;
  }

  public void setSuccessPerDatasource(Map<String, Long> successPerDatasource) {
    this.successPerDatasource = successPerDatasource;
  }

  public Map<String, Long> getSuccessPerDataset() {
    return successPerDataset;
  }

  public void setSuccessPerDataset(Map<String, Long> successPerDataset) {
    this.successPerDataset = successPerDataset;
  }

  public Map<String, Long> getSuccessPerMetric() {
    return successPerMetric;
  }

  public void setSuccessPerMetric(Map<String, Long> successPerMetric) {
    this.successPerMetric = successPerMetric;
  }

  public Map<String, Long> getSuccessPerPrincipal() {
    return successPerPrincipal;
  }

  public void setSuccessPerPrincipal(Map<String, Long> successPerPrincipal) {
    this.successPerPrincipal = successPerPrincipal;
  }

  public long getSuccessTotal() {
    return successTotal;
  }

  public void setSuccessTotal(long successTotal) {
    this.successTotal = successTotal;
  }

  public Map<String, Long> getFailurePerDatasource() {
    return failurePerDatasource;
  }

  public void setFailurePerDatasource(Map<String, Long> failurePerDatasource) {
    this.failurePerDatasource = failurePerDatasource;
  }

  public Map<String, Long> getFailurePerDataset() {
    return failurePerDataset;
  }

  public void setFailurePerDataset(Map<String, Long> failurePerDataset) {
    this.failurePerDataset = failurePerDataset;
  }

  public Map<String, Long> getFailurePerMetric() {
    return failurePerMetric;
  }

  public void setFailurePerMetric(Map<String, Long> failurePerMetric) {
    this.failurePerMetric = failurePerMetric;
  }

  public Map<String, Long> getFailurePerPrincipal() {
    return failurePerPrincipal;
  }

  public void setFailurePerPrincipal(Map<String, Long> failurePerPrincipal) {
    this.failurePerPrincipal = failurePerPrincipal;
  }

  public long getFailureTotal() {
    return failureTotal;
  }

  public void setFailureTotal(long failureTotal) {
    this.failureTotal = failureTotal;
  }

  public Map<String, Long> getDurationPerDatasource() {
    return durationPerDatasource;
  }

  public void setDurationPerDatasource(Map<String, Long> durationPerDatasource) {
    this.durationPerDatasource = durationPerDatasource;
  }

  public Map<String, Long> getDurationPerDataset() {
    return durationPerDataset;
  }

  public void setDurationPerDataset(Map<String, Long> durationPerDataset) {
    this.durationPerDataset = durationPerDataset;
  }

  public Map<String, Long> getDurationPerMetric() {
    return durationPerMetric;
  }

  public void setDurationPerMetric(Map<String, Long> durationPerMetric) {
    this.durationPerMetric = durationPerMetric;
  }

  public Map<String, Long> getDurationPerPrincipal() {
    return durationPerPrincipal;
  }

  public void setDurationPerPrincipal(Map<String, Long> durationPerPrincipal) {
    this.durationPerPrincipal = durationPerPrincipal;
  }

  public long getDurationTotal() {
    return durationTotal;
  }

  public void setDurationTotal(long durationTotal) {
    this.durationTotal = durationTotal;
  }
}
