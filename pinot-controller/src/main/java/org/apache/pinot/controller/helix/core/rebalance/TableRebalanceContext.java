/**
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
package org.apache.pinot.controller.helix.core.rebalance;

/**
 * Track the job configs and attempt numbers as part of the job ZK metadata to retry failed rebalance.
 */
public class TableRebalanceContext {
  private static final int INITIAL_ATTEMPT_ID = 1;
  private String _jobId;
  private String _originalJobId;
  private RebalanceConfig _config;
  private int _attemptId;
  // Default to true for all user initiated rebalances, so that they can be retried if they fail or get stuck.
  private boolean _allowRetries = true;

  /**
   * Creates a new TableRebalanceContext for the initial attempt of a rebalance job.
   *
   * @param originalJobId The original job ID for the rebalance job.
   * @param config The rebalance configuration.
   * @param allowRetries Whether retries are allowed for this rebalance job. This isn't part of {@link RebalanceConfig}
   *                     because user initiated rebalances should always support retries for failed and stuck jobs.
   * @return A new TableRebalanceContext instance.
   */
  public static TableRebalanceContext forInitialAttempt(String originalJobId, RebalanceConfig config,
      boolean allowRetries) {
    return new TableRebalanceContext(originalJobId, config, INITIAL_ATTEMPT_ID, allowRetries);
  }

  /**
   * Creates a new TableRebalanceContext for a retry attempt of a rebalance job.
   *
   * @param originalJobId The original job ID for the rebalance job.
   * @param config The rebalance configuration.
   * @param attemptId The attempt ID for the retry.
   * @return A new TableRebalanceContext instance.
   */
  public static TableRebalanceContext forRetry(String originalJobId, RebalanceConfig config, int attemptId) {
    return new TableRebalanceContext(originalJobId, config, attemptId, true);
  }

  public TableRebalanceContext() {
    // For JSON deserialization.
  }

  private TableRebalanceContext(String originalJobId, RebalanceConfig config, int attemptId, boolean allowRetries) {
    _jobId = createAttemptJobId(originalJobId, attemptId);
    _originalJobId = originalJobId;
    _config = config;
    _attemptId = attemptId;
    _allowRetries = allowRetries;
  }

  public int getAttemptId() {
    return _attemptId;
  }

  public void setAttemptId(int attemptId) {
    _attemptId = attemptId;
  }

  public String getOriginalJobId() {
    return _originalJobId;
  }

  public void setOriginalJobId(String originalJobId) {
    _originalJobId = originalJobId;
  }

  public String getJobId() {
    return _jobId;
  }

  public void setJobId(String jobId) {
    _jobId = jobId;
  }

  public RebalanceConfig getConfig() {
    return _config;
  }

  public void setConfig(RebalanceConfig config) {
    _config = config;
  }

  public boolean getAllowRetries() {
    return _allowRetries;
  }

  public void setAllowRetries(boolean allowRetries) {
    _allowRetries = allowRetries;
  }

  @Override
  public String toString() {
    return "TableRebalanceContext{" + "_jobId='" + _jobId + '\'' + ", _originalJobId='" + _originalJobId + '\''
        + ", _config=" + _config + ", _attemptId=" + _attemptId + ", _allowRetries=" + _allowRetries + "}";
  }

  private static String createAttemptJobId(String originalJobId, int attemptId) {
    if (attemptId == INITIAL_ATTEMPT_ID) {
      return originalJobId;
    }
    return originalJobId + "_" + attemptId;
  }
}
