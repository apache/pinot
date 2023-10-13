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

import com.google.common.annotations.VisibleForTesting;


/**
 * Track the job configs and attempt numbers as part of the job ZK metadata for future retry.
 */
public class TableRebalanceAttemptContext {
  private static final int INITIAL_ATTEMPT_ID = 1;
  private String _jobId;
  private String _originalJobId;
  private RebalanceConfig _config;
  private int _attemptId;

  public static TableRebalanceAttemptContext forInitialAttempt(String originalJobId, RebalanceConfig config) {
    return new TableRebalanceAttemptContext(originalJobId, config, INITIAL_ATTEMPT_ID);
  }

  public static TableRebalanceAttemptContext forRetry(String originalJobId, RebalanceConfig config, int attemptId) {
    return new TableRebalanceAttemptContext(originalJobId, config, attemptId);
  }

  public TableRebalanceAttemptContext() {
    // For JSON deserialization.
  }

  private TableRebalanceAttemptContext(String originalJobId, RebalanceConfig config, int attemptId) {
    _jobId = createAttemptJobId(originalJobId, attemptId);
    _originalJobId = originalJobId;
    _config = config;
    _attemptId = attemptId;
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

  @Override
  public String toString() {
    return "TableRebalanceAttemptContext{" + "_jobId='" + _jobId + '\'' + ", _originalJobId='" + _originalJobId + '\''
        + ", _config=" + _config + ", _attemptId=" + _attemptId + '}';
  }

  @VisibleForTesting
  static String createAttemptJobId(String originalJobId, int attemptId) {
    if (attemptId == INITIAL_ATTEMPT_ID) {
      return originalJobId;
    }
    return originalJobId + "_" + attemptId;
  }
}
