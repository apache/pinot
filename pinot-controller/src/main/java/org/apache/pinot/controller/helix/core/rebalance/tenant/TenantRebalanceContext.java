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
package org.apache.pinot.controller.helix.core.rebalance.tenant;

/**
 * Abstract class for tracking job configs and attempt numbers as part of the job ZK metadata to retry failed tenant rebalance.
 */
public abstract class TenantRebalanceContext {
  protected static final int INITIAL_ATTEMPT_ID = 1;
  private final String _jobId;
  private final String _originalJobId;
  private final TenantRebalanceConfig _config;
  private final int _attemptId;
  // Default to true for all user initiated rebalances, so that they can be retried if they fail or get stuck.
  private final boolean _allowRetries;

  protected TenantRebalanceContext(String originalJobId, TenantRebalanceConfig config, int attemptId, boolean allowRetries) {
    _jobId = createAttemptJobId(originalJobId, attemptId);
    _originalJobId = originalJobId;
    _config = config;
    _attemptId = attemptId;
    _allowRetries = allowRetries;
  }

  public int getAttemptId() {
    return _attemptId;
  }

  public String getOriginalJobId() {
    return _originalJobId;
  }

  public String getJobId() {
    return _jobId;
  }

  public TenantRebalanceConfig getConfig() {
    return _config;
  }

  public boolean getAllowRetries() {
    return _allowRetries;
  }

  @Override
  public String toString() {
    return "TenantRebalanceContext{" + "_jobId='" + _jobId + '\'' + ", _originalJobId='" + _originalJobId + '\''
        + ", _config=" + _config + ", _attemptId=" + _attemptId + ", _allowRetries=" + _allowRetries + "}";
  }

  private static String createAttemptJobId(String originalJobId, int attemptId) {
    if (attemptId == INITIAL_ATTEMPT_ID) {
      return originalJobId;
    }
    return originalJobId + "_" + attemptId;
  }
}
