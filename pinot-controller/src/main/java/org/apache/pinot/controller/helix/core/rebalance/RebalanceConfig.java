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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;


@ApiModel
public class RebalanceConfig {
  public static final int DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME = 1;
  public static final long DEFAULT_EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS = 1000L; // 1 second
  public static final long DEFAULT_EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS = 3600000L; // 1 hour

  // Whether to rebalance table in dry-run mode
  @JsonProperty("dryRun")
  @ApiModelProperty(example = "false")
  private boolean _dryRun = false;

  // Whether to reassign instances before reassigning segments
  @JsonProperty("reassignInstances")
  @ApiModelProperty(example = "false")
  private boolean _reassignInstances = false;

  // Whether to reassign CONSUMING segments
  @JsonProperty("includeConsuming")
  @ApiModelProperty(example = "false")
  private boolean _includeConsuming = false;

  // Whether to rebalance table in bootstrap mode (regardless of minimum segment movement, reassign all segments in a
  // round-robin fashion as if adding new segments to an empty table)
  @JsonProperty("bootstrap")
  @ApiModelProperty(example = "false")
  private boolean _bootstrap = false;

  // Whether to allow downtime for the rebalance
  @JsonProperty("downtime")
  @ApiModelProperty(example = "false")
  private boolean _downtime = false;

  // For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or maximum number of replicas
  // allowed to be unavailable if value is negative
  @JsonProperty("minAvailableReplicas")
  @ApiModelProperty(example = "1")
  private int _minAvailableReplicas = DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME;

  // Whether to use best-efforts to rebalance (not fail the rebalance when the no-downtime contract cannot be achieved)
  // When using best-efforts to rebalance, the following scenarios won't fail the rebalance (will log warnings instead):
  // - Segment falls into ERROR state in ExternalView -> count ERROR state as good state
  // - ExternalView has not converged within the maximum wait time -> continue to the next stage
  @JsonProperty("bestEfforts")
  @ApiModelProperty(example = "false")
  private boolean _bestEfforts = false;

  // The check on external view can be very costly when the table has very large ideal and external states, i.e. when
  // having a huge number of segments. These two configs help reduce the cpu load on controllers, e.g. by doing the
  // check less frequently and bail out sooner to rebalance at best effort if configured so.
  @JsonProperty("externalViewCheckIntervalInMs")
  @ApiModelProperty(example = "1000")
  private long _externalViewCheckIntervalInMs = DEFAULT_EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS;

  @JsonProperty("externalViewStabilizationTimeoutInMs")
  @ApiModelProperty(example = "3600000")
  private long _externalViewStabilizationTimeoutInMs = DEFAULT_EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS;

  @JsonProperty("updateTargetTier")
  @ApiModelProperty(example = "false")
  private boolean _updateTargetTier = false;

  // Update job status every this interval as heartbeat, to indicate the job is still actively running.
  @JsonProperty("heartbeatIntervalInMs")
  @ApiModelProperty(example = "300000")
  private long _heartbeatIntervalInMs = 300000L;

  // The job is considered as failed if not updating its status by this timeout, even though it's IN_PROGRESS status.
  @JsonProperty("heartbeatTimeoutInMs")
  @ApiModelProperty(example = "3600000")
  private long _heartbeatTimeoutInMs = 3600000L;

  @JsonProperty("maxAttempts")
  @ApiModelProperty(example = "3")
  private int _maxAttempts = 3;

  @JsonProperty("retryInitialDelayInMs")
  @ApiModelProperty(example = "300000")
  private long _retryInitialDelayInMs = 300000L;

  public boolean isDryRun() {
    return _dryRun;
  }

  public void setDryRun(boolean dryRun) {
    _dryRun = dryRun;
  }

  public boolean isReassignInstances() {
    return _reassignInstances;
  }

  public void setReassignInstances(boolean reassignInstances) {
    _reassignInstances = reassignInstances;
  }

  public boolean isIncludeConsuming() {
    return _includeConsuming;
  }

  public void setIncludeConsuming(boolean includeConsuming) {
    _includeConsuming = includeConsuming;
  }

  public boolean isBootstrap() {
    return _bootstrap;
  }

  public void setBootstrap(boolean bootstrap) {
    _bootstrap = bootstrap;
  }

  public boolean isDowntime() {
    return _downtime;
  }

  public void setDowntime(boolean downtime) {
    _downtime = downtime;
  }

  public int getMinAvailableReplicas() {
    return _minAvailableReplicas;
  }

  public void setMinAvailableReplicas(int minAvailableReplicas) {
    _minAvailableReplicas = minAvailableReplicas;
  }

  public boolean isBestEfforts() {
    return _bestEfforts;
  }

  public void setBestEfforts(boolean bestEfforts) {
    _bestEfforts = bestEfforts;
  }

  public long getExternalViewCheckIntervalInMs() {
    return _externalViewCheckIntervalInMs;
  }

  public void setExternalViewCheckIntervalInMs(long externalViewCheckIntervalInMs) {
    _externalViewCheckIntervalInMs = externalViewCheckIntervalInMs;
  }

  public long getExternalViewStabilizationTimeoutInMs() {
    return _externalViewStabilizationTimeoutInMs;
  }

  public void setExternalViewStabilizationTimeoutInMs(long externalViewStabilizationTimeoutInMs) {
    _externalViewStabilizationTimeoutInMs = externalViewStabilizationTimeoutInMs;
  }

  public boolean isUpdateTargetTier() {
    return _updateTargetTier;
  }

  public void setUpdateTargetTier(boolean updateTargetTier) {
    _updateTargetTier = updateTargetTier;
  }

  public long getHeartbeatIntervalInMs() {
    return _heartbeatIntervalInMs;
  }

  public void setHeartbeatIntervalInMs(long heartbeatIntervalInMs) {
    _heartbeatIntervalInMs = heartbeatIntervalInMs;
  }

  public long getHeartbeatTimeoutInMs() {
    return _heartbeatTimeoutInMs;
  }

  public void setHeartbeatTimeoutInMs(long heartbeatTimeoutInMs) {
    _heartbeatTimeoutInMs = heartbeatTimeoutInMs;
  }

  public int getMaxAttempts() {
    return _maxAttempts;
  }

  public void setMaxAttempts(int maxAttempts) {
    _maxAttempts = maxAttempts;
  }

  public long getRetryInitialDelayInMs() {
    return _retryInitialDelayInMs;
  }

  public void setRetryInitialDelayInMs(long retryInitialDelayInMs) {
    _retryInitialDelayInMs = retryInitialDelayInMs;
  }

  @Override
  public String toString() {
    return "RebalanceConfig{" + "_dryRun=" + _dryRun + ", _reassignInstances=" + _reassignInstances
        + ", _includeConsuming=" + _includeConsuming + ", _bootstrap=" + _bootstrap + ", _downtime=" + _downtime
        + ", _minAvailableReplicas=" + _minAvailableReplicas + ", _bestEfforts=" + _bestEfforts
        + ", _externalViewCheckIntervalInMs=" + _externalViewCheckIntervalInMs
        + ", _externalViewStabilizationTimeoutInMs=" + _externalViewStabilizationTimeoutInMs + ", _updateTargetTier="
        + _updateTargetTier + ", _heartbeatIntervalInMs=" + _heartbeatIntervalInMs + ", _heartbeatTimeoutInMs="
        + _heartbeatTimeoutInMs + ", _maxAttempts=" + _maxAttempts + ", _retryInitialDelayInMs="
        + _retryInitialDelayInMs + '}';
  }

  public static RebalanceConfig copy(RebalanceConfig cfg) {
    RebalanceConfig rc = new RebalanceConfig();
    rc._dryRun = cfg._dryRun;
    rc._reassignInstances = cfg._reassignInstances;
    rc._includeConsuming = cfg._includeConsuming;
    rc._bootstrap = cfg._bootstrap;
    rc._downtime = cfg._downtime;
    rc._minAvailableReplicas = cfg._minAvailableReplicas;
    rc._bestEfforts = cfg._bestEfforts;
    rc._externalViewCheckIntervalInMs = cfg._externalViewCheckIntervalInMs;
    rc._externalViewStabilizationTimeoutInMs = cfg._externalViewStabilizationTimeoutInMs;
    rc._updateTargetTier = cfg._updateTargetTier;
    rc._heartbeatIntervalInMs = cfg._heartbeatIntervalInMs;
    rc._heartbeatTimeoutInMs = cfg._heartbeatTimeoutInMs;
    rc._maxAttempts = cfg._maxAttempts;
    rc._retryInitialDelayInMs = cfg._retryInitialDelayInMs;
    return rc;
  }
}
