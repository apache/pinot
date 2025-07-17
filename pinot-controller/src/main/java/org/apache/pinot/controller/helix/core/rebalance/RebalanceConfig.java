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
import com.google.common.base.Preconditions;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.pinot.controller.api.resources.ForceCommitBatchConfig;
import org.apache.pinot.spi.utils.Enablement;


@ApiModel
public class RebalanceConfig {
  public static final int DISABLE_BATCH_SIZE_PER_SERVER = -1;
  public static final int DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME = 1;
  public static final long DEFAULT_EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS = 1000L; // 1 second
  public static final long DEFAULT_EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS = 3600000L; // 1 hour

  // Whether to rebalance table in dry-run mode.
  @JsonProperty("dryRun")
  @ApiModelProperty(example = "true")
  private boolean _dryRun = false;

  // Whether to perform pre-checks for rebalance. This only returns the status of each pre-check and does not fail
  // rebalance
  @JsonProperty("preChecks")
  @ApiModelProperty(example = "false")
  private boolean _preChecks = false;

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

  // This flag only applies to peer-download enabled tables undergoing downtime=true or minAvailableReplicas=0
  // rebalance (both of which can result in possible data loss scenarios). If enabled, this flag will allow the
  // rebalance to continue even in cases where data loss scenarios have been detected, otherwise the rebalance will
  // be failed and user action will be required to rebalance again. This flag should be used with caution and only
  // used in scenarios where data loss is acceptable.
  @JsonProperty("allowPeerDownloadDataLoss")
  @ApiModelProperty(example = "false")
  private boolean _allowPeerDownloadDataLoss = false;

  // For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or maximum number of replicas
  // allowed to be unavailable if value is negative
  @JsonProperty("minAvailableReplicas")
  @ApiModelProperty(example = "1")
  private int _minAvailableReplicas = DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME;

  // For no-downtime rebalance, whether to enable low disk mode during rebalance. When enabled, segments will first be
  // offloaded from servers, then added to servers after offload is done while maintaining the min available replicas.
  // It may increase the total time of the rebalance, but can be useful when servers are low on disk space, and we want
  // to scale up the cluster and rebalance the table to more servers.
  @JsonProperty("lowDiskMode")
  @ApiModelProperty(example = "false")
  private boolean _lowDiskMode = false;

  // Whether to use best-efforts to rebalance (not fail the rebalance when the no-downtime contract cannot be achieved)
  // When using best-efforts to rebalance, the following scenarios won't fail the rebalance (will log warnings instead):
  // - Segment falls into ERROR state in ExternalView -> count ERROR state as good state
  // - ExternalView has not converged within the maximum wait time -> continue to the next stage
  @JsonProperty("bestEfforts")
  @ApiModelProperty(example = "false")
  private boolean _bestEfforts = false;

  // Whether to run Minimal Data Movement Algorithm, overriding the minimizeDataMovement flag in table config. If set
  // to DEFAULT, the minimizeDataMovement flag in table config will be used to determine whether to run the Minimal
  // Data Movement Algorithm.
  @JsonProperty("minimizeDataMovement")
  @ApiModelProperty(dataType = "string", allowableValues = "ENABLE, DISABLE, DEFAULT", example = "ENABLE")
  private Enablement _minimizeDataMovement = Enablement.ENABLE;

  // How many segment add updates to make per server to the IdealState as part of each rebalance step. This is used
  // as closest estimated upper-bound. For strict replica group based assignment, there is the additional constraint
  // to move each partitionId replica as a whole rather than splitting it up. In this case the total segment adds per
  // server may be above the threshold to accommodate a full partition. The minReplicasAvailable invariant is also
  // maintained, so fewer segments than the batchSizePerServer may also be selected. Batching is disabled by default by
  // setting it to -1.
  @JsonProperty("batchSizePerServer")
  @ApiModelProperty(example = "100")
  private int _batchSizePerServer = DISABLE_BATCH_SIZE_PER_SERVER;

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

  // Disk utilization threshold override. If set, this will override the default disk utilization threshold
  // configured at the controller level. Value should be between 0.0 and 1.0 (e.g., 0.85 for 85%) or -1.0, which means
  // no override. In the latter case the pre-checker will use the default disk utilization threshold from the controller
  // config.
  @JsonProperty("diskUtilizationThreshold")
  @ApiModelProperty(example = "0.85")
  private double _diskUtilizationThreshold = -1.0;

  @JsonProperty("forceCommit")
  @ApiModelProperty(example = "false")
  private boolean _forceCommit = false;

  @JsonProperty("forceCommitBatchSize")
  @ApiModelProperty(example = ForceCommitBatchConfig.DEFAULT_BATCH_SIZE + "")
  private int _forceCommitBatchSize = ForceCommitBatchConfig.DEFAULT_BATCH_SIZE;

  @JsonProperty("forceCommitBatchStatusCheckIntervalMs")
  @ApiModelProperty(example = ForceCommitBatchConfig.DEFAULT_STATUS_CHECK_INTERVAL_SEC * 1000 + "")
  private int _forceCommitBatchStatusCheckIntervalMs = ForceCommitBatchConfig.DEFAULT_STATUS_CHECK_INTERVAL_SEC * 1000;

  @JsonProperty("forceCommitBatchStatusCheckTimeoutMs")
  @ApiModelProperty(example = ForceCommitBatchConfig.DEFAULT_STATUS_CHECK_TIMEOUT_SEC * 1000 + "")
  private int _forceCommitBatchStatusCheckTimeoutMs = ForceCommitBatchConfig.DEFAULT_STATUS_CHECK_TIMEOUT_SEC * 1000;

  public boolean isDryRun() {
    return _dryRun;
  }

  public void setDryRun(boolean dryRun) {
    _dryRun = dryRun;
  }

  public boolean isPreChecks() {
    return _preChecks;
  }

  public void setPreChecks(boolean preChecks) {
    _preChecks = preChecks;
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

  public boolean isAllowPeerDownloadDataLoss() {
    return _allowPeerDownloadDataLoss;
  }

  public void setAllowPeerDownloadDataLoss(boolean allowPeerDownloadDataLoss) {
    _allowPeerDownloadDataLoss = allowPeerDownloadDataLoss;
  }

  public int getMinAvailableReplicas() {
    return _minAvailableReplicas;
  }

  public void setMinAvailableReplicas(int minAvailableReplicas) {
    _minAvailableReplicas = minAvailableReplicas;
  }

  public boolean isLowDiskMode() {
    return _lowDiskMode;
  }

  public void setLowDiskMode(boolean lowDiskMode) {
    _lowDiskMode = lowDiskMode;
  }

  public boolean isBestEfforts() {
    return _bestEfforts;
  }

  public void setBestEfforts(boolean bestEfforts) {
    _bestEfforts = bestEfforts;
  }

  public int getBatchSizePerServer() {
    return _batchSizePerServer;
  }

  public void setBatchSizePerServer(int batchSizePerServer) {
    _batchSizePerServer = batchSizePerServer;
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

  public boolean isForceCommit() {
    return _forceCommit;
  }

  public void setForceCommit(boolean forceCommit) {
    _forceCommit = forceCommit;
  }

  public int getForceCommitBatchSize() {
    return _forceCommitBatchSize;
  }

  public void setForceCommitBatchSize(int forceCommitBatchSize) {
    _forceCommitBatchSize = forceCommitBatchSize;
  }

  public int getForceCommitBatchStatusCheckIntervalMs() {
    return _forceCommitBatchStatusCheckIntervalMs;
  }

  public void setForceCommitBatchStatusCheckIntervalMs(int forceCommitBatchStatusCheckIntervalMs) {
    _forceCommitBatchStatusCheckIntervalMs = forceCommitBatchStatusCheckIntervalMs;
  }

  public int getForceCommitBatchStatusCheckTimeoutMs() {
    return _forceCommitBatchStatusCheckTimeoutMs;
  }

  public void setForceCommitBatchStatusCheckTimeoutMs(int forceCommitBatchStatusCheckTimeoutMs) {
    _forceCommitBatchStatusCheckTimeoutMs = forceCommitBatchStatusCheckTimeoutMs;
  }

  public Enablement getMinimizeDataMovement() {
    return _minimizeDataMovement;
  }

  public void setMinimizeDataMovement(Enablement minimizeDataMovement) {
    Preconditions.checkArgument(minimizeDataMovement != null,
        "'minimizeDataMovement' cannot be null, must be ENABLE, DISABLE or DEFAULT");
    _minimizeDataMovement = minimizeDataMovement;
  }

  public double getDiskUtilizationThreshold() {
    return _diskUtilizationThreshold;
  }

  public void setDiskUtilizationThreshold(double diskUtilizationThreshold) {
    _diskUtilizationThreshold = diskUtilizationThreshold;
  }

  @Override
  public String toString() {
    return "RebalanceConfig{" + "_dryRun=" + _dryRun + ", preChecks=" + _preChecks + ", _reassignInstances="
        + _reassignInstances + ", _includeConsuming=" + _includeConsuming + ", _minimizeDataMovement="
        + _minimizeDataMovement + ", _bootstrap=" + _bootstrap + ", _downtime=" + _downtime
        + ", _allowPeerDownloadDataLoss=" + _allowPeerDownloadDataLoss + ", _minAvailableReplicas="
        + _minAvailableReplicas + ", _bestEfforts=" + _bestEfforts + ", batchSizePerServer="
        + _batchSizePerServer + ", _externalViewCheckIntervalInMs=" + _externalViewCheckIntervalInMs
        + ", _externalViewStabilizationTimeoutInMs=" + _externalViewStabilizationTimeoutInMs + ", _updateTargetTier="
        + _updateTargetTier + ", _heartbeatIntervalInMs=" + _heartbeatIntervalInMs + ", _heartbeatTimeoutInMs="
        + _heartbeatTimeoutInMs + ", _maxAttempts=" + _maxAttempts + ", _retryInitialDelayInMs="
        + _retryInitialDelayInMs + ", _diskUtilizationThreshold=" + _diskUtilizationThreshold + ", _forceCommit="
        + _forceCommit + ", _forceCommitBatchSize=" + _forceCommitBatchSize
        + ", _forceCommitBatchStatusCheckIntervalMs=" + _forceCommitBatchStatusCheckIntervalMs
        + ", _forceCommitBatchStatusCheckTimeoutMs=" + _forceCommitBatchStatusCheckTimeoutMs + '}';
  }

  public String toQueryString() {
    return "dryRun=" + _dryRun + "&preChecks=" + _preChecks + "&reassignInstances=" + _reassignInstances
        + "&includeConsuming=" + _includeConsuming + "&bootstrap=" + _bootstrap + "&downtime=" + _downtime
        + "&allowPeerDownloadDataLoss=" + _allowPeerDownloadDataLoss + "&minAvailableReplicas=" + _minAvailableReplicas
        + "&bestEfforts=" + _bestEfforts + "&minimizeDataMovement=" + _minimizeDataMovement.name()
        + "&batchSizePerServer=" + _batchSizePerServer
        + "&externalViewCheckIntervalInMs=" + _externalViewCheckIntervalInMs
        + "&externalViewStabilizationTimeoutInMs=" + _externalViewStabilizationTimeoutInMs
        + "&updateTargetTier=" + _updateTargetTier + "&heartbeatIntervalInMs=" + _heartbeatIntervalInMs
        + "&heartbeatTimeoutInMs=" + _heartbeatTimeoutInMs + "&maxAttempts=" + _maxAttempts
        + "&retryInitialDelayInMs=" + _retryInitialDelayInMs
        + "&forceCommit=" + _forceCommit
        + "&forceCommitBatchSize=" + _forceCommitBatchSize
        + "&forceCommitBatchStatusCheckIntervalMs=" + _forceCommitBatchStatusCheckIntervalMs
        + "&forceCommitBatchStatusCheckTimeoutMs=" + _forceCommitBatchStatusCheckTimeoutMs;
  }

  public static RebalanceConfig copy(RebalanceConfig cfg) {
    RebalanceConfig rc = new RebalanceConfig();
    rc._dryRun = cfg._dryRun;
    rc._preChecks = cfg._preChecks;
    rc._reassignInstances = cfg._reassignInstances;
    rc._includeConsuming = cfg._includeConsuming;
    rc._bootstrap = cfg._bootstrap;
    rc._downtime = cfg._downtime;
    rc._allowPeerDownloadDataLoss = cfg._allowPeerDownloadDataLoss;
    rc._minAvailableReplicas = cfg._minAvailableReplicas;
    rc._bestEfforts = cfg._bestEfforts;
    rc._minimizeDataMovement = cfg._minimizeDataMovement;
    rc._batchSizePerServer = cfg._batchSizePerServer;
    rc._externalViewCheckIntervalInMs = cfg._externalViewCheckIntervalInMs;
    rc._externalViewStabilizationTimeoutInMs = cfg._externalViewStabilizationTimeoutInMs;
    rc._updateTargetTier = cfg._updateTargetTier;
    rc._heartbeatIntervalInMs = cfg._heartbeatIntervalInMs;
    rc._heartbeatTimeoutInMs = cfg._heartbeatTimeoutInMs;
    rc._maxAttempts = cfg._maxAttempts;
    rc._retryInitialDelayInMs = cfg._retryInitialDelayInMs;
    rc._diskUtilizationThreshold = cfg._diskUtilizationThreshold;
    rc._forceCommit = cfg._forceCommit;
    rc._forceCommitBatchSize = cfg._forceCommitBatchSize;
    rc._forceCommitBatchStatusCheckIntervalMs = cfg._forceCommitBatchStatusCheckIntervalMs;
    rc._forceCommitBatchStatusCheckTimeoutMs = cfg._forceCommitBatchStatusCheckTimeoutMs;
    return rc;
  }
}
