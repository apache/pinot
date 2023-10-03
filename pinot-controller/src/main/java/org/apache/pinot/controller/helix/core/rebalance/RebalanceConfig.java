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
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;


@ApiModel
public class RebalanceConfig {
  @JsonProperty("dryRun")
  @ApiModelProperty(example = "false")
  private boolean _dryRun = false;
  @JsonProperty("reassignInstances")
  @ApiModelProperty(example = "false")
  private boolean _reassignInstances = false;
  @JsonProperty("includeConsuming")
  @ApiModelProperty(example = "false")
  private boolean _includeConsuming = false;
  @JsonProperty("bootstrap")
  @ApiModelProperty(example = "false")
  private boolean _bootstrap = false;
  @JsonProperty("downtime")
  @ApiModelProperty(example = "false")
  private boolean _downtime = false;
  @JsonProperty("minAvailableReplicas")
  @ApiModelProperty(example = "1")
  private int _minAvailableReplicas = 1;
  @JsonProperty("bestEfforts")
  @ApiModelProperty(example = "false")
  private boolean _bestEfforts = false;
  @JsonProperty("externalViewCheckIntervalInMs")
  @ApiModelProperty(example = "1000")
  private long _externalViewCheckIntervalInMs = 1000L;
  @JsonProperty("externalViewStabilizationTimeoutInMs")
  @ApiModelProperty(example = "3600000")
  private long _externalViewStabilizationTimeoutInMs = 3600000L;
  @JsonProperty("updateTargetTier")
  @ApiModelProperty(example = "false")
  private boolean _updateTargetTier = false;

  @JsonProperty("jobId")
  private String _jobId = null;

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

  public String getJobId() {
    return _jobId;
  }

  public void setJobId(String jobId) {
    _jobId = jobId;
  }

  // Helper method to deprecate the use of Configuration to keep rebalance configs.
  public static RebalanceConfig fromConfiguration(Configuration cfg) {
    RebalanceConfig rc = new RebalanceConfig();
    rc.setDryRun(cfg.getBoolean(RebalanceConfigConstants.DRY_RUN, RebalanceConfigConstants.DEFAULT_DRY_RUN));
    rc.setReassignInstances(cfg.getBoolean(RebalanceConfigConstants.REASSIGN_INSTANCES,
        RebalanceConfigConstants.DEFAULT_REASSIGN_INSTANCES));
    rc.setIncludeConsuming(
        cfg.getBoolean(RebalanceConfigConstants.INCLUDE_CONSUMING, RebalanceConfigConstants.DEFAULT_INCLUDE_CONSUMING));
    rc.setBootstrap(cfg.getBoolean(RebalanceConfigConstants.BOOTSTRAP, RebalanceConfigConstants.DEFAULT_BOOTSTRAP));
    rc.setDowntime(cfg.getBoolean(RebalanceConfigConstants.DOWNTIME, RebalanceConfigConstants.DEFAULT_DOWNTIME));
    rc.setMinAvailableReplicas(cfg.getInt(RebalanceConfigConstants.MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME,
        RebalanceConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME));
    rc.setBestEfforts(
        cfg.getBoolean(RebalanceConfigConstants.BEST_EFFORTS, RebalanceConfigConstants.DEFAULT_BEST_EFFORTS));
    rc.setExternalViewCheckIntervalInMs(cfg.getLong(RebalanceConfigConstants.EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS,
        RebalanceConfigConstants.DEFAULT_EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS));
    rc.setExternalViewStabilizationTimeoutInMs(
        cfg.getLong(RebalanceConfigConstants.EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS,
            RebalanceConfigConstants.DEFAULT_EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS));
    rc.setUpdateTargetTier(cfg.getBoolean(RebalanceConfigConstants.UPDATE_TARGET_TIER,
        RebalanceConfigConstants.DEFAULT_UPDATE_TARGET_TIER));
    rc.setJobId(cfg.getString(CommonConstants.ControllerJob.JOB_ID, null));
    return rc;
  }
}
