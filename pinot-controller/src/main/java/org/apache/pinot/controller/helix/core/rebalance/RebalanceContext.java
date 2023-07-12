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
public class RebalanceContext {
  @JsonProperty("dryRun")
  @ApiModelProperty(example = "false")
  private Boolean _dryRun = false;
  @JsonProperty("reassignInstances")
  @ApiModelProperty(example = "false")
  private Boolean _reassignInstances = false;
  @JsonProperty("includeConsuming")
  @ApiModelProperty(example = "false")
  private Boolean _includeConsuming = false;
  @JsonProperty("bootstrap")
  @ApiModelProperty(example = "false")
  private Boolean _bootstrap = false;
  @JsonProperty("downtime")
  @ApiModelProperty(example = "false")
  private Boolean _downtime = false;
  @JsonProperty("minAvailableReplicas")
  @ApiModelProperty(example = "1")
  private Integer _minAvailableReplicas = 1;
  @JsonProperty("bestEfforts")
  @ApiModelProperty(example = "false")
  private Boolean _bestEfforts = false;
  @JsonProperty("externalViewCheckIntervalInMs")
  @ApiModelProperty(example = "1000")
  private Long _externalViewCheckIntervalInMs = 1000L;
  @JsonProperty("externalViewStabilizationTimeoutInMs")
  @ApiModelProperty(example = "3600000")
  private Long _externalViewStabilizationTimeoutInMs = 3600000L;
  @JsonProperty("updateTargetTier")
  @ApiModelProperty(example = "false")
  private Boolean _updateTargetTier = false;

  public Boolean isDryRun() {
    return _dryRun;
  }

  public void setDryRun(Boolean dryRun) {
    _dryRun = dryRun;
  }

  public Boolean isReassignInstances() {
    return _reassignInstances;
  }

  public void setReassignInstances(Boolean reassignInstances) {
    _reassignInstances = reassignInstances;
  }

  public Boolean isIncludeConsuming() {
    return _includeConsuming;
  }

  public void setIncludeConsuming(Boolean includeConsuming) {
    _includeConsuming = includeConsuming;
  }

  public Boolean isBootstrap() {
    return _bootstrap;
  }

  public void setBootstrap(Boolean bootstrap) {
    _bootstrap = bootstrap;
  }

  public Boolean isDowntime() {
    return _downtime;
  }

  public void setDowntime(Boolean downtime) {
    _downtime = downtime;
  }

  public Integer getMinAvailableReplicas() {
    return _minAvailableReplicas;
  }

  public void setMinAvailableReplicas(Integer minAvailableReplicas) {
    _minAvailableReplicas = minAvailableReplicas;
  }

  public Boolean isBestEfforts() {
    return _bestEfforts;
  }

  public void setBestEfforts(Boolean bestEfforts) {
    _bestEfforts = bestEfforts;
  }

  public Long getExternalViewCheckIntervalInMs() {
    return _externalViewCheckIntervalInMs;
  }

  public void setExternalViewCheckIntervalInMs(Long externalViewCheckIntervalInMs) {
    _externalViewCheckIntervalInMs = externalViewCheckIntervalInMs;
  }

  public Long getExternalViewStabilizationTimeoutInMs() {
    return _externalViewStabilizationTimeoutInMs;
  }

  public void setExternalViewStabilizationTimeoutInMs(Long externalViewStabilizationTimeoutInMs) {
    _externalViewStabilizationTimeoutInMs = externalViewStabilizationTimeoutInMs;
  }

  public Boolean isUpdateTargetTier() {
    return _updateTargetTier;
  }

  public void setUpdateTargetTier(Boolean updateTargetTier) {
    _updateTargetTier = updateTargetTier;
  }
}
