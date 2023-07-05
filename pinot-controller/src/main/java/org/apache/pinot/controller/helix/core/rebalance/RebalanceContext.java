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
    this._dryRun = dryRun;
  }

  public Boolean isReassignInstances() {
    return _reassignInstances;
  }

  public void setReassignInstances(Boolean reassignInstances) {
    this._reassignInstances = reassignInstances;
  }

  public Boolean isIncludeConsuming() {
    return _includeConsuming;
  }

  public void setIncludeConsuming(Boolean includeConsuming) {
    this._includeConsuming = includeConsuming;
  }

  public Boolean isBootstrap() {
    return _bootstrap;
  }

  public void setBootstrap(Boolean bootstrap) {
    this._bootstrap = bootstrap;
  }

  public Boolean isDowntime() {
    return _downtime;
  }

  public void setDowntime(Boolean downtime) {
    this._downtime = downtime;
  }

  public Integer getMinAvailableReplicas() {
    return _minAvailableReplicas;
  }

  public void setMinAvailableReplicas(Integer minAvailableReplicas) {
    this._minAvailableReplicas = minAvailableReplicas;
  }

  public Boolean isBestEfforts() {
    return _bestEfforts;
  }

  public void setBestEfforts(Boolean bestEfforts) {
    this._bestEfforts = bestEfforts;
  }

  public Long getExternalViewCheckIntervalInMs() {
    return _externalViewCheckIntervalInMs;
  }

  public void setExternalViewCheckIntervalInMs(Long externalViewCheckIntervalInMs) {
    this._externalViewCheckIntervalInMs = externalViewCheckIntervalInMs;
  }

  public Long getExternalViewStabilizationTimeoutInMs() {
    return _externalViewStabilizationTimeoutInMs;
  }

  public void setExternalViewStabilizationTimeoutInMs(Long externalViewStabilizationTimeoutInMs) {
    this._externalViewStabilizationTimeoutInMs = externalViewStabilizationTimeoutInMs;
  }

  public Boolean isUpdateTargetTier() {
    return _updateTargetTier;
  }

  public void setUpdateTargetTier(Boolean updateTargetTier) {
    this._updateTargetTier = updateTargetTier;
  }
}
