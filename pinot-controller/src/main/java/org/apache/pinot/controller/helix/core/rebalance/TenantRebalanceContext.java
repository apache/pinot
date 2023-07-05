package org.apache.pinot.controller.helix.core.rebalance;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.HashSet;
import java.util.Set;


public class TenantRebalanceContext extends RebalanceContext {
  @JsonProperty("degreeOfParallelism")
  @ApiModelProperty(example = "1")
  Integer _degreeOfParallelism = 1;
  @JsonProperty("parallelWhitelist")
  private Set<String> _parallelWhitelist = new HashSet<>();
  @JsonProperty("parallelBlacklist")
  private Set<String> _parallelBlacklist = new HashSet<>();;

  public int getDegreeOfParallelism() {
    return _degreeOfParallelism;
  }

  public void setDegreeOfParallelism(int degreeOfParallelism) {
    this._degreeOfParallelism = degreeOfParallelism;
  }

  public Set<String> getParallelWhitelist() {
    return _parallelWhitelist;
  }

  public void setParallelWhitelist(Set<String> parallelWhitelist) {
    this._parallelWhitelist = parallelWhitelist;
  }

  public Set<String> getParallelBlacklist() {
    return _parallelBlacklist;
  }

  public void setParallelBlacklist(Set<String> parallelBlacklist) {
    this._parallelBlacklist = parallelBlacklist;
  }
}
