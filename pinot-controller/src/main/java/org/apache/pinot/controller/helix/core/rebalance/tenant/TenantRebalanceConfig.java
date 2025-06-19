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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;


public class TenantRebalanceConfig extends RebalanceConfig {
  // These fields are parameters for tenant rebalance. Hiding them in the swagger UI because we expect them to be set
  // via query parameters. User can still set the fields in the POST body without errors, but it will be overridden by
  // the values specified via query parameters.
  @JsonIgnore
  @ApiModelProperty(hidden = true)
  private String _tenantName;
  @JsonProperty("degreeOfParallelism")
  @ApiModelProperty(hidden = true)
  private int _degreeOfParallelism = 1;
  @JsonProperty("parallelBlacklist")
  @ApiModelProperty(hidden = true)
  private Set<String> _parallelBlacklist = new HashSet<>();
  @JsonProperty("parallelWhitelist")
  @ApiModelProperty(hidden = true)
  private Set<String> _parallelWhitelist = new HashSet<>();
  // If empty, default to allow all tables
  @JsonProperty("includeTables")
  @ApiModelProperty(hidden = true)
  private Set<String> _includeTables = new HashSet<>();
  @JsonProperty("excludeTables")
  @ApiModelProperty(hidden = true)
  private Set<String> _excludeTables = new HashSet<>();

  private boolean _verboseResult = false;

  @ApiModelProperty(hidden = true)
  public String getTenantName() {
    return _tenantName;
  }

  public void setTenantName(String tenantName) {
    _tenantName = tenantName;
  }

  @ApiModelProperty(hidden = true)
  public int getDegreeOfParallelism() {
    return _degreeOfParallelism;
  }

  public void setDegreeOfParallelism(int degreeOfParallelism) {
    _degreeOfParallelism = degreeOfParallelism;
  }

  @ApiModelProperty(hidden = true)
  public Set<String> getParallelWhitelist() {
    return _parallelWhitelist;
  }

  public void setParallelWhitelist(Set<String> parallelWhitelist) {
    _parallelWhitelist = parallelWhitelist;
  }

  @ApiModelProperty(hidden = true)
  public Set<String> getParallelBlacklist() {
    return _parallelBlacklist;
  }

  public void setParallelBlacklist(Set<String> parallelBlacklist) {
    _parallelBlacklist = parallelBlacklist;
  }

  @ApiModelProperty(hidden = true)
  public Set<String> getIncludeTables() {
    return _includeTables;
  }

  public void setIncludeTables(Set<String> includeTables) {
    _includeTables = includeTables;
  }

  @ApiModelProperty(hidden = true)
  public Set<String> getExcludeTables() {
    return _excludeTables;
  }

  public void setExcludeTables(Set<String> excludeTables) {
    _excludeTables = excludeTables;
  }

  @ApiModelProperty(hidden = true)
  public boolean isVerboseResult() {
    return _verboseResult;
  }

  public void setVerboseResult(boolean verboseResult) {
    _verboseResult = verboseResult;
  }
}
