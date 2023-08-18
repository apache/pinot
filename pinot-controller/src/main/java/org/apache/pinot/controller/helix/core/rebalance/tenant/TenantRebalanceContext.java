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
import org.apache.pinot.controller.helix.core.rebalance.RebalanceContext;


public class TenantRebalanceContext extends RebalanceContext {
  @JsonIgnore
  private String _tenantName;
  @JsonProperty("degreeOfParallelism")
  @ApiModelProperty(example = "1")
  private Integer _degreeOfParallelism = 1;
  @JsonProperty("parallelWhitelist")
  private Set<String> _parallelWhitelist = new HashSet<>();
  @JsonProperty("parallelBlacklist")
  private Set<String> _parallelBlacklist = new HashSet<>();

  private boolean _verboseResult = false;

  public String getTenantName() {
    return _tenantName;
  }

  public void setTenantName(String tenantName) {
    _tenantName = tenantName;
  }

  public int getDegreeOfParallelism() {
    return _degreeOfParallelism;
  }

  public void setDegreeOfParallelism(int degreeOfParallelism) {
    _degreeOfParallelism = degreeOfParallelism;
  }

  public Set<String> getParallelWhitelist() {
    return _parallelWhitelist;
  }

  public void setParallelWhitelist(Set<String> parallelWhitelist) {
    _parallelWhitelist = parallelWhitelist;
  }

  public Set<String> getParallelBlacklist() {
    return _parallelBlacklist;
  }

  public void setParallelBlacklist(Set<String> parallelBlacklist) {
    _parallelBlacklist = parallelBlacklist;
  }

  public boolean isVerboseResult() {
    return _verboseResult;
  }

  public void setVerboseResult(boolean verboseResult) {
    _verboseResult = verboseResult;
  }
}
