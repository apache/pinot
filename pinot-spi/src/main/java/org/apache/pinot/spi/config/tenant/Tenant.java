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
package org.apache.pinot.spi.config.tenant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class Tenant extends BaseJsonConfig {
  private final TenantRole _tenantRole;
  private final String _tenantName;
  private final int _numberOfInstances;
  // For SERVER tenant
  private final int _offlineInstances;
  private final int _realtimeInstances;

  @JsonCreator
  public Tenant(@JsonProperty(value = "tenantRole", required = true) TenantRole tenantRole,
      @JsonProperty(value = "tenantName", required = true) String tenantName,
      @JsonProperty("numberOfInstances") int numberOfInstances, @JsonProperty("offlineInstances") int offlineInstances,
      @JsonProperty("realtimeInstances") int realtimeInstances) {
    Preconditions.checkArgument(tenantRole != null, "'tenantRole' must be configured");
    Preconditions.checkArgument(tenantName != null, "'tenantName' must be configured");
    _tenantRole = tenantRole;
    _tenantName = tenantName;
    _numberOfInstances = numberOfInstances;
    _offlineInstances = offlineInstances;
    _realtimeInstances = realtimeInstances;
  }

  public TenantRole getTenantRole() {
    return _tenantRole;
  }

  public String getTenantName() {
    return _tenantName;
  }

  public int getNumberOfInstances() {
    return _numberOfInstances;
  }

  public int getOfflineInstances() {
    return _offlineInstances;
  }

  public int getRealtimeInstances() {
    return _realtimeInstances;
  }

  @JsonIgnore
  public boolean isCoLocated() {
    return _realtimeInstances + _offlineInstances > _numberOfInstances;
  }
}
