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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.common.utils.TenantRole;


@JsonIgnoreProperties(ignoreUnknown = true)
public class Tenant {
  private TenantRole _tenantRole;
  private String _tenantName;
  private int _numberOfInstances = 0;
  private int _offlineInstances = 0;
  private int _realtimeInstances = 0;

  public TenantRole getTenantRole() {
    return _tenantRole;
  }

  public void setTenantRole(TenantRole tenantRole) {
    _tenantRole = tenantRole;
  }

  public String getTenantName() {
    return _tenantName;
  }

  public void setTenantName(String tenantName) {
    _tenantName = tenantName;
  }

  public int getNumberOfInstances() {
    return _numberOfInstances;
  }

  public void setNumberOfInstances(int numberOfInstances) {
    _numberOfInstances = numberOfInstances;
  }

  public int getOfflineInstances() {
    return _offlineInstances;
  }

  public void setOfflineInstances(int offlineInstances) {
    _offlineInstances = offlineInstances;
  }

  public int getRealtimeInstances() {
    return _realtimeInstances;
  }

  public void setRealtimeInstances(int realtimeInstances) {
    _realtimeInstances = realtimeInstances;
  }

  @JsonIgnore
  public boolean isCoLocated() {
    return _realtimeInstances + _offlineInstances > _numberOfInstances;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof Tenant) {
      Tenant that = (Tenant) obj;
      return EqualityUtils.isEqual(_numberOfInstances, that._numberOfInstances) && EqualityUtils
          .isEqual(_offlineInstances, that._offlineInstances) && EqualityUtils
          .isEqual(_realtimeInstances, that._realtimeInstances) && EqualityUtils.isEqual(_tenantRole, that._tenantRole)
          && EqualityUtils.isEqual(_tenantName, that._tenantName);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_tenantRole);
    result = EqualityUtils.hashCodeOf(result, _tenantName);
    result = EqualityUtils.hashCodeOf(result, _numberOfInstances);
    result = EqualityUtils.hashCodeOf(result, _offlineInstances);
    result = EqualityUtils.hashCodeOf(result, _realtimeInstances);
    return result;
  }

  public static class TenantBuilder {
    private final Tenant _tenant;

    public TenantBuilder(String name) {
      _tenant = new Tenant();
      _tenant.setTenantName(name);
    }

    public TenantBuilder setRole(TenantRole role) {
      _tenant.setTenantRole(role);
      return this;
    }

    public TenantBuilder setTotalInstances(int totalInstances) {
      _tenant.setNumberOfInstances(totalInstances);
      return this;
    }

    public TenantBuilder setOfflineInstances(int offlineInstances) {
      _tenant.setOfflineInstances(offlineInstances);
      return this;
    }

    public TenantBuilder setRealtimeInstances(int realtimeInstances) {
      _tenant.setRealtimeInstances(realtimeInstances);
      return this;
    }

    public Tenant build() {
      return _tenant;
    }
  }
}
