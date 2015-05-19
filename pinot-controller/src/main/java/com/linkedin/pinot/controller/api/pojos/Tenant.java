/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.pojos;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.linkedin.pinot.common.utils.TenantRole;


public class Tenant {

  private final static String TENANT_ROLE = "role";
  private final static String TENANT_NAME = "name";
  private final static String NUMBER_OF_INSTANCES = "numberOfInstances";
  private final static String OFFLINE_INSTANCES = "offlineInstances";
  private final static String REALTIME_INSTANCES = "realtimeInstances";

  private final TenantRole _tenantRole;
  private final String _tenantName;
  private final int _numberOfInstances;
  private final int _offlineInstances;
  private final int _realtimeInstances;

  @JsonCreator
  public Tenant(@JsonProperty(TENANT_ROLE) TenantRole tenantRole,
      @JsonProperty(TENANT_NAME) String tenantName,
      @JsonProperty(NUMBER_OF_INSTANCES) int numberOfInstances,
      @JsonProperty(OFFLINE_INSTANCES) int offlineInstances,
      @JsonProperty(REALTIME_INSTANCES) int realtimeInstances) {

    this._tenantRole = tenantRole;
    this._tenantName = tenantName;
    this._numberOfInstances = numberOfInstances;
    if (_tenantRole == TenantRole.SERVER) {
      if (offlineInstances > numberOfInstances || realtimeInstances > numberOfInstances) {
        throw new RuntimeException("OfflineInstances/RealtimeInstances shouldn't exceed numberOfInstances");
      }
      this._offlineInstances = offlineInstances;
      this._realtimeInstances = realtimeInstances;
    } else {
      this._offlineInstances = -1;
      this._realtimeInstances = -1;
    }
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

  public boolean isColoated() {
    return (_realtimeInstances + _offlineInstances > _numberOfInstances);
  }

  public static Tenant fromMap(Map<String, String> props) {
    TenantRole tenantRole = TenantRole.valueOf(props.get(TENANT_ROLE));
    String tenantName = props.get(TENANT_NAME);
    int numberOfInstances = Integer.parseInt(props.get(NUMBER_OF_INSTANCES));
    if (tenantRole == TenantRole.SERVER) {
      int offlineInstances = Integer.parseInt(props.get(OFFLINE_INSTANCES));
      int realtimeInstances = Integer.parseInt(props.get(REALTIME_INSTANCES));
      return new Tenant(tenantRole, tenantName, numberOfInstances, offlineInstances, realtimeInstances);
    } else {
      return new Tenant(tenantRole, tenantName, numberOfInstances, -1, -1);
    }
  }

  /**
   *  returns true if all properties are the same
   */
  @Override
  public boolean equals(Object object) {
    if (!(object instanceof Tenant)) {
      return false;
    }

    final Tenant toCompare = (Tenant) object;

    if ((toCompare.getTenantRole().equals(getTenantName())) && (toCompare.getTenantName().equals(getTenantName()))
        && (toCompare.getNumberOfInstances() == getNumberOfInstances())) {
      if (getTenantRole() == TenantRole.SERVER) {
        if (toCompare.getOfflineInstances() == getOfflineInstances() && toCompare.getRealtimeInstances() == getRealtimeInstances()) {
          return true;
        } else {
          return false;
        }
      } else {
        return true;
      }
    }
    return false;
  }

  public int hashCode() {
    if (_tenantRole == TenantRole.SERVER) {
      return Objects.hashCode(
          _tenantRole,
          _tenantName,
          _numberOfInstances,
          _offlineInstances,
          _realtimeInstances);
    } else {
      return Objects.hashCode(
          _tenantRole,
          _tenantName,
          _numberOfInstances);

    }

  }

  public Map<String, String> toMap() {
    final Map<String, String> ret = new HashMap<String, String>();
    ret.put(TENANT_ROLE, _tenantRole.toString());
    ret.put(TENANT_NAME, _tenantName);
    ret.put(NUMBER_OF_INSTANCES, String.valueOf(_numberOfInstances));
    if (_tenantRole == TenantRole.SERVER) {
      ret.put(OFFLINE_INSTANCES, String.valueOf(_offlineInstances));
      ret.put(REALTIME_INSTANCES, String.valueOf(_realtimeInstances));
    }
    return ret;
  }

  @Override
  public String toString() {
    final StringBuilder bld = new StringBuilder();
    bld.append(TENANT_ROLE + " : " + _tenantRole + "\n");
    bld.append(TENANT_NAME + " : " + _tenantName + "\n");
    bld.append(NUMBER_OF_INSTANCES + " : " + _numberOfInstances + "\n");
    if (_tenantRole == TenantRole.SERVER) {
      bld.append(OFFLINE_INSTANCES + " : " + _offlineInstances + "\n");
      bld.append(REALTIME_INSTANCES + " : " + _realtimeInstances + "\n");
    }
    return bld.toString();
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put(TENANT_ROLE, _tenantRole);
    ret.put(TENANT_NAME, _tenantName);
    ret.put(NUMBER_OF_INSTANCES, _numberOfInstances);
    if (_tenantRole == TenantRole.SERVER) {
      ret.put(OFFLINE_INSTANCES, _offlineInstances);
      ret.put(REALTIME_INSTANCES, _realtimeInstances);
    }
    return ret;
  }

}
