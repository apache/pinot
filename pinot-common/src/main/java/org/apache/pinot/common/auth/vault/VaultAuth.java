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
package org.apache.pinot.common.auth.vault;

import com.google.common.base.MoreObjects;
import com.google.gson.annotations.SerializedName;


public class VaultAuth {
  @SerializedName("client_token")
  private String _clientToken;
  private String _accessor;
  private String[] _policies;
  @SerializedName("token_policies")
  private String[] _tokenPolicies;
  @SerializedName("lease_duration")
  private int _leaseDuration;

  private boolean _renewable;
  @SerializedName("entity_id")
  private String _entityId;
  @SerializedName("token_type")
  private String _tokenType;

  private boolean _orphan;

  public String getClientToken() {
    return _clientToken;
  }

  public void setClientToken(String clientToken) {
    _clientToken = clientToken;
  }

  public String getAccessor() {
    return _accessor;
  }

  public void setAccessor(String accessor) {
    _accessor = accessor;
  }

  public String[] getPolicies() {
    return _policies;
  }

  public void setPolicies(String[] policies) {
    _policies = policies;
  }

  public String[] getTokenPolicies() {
    return _tokenPolicies;
  }

  public void setTokenPolicies(String[] tokenPolicies) {
    _tokenPolicies = tokenPolicies;
  }

  public int getLeaseDuration() {
    return _leaseDuration;
  }

  public void setLeaseDuration(int leaseDuration) {
    _leaseDuration = leaseDuration;
  }

  public boolean isRenewable() {
    return _renewable;
  }

  public void setRenewable(boolean renewable) {
    _renewable = renewable;
  }

  public String getEntityId() {
    return _entityId;
  }

  public void setEntityId(String entityId) {
    _entityId = entityId;
  }

  public String getTokenType() {
    return _tokenType;
  }

  public void setTokenType(String tokenType) {
    _tokenType = tokenType;
  }

  public boolean isOrphan() {
    return _orphan;
  }

  public void setOrphan(boolean orphan) {
    _orphan = orphan;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("clientToken", _clientToken).add("accessor", _accessor)
        .add("tokenPolicies", _tokenPolicies).add("tokenType", _tokenType).toString();
  }
}
