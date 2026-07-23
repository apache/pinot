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
import java.util.Map;


public class VaultResponse {
  @SerializedName("request_id")
  private String _requestId;
  @SerializedName("lease_id")
  private String _leaseId;
  private boolean _renewable;
  @SerializedName("lease_duration")
  private int _leaseDuration;
  private Map<String, String> _data;
  @SerializedName("wrap_info")
  private String _wrapInfo;
  private String _warnings;
  private VaultAuth _auth;
  private String[] _errors;

  public String getRequestId() {
    return _requestId;
  }

  public void setRequestId(String requestId) {
    _requestId = requestId;
  }

  public String getLeaseId() {
    return _leaseId;
  }

  public void setLeaseId(String leaseId) {
    _leaseId = leaseId;
  }

  public boolean isRenewable() {
    return _renewable;
  }

  public void setRenewable(boolean renewable) {
    _renewable = renewable;
  }

  public int getLeaseDuration() {
    return _leaseDuration;
  }

  public void setLeaseDuration(int leaseDuration) {
    _leaseDuration = leaseDuration;
  }

  public Map<String, String> getData() {
    return _data;
  }

  public void setData(Map<String, String> data) {
    _data = data;
  }

  public String getWrapInfo() {
    return _wrapInfo;
  }

  public void setWrapInfo(String wrapInfo) {
    _wrapInfo = wrapInfo;
  }

  public String getWarnings() {
    return _warnings;
  }

  public void setWarnings(String warnings) {
    _warnings = warnings;
  }

  public VaultAuth getAuth() {
    return _auth;
  }

  public void setAuth(VaultAuth auth) {
    _auth = auth;
  }

  public String[] getErrors() {
    return _errors;
  }

  public void setErrors(String[] errors) {
    _errors = errors;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("errors", _errors).add("auth", _auth).add("requestId", _requestId)
        .add("errors", _errors).add("data", _data).toString();
  }
}
