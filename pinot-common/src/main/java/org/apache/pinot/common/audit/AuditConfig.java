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
package org.apache.pinot.common.audit;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Pure data class for audit logging configuration.
 * Uses Jackson annotations for automatic JSON mapping from ClusterConfiguration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class AuditConfig {

  @JsonProperty("enabled")
  private boolean _enabled = false;

  @JsonProperty("capture.request.payload.enabled")
  private boolean _captureRequestPayload = false;

  @JsonProperty("capture.request.headers")
  private boolean _captureRequestHeaders = false;

  @JsonProperty("payload.size.max.bytes")
  private int _maxPayloadSize = 10_240;

  @JsonProperty("excluded.endpoints")
  private String _excludedEndpoints = "";

  public boolean isEnabled() {
    return _enabled;
  }

  public void setEnabled(boolean enabled) {
    _enabled = enabled;
  }

  public boolean isCaptureRequestPayload() {
    return _captureRequestPayload;
  }

  public void setCaptureRequestPayload(boolean captureRequestPayload) {
    _captureRequestPayload = captureRequestPayload;
  }

  public boolean isCaptureRequestHeaders() {
    return _captureRequestHeaders;
  }

  public void setCaptureRequestHeaders(boolean captureRequestHeaders) {
    _captureRequestHeaders = captureRequestHeaders;
  }

  public int getMaxPayloadSize() {
    return _maxPayloadSize;
  }

  public void setMaxPayloadSize(int maxPayloadSize) {
    _maxPayloadSize = maxPayloadSize;
  }

  public String getExcludedEndpoints() {
    return _excludedEndpoints;
  }

  public void setExcludedEndpoints(String excludedEndpoints) {
    _excludedEndpoints = excludedEndpoints;
  }

  @Override
  public String toString() {
    return "AuditConfig{" + "enabled=" + _enabled + ", captureRequestPayload=" + _captureRequestPayload
        + ", captureRequestHeaders=" + _captureRequestHeaders + ", maxPayloadSize=" + _maxPayloadSize
        + ", excludedEndpoints='" + _excludedEndpoints + "'}";
  }
}
