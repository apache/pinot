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
import java.util.StringJoiner;


/**
 * Pure data class for audit logging configuration.
 * Uses Jackson annotations for automatic JSON mapping from ClusterConfiguration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class AuditConfig {

  public static final int MAX_AUDIT_PAYLOAD_SIZE_BYTES = 65536; // Hard max. This overrides _maxPayloadSize
  public static final int MAX_AUDIT_PAYLOAD_SIZE_BYTES_DEFAULT = 8192;

  @JsonProperty("enabled")
  private boolean _enabled = false;

  @JsonProperty("capture.request.payload.enabled")
  private boolean _captureRequestPayload = false;

  @JsonProperty("capture.request.headers")
  private String _captureRequestHeaders = "";

  @JsonProperty("request.payload.size.max.bytes")
  private int _maxPayloadSize = MAX_AUDIT_PAYLOAD_SIZE_BYTES_DEFAULT;

  @JsonProperty("url.filter.exclude.patterns")
  private String _urlFilterExcludePatterns = "";

  @JsonProperty("userid.header")
  private String _useridHeader = "";

  @JsonProperty("userid.jwt.claim")
  private String _useridJwtClaimName = "";

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

  public String getCaptureRequestHeaders() {
    return _captureRequestHeaders;
  }

  public void setCaptureRequestHeaders(String captureRequestHeaders) {
    _captureRequestHeaders = captureRequestHeaders;
  }

  public int getMaxPayloadSize() {
    return _maxPayloadSize;
  }

  public void setMaxPayloadSize(int maxPayloadSize) {
    _maxPayloadSize = maxPayloadSize;
  }

  public String getUrlFilterExcludePatterns() {
    return _urlFilterExcludePatterns;
  }

  public void setUrlFilterExcludePatterns(String urlFilterExcludePatterns) {
    _urlFilterExcludePatterns = urlFilterExcludePatterns;
  }

  public String getUseridHeader() {
    return _useridHeader;
  }

  public void setUseridHeader(String useridHeader) {
    _useridHeader = useridHeader;
  }

  public String getUseridJwtClaimName() {
    return _useridJwtClaimName;
  }

  public void setUseridJwtClaimName(String useridJwtClaimName) {
    _useridJwtClaimName = useridJwtClaimName;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", AuditConfig.class.getSimpleName() + "[", "]").add("_enabled=" + _enabled)
        .add("_captureRequestPayload=" + _captureRequestPayload)
        .add("_captureRequestHeaders='" + _captureRequestHeaders + "'")
        .add("_maxPayloadSize=" + _maxPayloadSize)
        .add("_urlFilterExcludePatterns='" + _urlFilterExcludePatterns + "'")
        .add("_useridHeader='" + _useridHeader + "'")
        .add("_useridJwtClaimName='" + _useridJwtClaimName + "'")
        .toString();
  }
}
