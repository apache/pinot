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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;


/**
 * Data class representing an audit event for Pinot Controller API requests.
 * Contains all required fields as specified in the Phase 1 audit logging specification.
 */
public class AuditEvent {

  @JsonProperty("timestamp")
  private String _timestamp;

  @JsonProperty("service_id")
  private String _serviceId;

  @JsonProperty("endpoint")
  private String _endpoint;

  @JsonProperty("method")
  private String _method;

  @JsonProperty("origin_ip_address")
  private String _originIpAddress;

  @JsonProperty("user_id")
  private String _userId;

  @JsonProperty("request")
  private Object _request;

  public AuditEvent() {
    // Default constructor for Jackson
  }

  public AuditEvent(String serviceId, String endpoint, String method, String originIpAddress, String userId,
      Object request) {
    _timestamp = Instant.now().toString();
    _serviceId = serviceId;
    _endpoint = endpoint;
    _method = method;
    _originIpAddress = originIpAddress;
    _userId = userId;
    _request = request;
  }

  public String getTimestamp() {
    return _timestamp;
  }

  public AuditEvent setTimestamp(String timestamp) {
    _timestamp = timestamp;
    return this;
  }

  public String getServiceId() {
    return _serviceId;
  }

  public AuditEvent setServiceId(String serviceId) {
    _serviceId = serviceId;
    return this;
  }

  public String getEndpoint() {
    return _endpoint;
  }

  public AuditEvent setEndpoint(String endpoint) {
    _endpoint = endpoint;
    return this;
  }

  public String getMethod() {
    return _method;
  }

  public AuditEvent setMethod(String method) {
    _method = method;
    return this;
  }

  public String getOriginIpAddress() {
    return _originIpAddress;
  }

  public AuditEvent setOriginIpAddress(String originIpAddress) {
    _originIpAddress = originIpAddress;
    return this;
  }

  public String getUserId() {
    return _userId;
  }

  public AuditEvent setUserId(String userId) {
    _userId = userId;
    return this;
  }

  public Object getRequest() {
    return _request;
  }

  public AuditEvent setRequest(Object request) {
    _request = request;
    return this;
  }
}
