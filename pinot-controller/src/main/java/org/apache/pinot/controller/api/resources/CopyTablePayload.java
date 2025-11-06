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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CopyTablePayload {

  String _sourceClusterUri;
  Map<String, String> _headers;
  /**
   * Broker tenant for the new table.
   * MUST NOT contain the tenant type suffix, i.e. _BROKER.
   */
  String _brokerTenant;
  /**
   * Server tenant for the new table.
   * MUST NOT contain the tenant type suffix, i.e. _REALTIME or _OFFLINE.
   */
  String _serverTenant;

  Map<String, String> _tagPoolReplacementMap;

  @JsonCreator
  public CopyTablePayload(@JsonProperty(value = "tableName", required = true) String tableName,
      @JsonProperty(value = "sourceClusterUri", required = true) String sourceClusterUri,
      @JsonProperty("sourceClusterHeaders") Map<String, String> headers,
      @JsonProperty(value = "brokerTenant", required = true) String brokerTenant,
      @JsonProperty(value = "serverTenant", required = true) String serverTenant,
      @JsonProperty("tagPoolReplacementMap") @Nullable Map<String, String> tagPoolReplacementMap) {
    _sourceClusterUri = sourceClusterUri;
    _headers = headers;
    _brokerTenant = brokerTenant;
    _serverTenant = serverTenant;
    _tagPoolReplacementMap = tagPoolReplacementMap;
  }

  @JsonGetter("sourceClusterUri")
  public String getSourceClusterUri() {
    return _sourceClusterUri;
  }

  @JsonGetter("sourceClusterHeaders")
  public Map<String, String> getHeaders() {
    return _headers;
  }

  @JsonGetter("brokerTenant")
  public String getBrokerTenant() {
    return _brokerTenant;
  }

  @JsonGetter("serverTenant")
  public String getServerTenant() {
    return _serverTenant;
  }

  @JsonGetter("tagPoolReplacementMap")
  public Map<String, String> getTagPoolReplacementMap() {
    return _tagPoolReplacementMap;
  }
}
