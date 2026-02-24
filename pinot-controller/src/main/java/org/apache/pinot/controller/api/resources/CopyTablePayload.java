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

/**
 * Payload for the copy table request.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CopyTablePayload {

  /**
   * Job type for table copy operation.
   */
  public enum JobType {
    /**
     * Controller-based job type (default). The copy job runs on the controller.
     */
    CONTROLLER,

    /**
     * Minion-based job type (not yet supported). The copy job would run on minion workers.
     */
    MINION
  }

  private String _sourceClusterUri;
  private Map<String, String> _headers;

  private String _destinationClusterUri;
  private Map<String, String> _destinationClusterHeaders;
  /**
   * Broker tenant for the new table.
   * MUST NOT contain the tenant type suffix, i.e. _BROKER.
   */
  private String _brokerTenant;
  /**
   * Server tenant for the new table.
   * MUST NOT contain the tenant type suffix, i.e. _REALTIME or _OFFLINE.
   */
  private String _serverTenant;
  private Integer _backfillParallism;

  /**
   * Job type for the copy operation.
   * Defaults to CONTROLLER if not specified.
   * Currently only CONTROLLER is supported.
   */
  private JobType _jobType;

  /**
   * The instanceAssignmentConfig's tagPoolConfig contains full tenant name. We will use this field to let user specify
   * the replacement relation from source cluster's full tenant to target cluster's full tenant.
   */
  private Map<String, String> _tagPoolReplacementMap;

  @JsonCreator
  public CopyTablePayload(
      @JsonProperty(value = "sourceClusterUri", required = true) String sourceClusterUri,
      @JsonProperty("sourceClusterHeaders") Map<String, String> headers,
      @JsonProperty(value = "destinationClusterUri", required = true) String destinationClusterUri,
      @JsonProperty(value = "destinationClusterHeaders") Map<String, String> destinationClusterHeaders,
      @JsonProperty(value = "brokerTenant", required = true) String brokerTenant,
      @JsonProperty(value = "serverTenant", required = true) String serverTenant,
      @JsonProperty("tagPoolReplacementMap") @Nullable Map<String, String> tagPoolReplacementMap,
      @JsonProperty("backfillParallism") @Nullable Integer backfillParallism,
      @JsonProperty("jobType") @Nullable JobType jobType) {
    _sourceClusterUri = sourceClusterUri;
    _headers = headers;
    _destinationClusterUri = destinationClusterUri;
    _destinationClusterHeaders = destinationClusterHeaders;
    _brokerTenant = brokerTenant;
    _serverTenant = serverTenant;
    _tagPoolReplacementMap = tagPoolReplacementMap;
    _backfillParallism = backfillParallism;
    _jobType = jobType != null ? jobType : JobType.CONTROLLER;
  }

  @JsonGetter("sourceClusterUri")
  public String getSourceClusterUri() {
    return _sourceClusterUri;
  }

  @JsonGetter("sourceClusterHeaders")
  public Map<String, String> getHeaders() {
    return _headers;
  }

  @JsonGetter("destinationClusterUri")
  public String getDestinationClusterUri() {
    return _destinationClusterUri;
  }

  @JsonGetter("destinationClusterHeaders")
  public Map<String, String> getDestinationClusterHeaders() {
    return _destinationClusterHeaders;
  }

  @JsonGetter("brokerTenant")
  public String getBrokerTenant() {
    return _brokerTenant;
  }

  @JsonGetter("serverTenant")
  public String getServerTenant() {
    return _serverTenant;
  }

  @JsonGetter("backfillParallism")
  public Integer getBackfillParallism() {
    return _backfillParallism;
  }

  @JsonGetter("tagPoolReplacementMap")
  public Map<String, String> getTagPoolReplacementMap() {
    return _tagPoolReplacementMap;
  }

  @JsonGetter("jobType")
  public JobType getJobType() {
    return _jobType;
  }
}
