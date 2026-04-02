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
package org.apache.pinot.controller.helix.core.ingest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Metadata manifest for a single INSERT INTO statement.
 *
 * <p>Persisted in ZooKeeper as a JSON-serialized ZNRecord so that the coordinator can
 * recover statement state across controller restarts. The manifest tracks the full lifecycle
 * of the statement from creation through garbage collection.
 *
 * <p>Instances are mutable but should only be modified under the coordinator's lock.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InsertStatementManifest {
  private final String _statementId;
  private final String _requestId;
  private final String _payloadHash;
  private final String _tableNameWithType;
  private final InsertType _insertType;
  private volatile InsertStatementState _state;
  private final long _createdTimeMs;
  private volatile long _lastUpdatedTimeMs;
  private volatile List<String> _segmentNames;
  private volatile String _errorMessage;
  private volatile String _lineageEntryId;

  @JsonCreator
  public InsertStatementManifest(
      @JsonProperty("statementId") String statementId,
      @JsonProperty("requestId") String requestId,
      @JsonProperty("payloadHash") String payloadHash,
      @JsonProperty("tableNameWithType") String tableNameWithType,
      @JsonProperty("insertType") InsertType insertType,
      @JsonProperty("state") InsertStatementState state,
      @JsonProperty("createdTimeMs") long createdTimeMs,
      @JsonProperty("lastUpdatedTimeMs") long lastUpdatedTimeMs,
      @JsonProperty("segmentNames") @Nullable List<String> segmentNames,
      @JsonProperty("errorMessage") @Nullable String errorMessage,
      @JsonProperty("lineageEntryId") @Nullable String lineageEntryId) {
    _statementId = statementId;
    _requestId = requestId;
    _payloadHash = payloadHash;
    _tableNameWithType = tableNameWithType;
    _insertType = insertType;
    _state = state;
    _createdTimeMs = createdTimeMs;
    _lastUpdatedTimeMs = lastUpdatedTimeMs;
    _segmentNames = segmentNames != null ? new ArrayList<>(segmentNames) : new ArrayList<>();
    _errorMessage = errorMessage;
    _lineageEntryId = lineageEntryId;
  }

  @JsonProperty("statementId")
  public String getStatementId() {
    return _statementId;
  }

  @JsonProperty("requestId")
  public String getRequestId() {
    return _requestId;
  }

  @JsonProperty("payloadHash")
  public String getPayloadHash() {
    return _payloadHash;
  }

  @JsonProperty("tableNameWithType")
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  @JsonProperty("insertType")
  public InsertType getInsertType() {
    return _insertType;
  }

  @JsonProperty("state")
  public InsertStatementState getState() {
    return _state;
  }

  public void setState(InsertStatementState state) {
    _state = state;
    _lastUpdatedTimeMs = System.currentTimeMillis();
  }

  @JsonProperty("createdTimeMs")
  public long getCreatedTimeMs() {
    return _createdTimeMs;
  }

  @JsonProperty("lastUpdatedTimeMs")
  public long getLastUpdatedTimeMs() {
    return _lastUpdatedTimeMs;
  }

  public void setLastUpdatedTimeMs(long lastUpdatedTimeMs) {
    _lastUpdatedTimeMs = lastUpdatedTimeMs;
  }

  @JsonProperty("segmentNames")
  public List<String> getSegmentNames() {
    return Collections.unmodifiableList(_segmentNames);
  }

  public void setSegmentNames(List<String> segmentNames) {
    _segmentNames = new ArrayList<>(segmentNames);
    _lastUpdatedTimeMs = System.currentTimeMillis();
  }

  @JsonProperty("errorMessage")
  @Nullable
  public String getErrorMessage() {
    return _errorMessage;
  }

  public void setErrorMessage(@Nullable String errorMessage) {
    _errorMessage = errorMessage;
    _lastUpdatedTimeMs = System.currentTimeMillis();
  }

  @JsonProperty("lineageEntryId")
  @Nullable
  public String getLineageEntryId() {
    return _lineageEntryId;
  }

  public void setLineageEntryId(@Nullable String lineageEntryId) {
    _lineageEntryId = lineageEntryId;
    _lastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * Serializes this manifest to a JSON string for ZK persistence.
   */
  public String toJsonString()
      throws IOException {
    return JsonUtils.objectToString(this);
  }

  /**
   * Deserializes a manifest from a JSON string read from ZK.
   */
  public static InsertStatementManifest fromJsonString(String json)
      throws IOException {
    return JsonUtils.stringToObject(json, InsertStatementManifest.class);
  }
}
