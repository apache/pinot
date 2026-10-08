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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.utils.JsonUtils;


/// Persisted result of one synchronous ROW insert. An ACCEPTED result can remain after a controller
/// crash; retaining its requestId prevents the retry from inserting duplicate data. Each mutable
/// manifest is owned by its submission thread and is never shared between threads.
public class InsertStatementManifest {
  private final String _statementId;
  private final String _requestId;
  private final String _payloadHash;
  private final String _tableNameWithType;
  private final InsertType _insertType;
  private InsertStatementState _state;
  private final long _createdTimeMs;
  private long _lastUpdatedTimeMs;
  private List<String> _segmentNames;
  private String _errorMessage;
  private String _errorCode;

  @JsonCreator
  public InsertStatementManifest(@JsonProperty("statementId") String statementId,
      @JsonProperty("requestId") @Nullable String requestId,
      @JsonProperty("payloadHash") @Nullable String payloadHash,
      @JsonProperty("tableNameWithType") String tableNameWithType,
      @JsonProperty("insertType") InsertType insertType,
      @JsonProperty("state") InsertStatementState state,
      @JsonProperty("createdTimeMs") long createdTimeMs,
      @JsonProperty("lastUpdatedTimeMs") long lastUpdatedTimeMs,
      @JsonProperty("segmentNames") @Nullable List<String> segmentNames,
      @JsonProperty("errorMessage") @Nullable String errorMessage,
      @JsonProperty("errorCode") @Nullable String errorCode) {
    _statementId = Objects.requireNonNull(statementId, "statementId");
    _requestId = requestId;
    _payloadHash = payloadHash;
    _tableNameWithType = Objects.requireNonNull(tableNameWithType, "tableNameWithType");
    _insertType = Objects.requireNonNull(insertType, "insertType");
    _state = Objects.requireNonNull(state, "state");
    _createdTimeMs = createdTimeMs;
    _lastUpdatedTimeMs = lastUpdatedTimeMs;
    _segmentNames = segmentNames != null ? List.copyOf(segmentNames) : List.of();
    _errorMessage = errorMessage;
    _errorCode = errorCode;
  }

  public String getStatementId() {
    return _statementId;
  }

  @Nullable
  public String getRequestId() {
    return _requestId;
  }

  @Nullable
  public String getPayloadHash() {
    return _payloadHash;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public InsertType getInsertType() {
    return _insertType;
  }

  public InsertStatementState getState() {
    return _state;
  }

  public long getCreatedTimeMs() {
    return _createdTimeMs;
  }

  public long getLastUpdatedTimeMs() {
    return _lastUpdatedTimeMs;
  }

  public List<String> getSegmentNames() {
    return _segmentNames;
  }

  @Nullable
  public String getErrorMessage() {
    return _errorMessage;
  }

  @Nullable
  public String getErrorCode() {
    return _errorCode;
  }


  public void setState(InsertStatementState state) {
    _state = state;
    _lastUpdatedTimeMs = System.currentTimeMillis();
  }

  public void setSegmentNames(List<String> segmentNames) {
    _segmentNames = List.copyOf(segmentNames);
  }

  public void setErrorMessage(@Nullable String errorMessage) {
    _errorMessage = errorMessage;
  }

  public void setErrorCode(@Nullable String errorCode) {
    _errorCode = errorCode;
  }

  public String toJsonString() throws IOException {
    return JsonUtils.objectToString(this);
  }

  public static InsertStatementManifest fromJsonString(String json) throws IOException {
    return JsonUtils.stringToObject(json, InsertStatementManifest.class);
  }
}
