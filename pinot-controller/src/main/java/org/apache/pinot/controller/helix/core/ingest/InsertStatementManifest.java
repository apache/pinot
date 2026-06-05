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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.ingest.InsertStatementState;
import org.apache.pinot.spi.ingest.InsertType;
import org.apache.pinot.spi.utils.JsonUtils;


/// Metadata manifest for a single INSERT INTO statement.
///
/// Persisted in ZooKeeper as a JSON-serialized ZNRecord so that the coordinator can
/// recover statement state across controller restarts. The manifest tracks the full lifecycle
/// of the statement from creation through garbage collection.
///
/// Instances are mutable but **NOT shared across threads**. The CAS-driven write
/// pattern in `InsertStatementCoordinator.persistWithCasRetry` re-reads the manifest fresh
/// from ZK on every attempt and applies a single mutator — each instance is owned by one thread for
/// the lifetime of one CAS attempt. Per-getter/setter synchronization would only be defensive
/// paranoia and would slow down Jackson serialization (one monitor acquisition per field); the
/// CAS version-check at ZK is the authoritative cross-instance safety mechanism, and callers that
/// mutate the same instance from multiple threads are using the manifest incorrectly.
public class InsertStatementManifest {
  /// Current schema version for this JSON shape. Bump when adding a field that changes semantics
  /// or renaming an existing field; older controllers reading a newer manifest can gate behavior
  /// on `schemaVersion <= MAX_SUPPORTED_VERSION` instead of silently dropping unknown data.
  /// Older manifests missing this field deserialize as `1`.
  public static final int CURRENT_SCHEMA_VERSION = 1;

  /// Highest schema version this controller can read safely. Equal to {@link #CURRENT_SCHEMA_VERSION}
  /// for the current build; bump only when the new format is fully backward-readable. A manifest
  /// persisted with a higher version (e.g., from a forward-rolled controller in a mixed-version
  /// cluster) is rejected at deserialization rather than silently parsed with unknown fields
  /// dropped — silent partial reads are how distributed-state bugs hide.
  public static final int MAX_SUPPORTED_VERSION = CURRENT_SCHEMA_VERSION;

  /// Schema version assigned to legacy manifests written before the `schemaVersion` field was
  /// introduced. Always `1` regardless of {@link #CURRENT_SCHEMA_VERSION} — only manifests
  /// with no field map to legacy.
  private static final int LEGACY_SCHEMA_VERSION = 1;

  private final int _schemaVersion;
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
  /// Operator-visible note attached to a non-error state (e.g., a VISIBLE statement that was
  /// auto-completed by the cleanup sweep without explicit segment-name surfacing). Separate from
  /// {@link #_errorMessage} so UI/JDBC clients reading the manifest don't confuse "informational
  /// note on success" with "executor failed".
  private String _informationalMessage;
  private String _lineageEntryId;
  /// Helix/Pinot task name returned by
  /// {@link org.apache.pinot.controller.helix.core.minion.PinotTaskManager#createTask} for FILE
  /// inserts. Persisted to ZK so that a new controller leader can still poll task state after
  /// failover without relying on the in-memory `_taskNames` map.
  private String _minionTaskName;

  /// Forward-compatibility carrier: any JSON fields present in a serialized manifest that are not
  /// known to this controller version are captured here and written back unchanged on the next CAS
  /// round-trip. Without this, a v1 reader CAS-rewriting a v2-written manifest would silently
  /// strip new fields, corrupting state during rolling upgrades. Pre-versioning manifests have
  /// no extra fields; this map is empty for them.
  private final Map<String, Object> _unknownFields = new HashMap<>();

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
      @JsonProperty("informationalMessage") @Nullable String informationalMessage,
      @JsonProperty("lineageEntryId") @Nullable String lineageEntryId,
      @JsonProperty("minionTaskName") @Nullable String minionTaskName,
      @JsonProperty("schemaVersion") @Nullable Integer schemaVersion) {
    /// Default missing schemaVersion to 1 — pre-versioning manifests are schema v1.
    _schemaVersion = schemaVersion != null ? schemaVersion : LEGACY_SCHEMA_VERSION;
    /// Reject malformed manifests at deserialization. Null required fields would silently
    /// propagate through the precondition predicates in persistWithCasRetry and through ZK-path
    /// construction, manifesting as NPE deep in cleanup-sweep code rather than a clear "bad
    /// manifest" error. Surface the malformed payload here so /insert/list never returns rows
    /// that would crash subsequent operations.
    if (statementId == null) {
      throw new IllegalArgumentException("InsertStatementManifest.statementId is required");
    }
    if (tableNameWithType == null) {
      throw new IllegalArgumentException(
          "InsertStatementManifest.tableNameWithType is required (statementId=" + statementId + ")");
    }
    if (state == null) {
      throw new IllegalArgumentException("InsertStatementManifest.state is required (statementId="
          + statementId + ", tableNameWithType=" + tableNameWithType + ")");
    }
    if (insertType == null) {
      throw new IllegalArgumentException("InsertStatementManifest.insertType is required (statementId="
          + statementId + ", tableNameWithType=" + tableNameWithType + ")");
    }
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
    _informationalMessage = informationalMessage;
    _lineageEntryId = lineageEntryId;
    _minionTaskName = minionTaskName;
  }

  /// Convenience constructor that stamps {@link #CURRENT_SCHEMA_VERSION} automatically. Use this
  /// when creating new manifests (write path); the `@JsonCreator` constructor is used by
  /// Jackson on the read path and honors the persisted version.
  public InsertStatementManifest(String statementId, String requestId, String payloadHash,
      String tableNameWithType, InsertType insertType, InsertStatementState state, long createdTimeMs,
      long lastUpdatedTimeMs, @Nullable List<String> segmentNames, @Nullable String errorMessage,
      @Nullable String lineageEntryId, @Nullable String minionTaskName) {
    this(statementId, requestId, payloadHash, tableNameWithType, insertType, state,
        createdTimeMs, lastUpdatedTimeMs, segmentNames, errorMessage, null, lineageEntryId, minionTaskName,
        CURRENT_SCHEMA_VERSION);
  }

  @JsonProperty("schemaVersion")
  public int getSchemaVersion() {
    return _schemaVersion;
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

  @JsonProperty("informationalMessage")
  @Nullable
  public String getInformationalMessage() {
    return _informationalMessage;
  }

  public void setInformationalMessage(@Nullable String informationalMessage) {
    _informationalMessage = informationalMessage;
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

  @JsonProperty("minionTaskName")
  @Nullable
  public String getMinionTaskName() {
    return _minionTaskName;
  }

  public void setMinionTaskName(@Nullable String minionTaskName) {
    _minionTaskName = minionTaskName;
    _lastUpdatedTimeMs = System.currentTimeMillis();
  }

  /// Captures unknown JSON fields encountered during deserialization. Jackson invokes this for
  /// each top-level property not bound to a known field. The value type is `Object` because
  /// Jackson hands us already-parsed JsonNode-equivalents (numbers, strings, arrays, maps).
  @JsonAnySetter
  public void setUnknownField(String key, Object value) {
    _unknownFields.put(key, value);
  }

  /// Emits unknown JSON fields back during serialization so a v1 controller's CAS read-modify-write
  /// preserves a v2-written manifest's new fields. Annotated `@JsonAnyGetter` so Jackson
  /// inlines the map's entries at the top level rather than nesting them under a key.
  @JsonAnyGetter
  public Map<String, Object> getUnknownFields() {
    return _unknownFields;
  }

  /// Returns the unknown-field map for testing. Production callers do not need this.
  @JsonIgnore
  public boolean hasUnknownFields() {
    return !_unknownFields.isEmpty();
  }

  /// Serializes this manifest to a JSON string for ZK persistence.
  public String toJsonString()
      throws IOException {
    return JsonUtils.objectToString(this);
  }

  /// Deserializes a manifest from a JSON string read from ZK. Rejects manifests written under a
  /// schema version this controller does not understand — silent partial reads of forward-only
  /// fields are a distributed-state hazard during rolling upgrades or rollbacks.
  public static InsertStatementManifest fromJsonString(String json)
      throws IOException {
    InsertStatementManifest manifest = JsonUtils.stringToObject(json, InsertStatementManifest.class);
    if (manifest.getSchemaVersion() > MAX_SUPPORTED_VERSION) {
      throw new IOException("Manifest schemaVersion=" + manifest.getSchemaVersion()
          + " exceeds max supported version=" + MAX_SUPPORTED_VERSION
          + " for this controller; refuse to read forward-incompatible state. "
          + "Upgrade this controller to read manifests written by a newer peer.");
    }
    return manifest;
  }
}
