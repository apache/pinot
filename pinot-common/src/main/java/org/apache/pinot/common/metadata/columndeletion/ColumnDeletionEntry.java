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
package org.apache.pinot.common.metadata.columndeletion;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


/// Immutable record for one explicit column deletion on a table.
///
/// {@code deletionEpochMs} and {@code schemaZkVersion} are captured when the deletion is accepted
/// and must not be rewritten after later unrelated schema edits. Progress fields
/// ({@code lastError}, {@code outstandingSegmentCount}) may change. Thread-safe because instances
/// are immutable; updates return a new object.
public final class ColumnDeletionEntry {
  private final String _columnName;
  private final String _deletionId;
  private final long _deletionEpochMs;
  private final int _schemaZkVersion;
  private final long _schemaZkMtimeMs;
  private final ColumnDeletionState _state;
  @Nullable
  private final String _lastError;
  private final int _outstandingSegmentCount;

  public ColumnDeletionEntry(String columnName, String deletionId, long deletionEpochMs, int schemaZkVersion,
      ColumnDeletionState state) {
    this(columnName, deletionId, deletionEpochMs, schemaZkVersion, -1L, state, null, 0);
  }

  public ColumnDeletionEntry(String columnName, String deletionId, long deletionEpochMs, int schemaZkVersion,
      long schemaZkMtimeMs, ColumnDeletionState state, @Nullable String lastError, int outstandingSegmentCount) {
    Preconditions.checkArgument(StringUtils.isNotBlank(columnName), "columnName is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(deletionId), "deletionId is required");
    Preconditions.checkArgument(deletionEpochMs >= 0, "deletionEpochMs must be >= 0");
    Preconditions.checkNotNull(state, "state");
    Preconditions.checkArgument(outstandingSegmentCount >= 0, "outstandingSegmentCount must be >= 0");
    _columnName = columnName;
    _deletionId = deletionId;
    _deletionEpochMs = deletionEpochMs;
    _schemaZkVersion = schemaZkVersion;
    _schemaZkMtimeMs = schemaZkMtimeMs;
    _state = state;
    _lastError = lastError;
    _outstandingSegmentCount = outstandingSegmentCount;
  }

  public String getColumnName() {
    return _columnName;
  }

  public String getDeletionId() {
    return _deletionId;
  }

  /// Immutable deletion epoch. Later schema znode mtimes must not replace this value.
  public long getDeletionEpochMs() {
    return _deletionEpochMs;
  }

  /// Schema znode version associated with this deletion event. May be the pre-write version
  /// while {@link ColumnDeletionState#PREPARED}, then the post-write version after advance.
  public int getSchemaZkVersion() {
    return _schemaZkVersion;
  }

  /// Schema znode mtime of the deletion event, or {@code -1} when unknown.
  public long getSchemaZkMtimeMs() {
    return _schemaZkMtimeMs;
  }

  public ColumnDeletionState getState() {
    return _state;
  }

  @Nullable
  public String getLastError() {
    return _lastError;
  }

  public int getOutstandingSegmentCount() {
    return _outstandingSegmentCount;
  }

  public boolean blocksReAdd() {
    return _state.blocksReAdd();
  }

  public ColumnDeletionEntry withState(ColumnDeletionState state) {
    return new ColumnDeletionEntry(_columnName, _deletionId, _deletionEpochMs, _schemaZkVersion, _schemaZkMtimeMs,
        state, _lastError, _outstandingSegmentCount);
  }

  public ColumnDeletionEntry withSchemaZkVersion(int schemaZkVersion) {
    return new ColumnDeletionEntry(_columnName, _deletionId, _deletionEpochMs, schemaZkVersion, _schemaZkMtimeMs,
        _state, _lastError, _outstandingSegmentCount);
  }

  public ColumnDeletionEntry withLastError(@Nullable String lastError) {
    return new ColumnDeletionEntry(_columnName, _deletionId, _deletionEpochMs, _schemaZkVersion, _schemaZkMtimeMs,
        _state, lastError, _outstandingSegmentCount);
  }

  public ColumnDeletionEntry withOutstandingSegmentCount(int outstandingSegmentCount) {
    return new ColumnDeletionEntry(_columnName, _deletionId, _deletionEpochMs, _schemaZkVersion, _schemaZkMtimeMs,
        _state, _lastError, outstandingSegmentCount);
  }

  public ColumnDeletionEntry withSchemaZkMtimeMs(long schemaZkMtimeMs) {
    return new ColumnDeletionEntry(_columnName, _deletionId, _deletionEpochMs, _schemaZkVersion, schemaZkMtimeMs,
        _state, _lastError, _outstandingSegmentCount);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnDeletionEntry that = (ColumnDeletionEntry) o;
    return _deletionEpochMs == that._deletionEpochMs && _schemaZkVersion == that._schemaZkVersion
        && _schemaZkMtimeMs == that._schemaZkMtimeMs && _outstandingSegmentCount == that._outstandingSegmentCount
        && _columnName.equals(that._columnName) && _deletionId.equals(that._deletionId) && _state == that._state
        && Objects.equals(_lastError, that._lastError);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_columnName, _deletionId, _deletionEpochMs, _schemaZkVersion, _schemaZkMtimeMs, _state,
        _lastError, _outstandingSegmentCount);
  }

  @Override
  public String toString() {
    return "ColumnDeletionEntry{columnName='" + _columnName + "', deletionId='" + _deletionId + "', state=" + _state
        + ", deletionEpochMs=" + _deletionEpochMs + '}';
  }
}
