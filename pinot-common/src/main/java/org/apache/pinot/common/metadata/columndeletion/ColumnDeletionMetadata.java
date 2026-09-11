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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.data.SchemaDiff;


/// Table-scoped column-deletion ledger stored at
/// {@code /COLUMN_DELETION_METADATA/<tableNameWithType>}.
///
/// One znode holds every explicit deletion for that table, including {@link ColumnDeletionState#COMPLETE}
/// tombstones. The record is format-versioned. Readers refuse versions newer than
/// {@link #CURRENT_FORMAT_VERSION} so a mixed-version controller cannot clobber a znode it cannot
/// parse.
///
/// This object is not thread-safe. Concurrent writers must read-modify-write through
/// {@link ColumnDeletionMetadataAccessHelper} using the PropertyStore expected version.
public final class ColumnDeletionMetadata {
  public static final int CURRENT_FORMAT_VERSION = 1;

  private static final String FORMAT_VERSION_KEY = "formatVersion";
  private static final String COLUMN_NAME_KEY = "columnName";
  private static final String DELETION_ID_KEY = "deletionId";
  private static final String DELETION_EPOCH_MS_KEY = "deletionEpochMs";
  private static final String SCHEMA_ZK_VERSION_KEY = "schemaZkVersion";
  private static final String SCHEMA_ZK_MTIME_MS_KEY = "schemaZkMtimeMs";
  private static final String STATE_KEY = "state";
  private static final String LAST_ERROR_KEY = "lastError";
  private static final String OUTSTANDING_SEGMENT_COUNT_KEY = "outstandingSegmentCount";

  private final String _tableNameWithType;
  private final int _formatVersion;
  private final Map<String, ColumnDeletionEntry> _entries;

  public ColumnDeletionMetadata(String tableNameWithType) {
    this(tableNameWithType, CURRENT_FORMAT_VERSION, new LinkedHashMap<>());
  }

  public ColumnDeletionMetadata(String tableNameWithType, int formatVersion, Map<String, ColumnDeletionEntry> entries) {
    Preconditions.checkArgument(StringUtils.isNotBlank(tableNameWithType), "tableNameWithType is required");
    Preconditions.checkArgument(formatVersion >= 1, "formatVersion must be >= 1");
    _tableNameWithType = tableNameWithType;
    _formatVersion = formatVersion;
    _entries = new LinkedHashMap<>(entries);
    checkNoDuplicateActiveColumns(_entries.values(), false);
  }

  public static String newDeletionId() {
    return UUID.randomUUID().toString();
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public int getFormatVersion() {
    return _formatVersion;
  }

  /// Unmodifiable view of entries keyed by deletion id, in insertion order.
  public Map<String, ColumnDeletionEntry> getEntries() {
    return Collections.unmodifiableMap(_entries);
  }

  @Nullable
  public ColumnDeletionEntry getEntry(String deletionId) {
    return _entries.get(deletionId);
  }

  public void addEntry(ColumnDeletionEntry entry) {
    addEntry(entry, false);
  }

  /// Adds an entry. Rejects a second {@link ColumnDeletionEntry#blocksReAdd()} row for the same
  /// column. A {@link ColumnDeletionState#COMPLETE} tombstone plus a new active row is allowed.
  public void addEntry(ColumnDeletionEntry entry, boolean ignoreCase) {
    Preconditions.checkNotNull(entry, "entry");
    Preconditions.checkArgument(!_entries.containsKey(entry.getDeletionId()),
        "Deletion id '%s' already exists", entry.getDeletionId());
    if (entry.blocksReAdd() && findActiveEntryForColumn(entry.getColumnName(), ignoreCase) != null) {
      throw new IllegalArgumentException(
          "Active column deletion already exists for column '" + entry.getColumnName() + "'");
    }
    _entries.put(entry.getDeletionId(), entry);
  }

  public void updateEntry(ColumnDeletionEntry entry) {
    updateEntry(entry, false);
  }

  public void updateEntry(ColumnDeletionEntry entry, boolean ignoreCase) {
    Preconditions.checkNotNull(entry, "entry");
    Preconditions.checkArgument(_entries.containsKey(entry.getDeletionId()),
        "Deletion id '%s' does not exist", entry.getDeletionId());
    if (entry.blocksReAdd()) {
      ColumnDeletionEntry existingActive = findActiveEntryForColumn(entry.getColumnName(), ignoreCase);
      if (existingActive != null && !existingActive.getDeletionId().equals(entry.getDeletionId())) {
        throw new IllegalArgumentException(
            "Active column deletion already exists for column '" + entry.getColumnName() + "'");
      }
    }
    _entries.put(entry.getDeletionId(), entry);
  }

  public void removeEntry(String deletionId) {
    _entries.remove(deletionId);
  }

  /// Names from every non-{@link ColumnDeletionState#COMPLETE} entry.
  ///
  /// When {@code ignoreCase} is true the returned names are {@link Locale#ROOT} lowercased so callers
  /// can compare with the same rule used by schema validation.
  public Set<String> getActiveDeletedColumnNames(boolean ignoreCase) {
    Set<String> names = new TreeSet<>();
    for (ColumnDeletionEntry entry : _entries.values()) {
      if (entry.blocksReAdd()) {
        names.add(SchemaDiff.normalizeColumnName(entry.getColumnName(), ignoreCase));
      }
    }
    return names;
  }

  /// True when {@code columnName} matches an active (non-complete) deletion.
  public boolean blocksReAdd(String columnName, boolean ignoreCase) {
    Preconditions.checkArgument(StringUtils.isNotBlank(columnName), "columnName is required");
    for (ColumnDeletionEntry entry : _entries.values()) {
      if (entry.blocksReAdd() && SchemaDiff.columnNamesEqual(entry.getColumnName(), columnName, ignoreCase)) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  public ColumnDeletionEntry findActiveEntryForColumn(String columnName, boolean ignoreCase) {
    for (ColumnDeletionEntry entry : _entries.values()) {
      if (entry.blocksReAdd() && SchemaDiff.columnNamesEqual(entry.getColumnName(), columnName, ignoreCase)) {
        return entry;
      }
    }
    return null;
  }

  /// Recover a {@link ColumnDeletionState#PREPARED} entry after controller restart.
  ///
  /// If the column is still in the schema, the schema write never landed and the entry is removed.
  /// If the column is already absent, the schema write landed and the entry becomes
  /// {@link ColumnDeletionState#RECLAIMING}.
  ///
  /// @return the post-reconciliation entry, or {@code null} when the prepared entry was aborted
  @Nullable
  public ColumnDeletionEntry reconcilePreparedEntry(String deletionId, boolean columnStillInSchema) {
    ColumnDeletionEntry entry = _entries.get(deletionId);
    Preconditions.checkArgument(entry != null, "Deletion id '%s' does not exist", deletionId);
    if (entry.getState() != ColumnDeletionState.PREPARED) {
      return entry;
    }
    if (columnStillInSchema) {
      _entries.remove(deletionId);
      return null;
    }
    ColumnDeletionEntry advanced = entry.withState(ColumnDeletionState.RECLAIMING);
    _entries.put(deletionId, advanced);
    return advanced;
  }

  public ZNRecord toZNRecord() {
    if (_formatVersion > CURRENT_FORMAT_VERSION) {
      throw new ColumnDeletionUnsupportedFormatException(_formatVersion, CURRENT_FORMAT_VERSION);
    }
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setSimpleField(FORMAT_VERSION_KEY, Integer.toString(CURRENT_FORMAT_VERSION));
    for (ColumnDeletionEntry entry : _entries.values()) {
      Map<String, String> fields = new LinkedHashMap<>();
      fields.put(COLUMN_NAME_KEY, entry.getColumnName());
      fields.put(DELETION_ID_KEY, entry.getDeletionId());
      fields.put(DELETION_EPOCH_MS_KEY, Long.toString(entry.getDeletionEpochMs()));
      fields.put(SCHEMA_ZK_VERSION_KEY, Integer.toString(entry.getSchemaZkVersion()));
      if (entry.getSchemaZkMtimeMs() >= 0) {
        fields.put(SCHEMA_ZK_MTIME_MS_KEY, Long.toString(entry.getSchemaZkMtimeMs()));
      }
      fields.put(STATE_KEY, entry.getState().name());
      if (entry.getLastError() != null) {
        fields.put(LAST_ERROR_KEY, entry.getLastError());
      }
      if (entry.getOutstandingSegmentCount() > 0) {
        fields.put(OUTSTANDING_SEGMENT_COUNT_KEY, Integer.toString(entry.getOutstandingSegmentCount()));
      }
      znRecord.setMapField(entry.getDeletionId(), fields);
    }
    return znRecord;
  }

  public static ColumnDeletionMetadata fromZNRecord(ZNRecord record) {
    Preconditions.checkNotNull(record, "record");
    String formatVersionString = record.getSimpleField(FORMAT_VERSION_KEY);
    if (StringUtils.isBlank(formatVersionString)) {
      throw new IllegalArgumentException(
          "Column deletion metadata for '" + record.getId() + "' is missing formatVersion");
    }
    int formatVersion;
    try {
      formatVersion = Integer.parseInt(formatVersionString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Column deletion metadata for '" + record.getId() + "' has invalid formatVersion: " + formatVersionString, e);
    }
    if (formatVersion > CURRENT_FORMAT_VERSION) {
      throw new ColumnDeletionUnsupportedFormatException(formatVersion, CURRENT_FORMAT_VERSION);
    }
    if (formatVersion < 1) {
      throw new IllegalArgumentException(
          "Column deletion metadata for '" + record.getId() + "' has invalid formatVersion: " + formatVersion);
    }

    Map<String, ColumnDeletionEntry> entries = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, String>> mapField : record.getMapFields().entrySet()) {
      ColumnDeletionEntry entry = parseEntry(record.getId(), mapField.getKey(), mapField.getValue());
      entries.put(entry.getDeletionId(), entry);
    }
    return new ColumnDeletionMetadata(record.getId(), formatVersion, entries);
  }

  /// Stored format version without refusing newer values. Used by writers that must not clobber
  /// a znode they cannot parse.
  static int peekFormatVersion(ZNRecord record) {
    Preconditions.checkNotNull(record, "record");
    String formatVersionString = record.getSimpleField(FORMAT_VERSION_KEY);
    if (StringUtils.isBlank(formatVersionString)) {
      throw new IllegalArgumentException(
          "Column deletion metadata for '" + record.getId() + "' is missing formatVersion");
    }
    try {
      return Integer.parseInt(formatVersionString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Column deletion metadata for '" + record.getId() + "' has invalid formatVersion: " + formatVersionString, e);
    }
  }

  private static void checkNoDuplicateActiveColumns(Iterable<ColumnDeletionEntry> entries, boolean ignoreCase) {
    Set<String> activeNames = new HashSet<>();
    for (ColumnDeletionEntry entry : entries) {
      if (!entry.blocksReAdd()) {
        continue;
      }
      String key = SchemaDiff.normalizeColumnName(entry.getColumnName(), ignoreCase);
      if (!activeNames.add(key)) {
        throw new IllegalArgumentException(
            "Active column deletion already exists for column '" + entry.getColumnName() + "'");
      }
    }
  }

  private static ColumnDeletionEntry parseEntry(String tableNameWithType, String deletionIdKey,
      Map<String, String> fields) {
    if (fields == null) {
      throw new IllegalArgumentException(
          "Column deletion entry '" + deletionIdKey + "' on '" + tableNameWithType + "' has no fields");
    }
    String storedDeletionId = fields.get(DELETION_ID_KEY);
    if (StringUtils.isNotBlank(storedDeletionId) && !storedDeletionId.equals(deletionIdKey)) {
      throw new IllegalArgumentException(
          "Column deletion entry key '" + deletionIdKey + "' does not match stored deletionId '" + storedDeletionId
              + "' on '" + tableNameWithType + "'");
    }
    String columnName = requiredField(fields, COLUMN_NAME_KEY, deletionIdKey, tableNameWithType);
    long deletionEpochMs = parseLongField(fields, DELETION_EPOCH_MS_KEY, deletionIdKey, tableNameWithType);
    int schemaZkVersion = parseIntField(fields, SCHEMA_ZK_VERSION_KEY, deletionIdKey, tableNameWithType);
    long schemaZkMtimeMs = -1L;
    if (fields.containsKey(SCHEMA_ZK_MTIME_MS_KEY)) {
      schemaZkMtimeMs = parseLongField(fields, SCHEMA_ZK_MTIME_MS_KEY, deletionIdKey, tableNameWithType);
    }
    String stateName = requiredField(fields, STATE_KEY, deletionIdKey, tableNameWithType);
    ColumnDeletionState state;
    try {
      state = ColumnDeletionState.valueOf(stateName);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Column deletion entry '" + deletionIdKey + "' on '" + tableNameWithType + "' has unknown state: "
              + stateName, e);
    }
    String lastError = fields.get(LAST_ERROR_KEY);
    int outstandingSegmentCount = 0;
    if (fields.containsKey(OUTSTANDING_SEGMENT_COUNT_KEY)) {
      outstandingSegmentCount =
          parseIntField(fields, OUTSTANDING_SEGMENT_COUNT_KEY, deletionIdKey, tableNameWithType);
    }
    return new ColumnDeletionEntry(columnName, deletionIdKey, deletionEpochMs, schemaZkVersion, schemaZkMtimeMs, state,
        lastError, outstandingSegmentCount);
  }

  private static String requiredField(Map<String, String> fields, String key, String deletionId,
      String tableNameWithType) {
    String value = fields.get(key);
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException(
          "Column deletion entry '" + deletionId + "' on '" + tableNameWithType + "' is missing " + key);
    }
    return value;
  }

  private static long parseLongField(Map<String, String> fields, String key, String deletionId,
      String tableNameWithType) {
    String value = requiredField(fields, key, deletionId, tableNameWithType);
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Column deletion entry '" + deletionId + "' on '" + tableNameWithType + "' has invalid " + key + ": " + value,
          e);
    }
  }

  private static int parseIntField(Map<String, String> fields, String key, String deletionId,
      String tableNameWithType) {
    String value = requiredField(fields, key, deletionId, tableNameWithType);
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Column deletion entry '" + deletionId + "' on '" + tableNameWithType + "' has invalid " + key + ": " + value,
          e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnDeletionMetadata that = (ColumnDeletionMetadata) o;
    return _formatVersion == that._formatVersion && _tableNameWithType.equals(that._tableNameWithType)
        && _entries.equals(that._entries);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_tableNameWithType, _formatVersion, _entries);
  }
}
