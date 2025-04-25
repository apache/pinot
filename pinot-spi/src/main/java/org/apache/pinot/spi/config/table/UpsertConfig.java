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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.utils.Enablement;


/// Class representing upsert configuration of a table.
public class UpsertConfig extends BaseJsonConfig {

  public enum Mode {
    FULL, PARTIAL, NONE
  }

  public enum Strategy {
    // Todo: add CUSTOM strategies
    APPEND, IGNORE, INCREMENT, MAX, MIN, OVERWRITE, FORCE_OVERWRITE, UNION
  }

  public enum ConsistencyMode {
    NONE, SYNC, SNAPSHOT
  }

  @JsonPropertyDescription("Upsert mode.")
  private Mode _mode = Mode.FULL;

  @JsonPropertyDescription("Function to hash the primary key.")
  private HashFunction _hashFunction = HashFunction.NONE;

  /// Maintains the mapping of merge strategies per column.
  /// Each key in the map is a columnName, value is a partial upsert merging strategy.
  /// Supported strategies are {OVERWRITE|INCREMENT|APPEND|UNION|IGNORE}.
  @JsonPropertyDescription("Partial update strategies.")
  @Nullable
  private Map<String, Strategy> _partialUpsertStrategies;

  @JsonPropertyDescription("default upsert strategy for partial mode")
  private Strategy _defaultPartialUpsertStrategy = Strategy.OVERWRITE;

  @JsonPropertyDescription("Class name for custom row merger implementation")
  private String _partialUpsertMergerClass;

  /// When two records have the same primary key, the comparison column(s) si used to determine the latest record. If
  /// not configured, the time column is used.
  /// When multiple comparison columns are configured, the latest record is determined by the first non-null value for
  /// the comparison columns. A typical use case is for partial upsert, where each record only contains non-null value
  /// for one of the comparison columns, and that value is used to determine the latest record. If the record is the
  /// latest, the non-null comparison column value is partially upserted to the previous record.
  @JsonPropertyDescription("Columns for upsert comparison, default to time column")
  @Nullable
  private List<String> _comparisonColumns;

  @JsonPropertyDescription("Boolean column to indicate whether a records should be deleted")
  @Nullable
  private String _deleteRecordColumn;

  @JsonPropertyDescription("Whether to drop out-of-order record")
  private boolean _dropOutOfOrderRecord;

  @JsonPropertyDescription("Boolean column to indicate whether a records is out-of-order")
  @Nullable
  private String _outOfOrderRecordColumn;

  @JsonPropertyDescription("Whether to use snapshot for fast upsert metadata recovery. Available values are ENABLE, "
      + "DISABLE and DEFAULT (use instance level default behavior).")
  private Enablement _snapshot = Enablement.DEFAULT;

  @JsonPropertyDescription("Whether to preload segments for fast upsert metadata recovery. Available values are "
      + "ENABLE, DISABLE and DEFAULT (use instance level default behavior).")
  private Enablement _preload = Enablement.DEFAULT;

  @JsonPropertyDescription("Whether to use TTL for upsert metadata cleanup, it uses the same unit as comparison col")
  private double _metadataTTL;

  @JsonPropertyDescription("TTL for upsert metadata cleanup for deleted keys, it uses the same unit as comparison col")
  private double _deletedKeysTTL;

  @JsonPropertyDescription("If we are using deletionKeysTTL + compaction we need to enable this for data consistency")
  private boolean _enableDeletedKeysCompactionConsistency;

  @JsonPropertyDescription("Configure the way to provide consistent view for upsert table")
  private ConsistencyMode _consistencyMode = ConsistencyMode.NONE;

  @JsonPropertyDescription("Refresh interval when using the snapshot consistency mode")
  private long _upsertViewRefreshIntervalMs = 3000;

  // Setting this time to 0 to disable the tracking feature.
  @JsonPropertyDescription("Track newly added segments on the server for a more complete upsert data view.")
  private long _newSegmentTrackingTimeMs = 10000;

  @JsonPropertyDescription("Custom class for upsert metadata manager")
  private String _metadataManagerClass;

  @JsonPropertyDescription("Custom configs for upsert metadata manager")
  @Nullable
  private Map<String, String> _metadataManagerConfigs;

  /// @deprecated use {@link #_snapshot} instead. This is kept here for backward compatibility.
  @Deprecated
  @JsonPropertyDescription("Whether to use snapshot for fast upsert metadata recovery")
  private boolean _enableSnapshot;

  /// @deprecated use {@link #_preload} instead. This is kept here for backward compatibility.
  @Deprecated
  @JsonPropertyDescription("Whether to preload segments for fast upsert metadata recovery")
  private boolean _enablePreload;

  /// @deprecated use {@link org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy)} instead.
  @Deprecated
  @JsonPropertyDescription("Whether to pause partial upsert table's partition consumption during commit")
  private boolean _allowPartialUpsertConsumptionDuringCommit;

  public UpsertConfig(Mode mode) {
    _mode = mode;
  }

  // Do not use this constructor. This is needed for JSON deserialization.
  public UpsertConfig() {
  }

  public Mode getMode() {
    return _mode;
  }

  public void setMode(Mode mode) {
    Preconditions.checkArgument(mode != null, "Upsert mode cannot be null");
    _mode = mode;
  }

  public HashFunction getHashFunction() {
    return _hashFunction;
  }

  public void setHashFunction(HashFunction hashFunction) {
    Preconditions.checkArgument(hashFunction != null, "Hash function cannot be null");
    _hashFunction = hashFunction;
  }

  @Nullable
  public Map<String, Strategy> getPartialUpsertStrategies() {
    return _partialUpsertStrategies;
  }

  public void setPartialUpsertStrategies(@Nullable Map<String, Strategy> partialUpsertStrategies) {
    _partialUpsertStrategies = partialUpsertStrategies;
  }

  public Strategy getDefaultPartialUpsertStrategy() {
    return _defaultPartialUpsertStrategy;
  }

  public void setDefaultPartialUpsertStrategy(Strategy defaultPartialUpsertStrategy) {
    Preconditions.checkArgument(defaultPartialUpsertStrategy != null, "Default partial upsert strategy cannot be null");
    _defaultPartialUpsertStrategy = defaultPartialUpsertStrategy;
  }

  @Nullable
  public String getPartialUpsertMergerClass() {
    return _partialUpsertMergerClass;
  }

  public void setPartialUpsertMergerClass(@Nullable String partialUpsertMergerClass) {
    _partialUpsertMergerClass = partialUpsertMergerClass;
  }

  @Nullable
  public List<String> getComparisonColumns() {
    return _comparisonColumns;
  }

  public void setComparisonColumns(@Nullable List<String> comparisonColumns) {
    if (CollectionUtils.isNotEmpty(comparisonColumns)) {
      _comparisonColumns = comparisonColumns;
    } else {
      _comparisonColumns = null;
    }
  }

  public void setComparisonColumn(@Nullable String comparisonColumn) {
    if (StringUtils.isNotEmpty(comparisonColumn)) {
      _comparisonColumns = List.of(comparisonColumn);
    } else {
      _comparisonColumns = null;
    }
  }

  @Nullable
  public String getDeleteRecordColumn() {
    return _deleteRecordColumn;
  }

  public void setDeleteRecordColumn(@Nullable String deleteRecordColumn) {
    _deleteRecordColumn = deleteRecordColumn;
  }

  public boolean isDropOutOfOrderRecord() {
    return _dropOutOfOrderRecord;
  }

  public void setDropOutOfOrderRecord(@Nullable boolean dropOutOfOrderRecord) {
    _dropOutOfOrderRecord = dropOutOfOrderRecord;
  }

  @Nullable
  public String getOutOfOrderRecordColumn() {
    return _outOfOrderRecordColumn;
  }

  public void setOutOfOrderRecordColumn(@Nullable String outOfOrderRecordColumn) {
    _outOfOrderRecordColumn = outOfOrderRecordColumn;
  }

  public Enablement getSnapshot() {
    return _snapshot;
  }

  public void setSnapshot(Enablement snapshot) {
    Preconditions.checkArgument(snapshot != null, "Snapshot cannot be null, must be one of ENABLE, DISABLE or DEFAULT");
    _snapshot = snapshot;
  }

  public Enablement getPreload() {
    return _preload;
  }

  public void setPreload(Enablement preload) {
    Preconditions.checkArgument(preload != null, "Preload cannot be null, must be one of ENABLE, DISABLE or DEFAULT");
    _preload = preload;
  }

  public double getMetadataTTL() {
    return _metadataTTL;
  }

  public void setMetadataTTL(double metadataTTL) {
    _metadataTTL = metadataTTL;
  }

  public double getDeletedKeysTTL() {
    return _deletedKeysTTL;
  }

  public void setDeletedKeysTTL(double deletedKeysTTL) {
    _deletedKeysTTL = deletedKeysTTL;
  }

  public boolean isEnableDeletedKeysCompactionConsistency() {
    return _enableDeletedKeysCompactionConsistency;
  }

  public void setEnableDeletedKeysCompactionConsistency(boolean enableDeletedKeysCompactionConsistency) {
    _enableDeletedKeysCompactionConsistency = enableDeletedKeysCompactionConsistency;
  }

  public ConsistencyMode getConsistencyMode() {
    return _consistencyMode;
  }

  public void setConsistencyMode(ConsistencyMode consistencyMode) {
    Preconditions.checkArgument(consistencyMode != null, "Consistency mode cannot be null");
    _consistencyMode = consistencyMode;
  }

  public long getUpsertViewRefreshIntervalMs() {
    return _upsertViewRefreshIntervalMs;
  }

  public void setUpsertViewRefreshIntervalMs(long upsertViewRefreshIntervalMs) {
    _upsertViewRefreshIntervalMs = upsertViewRefreshIntervalMs;
  }

  public long getNewSegmentTrackingTimeMs() {
    return _newSegmentTrackingTimeMs;
  }

  public void setNewSegmentTrackingTimeMs(long newSegmentTrackingTimeMs) {
    _newSegmentTrackingTimeMs = newSegmentTrackingTimeMs;
  }

  @Nullable
  public String getMetadataManagerClass() {
    return _metadataManagerClass;
  }

  public void setMetadataManagerClass(@Nullable String metadataManagerClass) {
    _metadataManagerClass = metadataManagerClass;
  }

  @Nullable
  public Map<String, String> getMetadataManagerConfigs() {
    return _metadataManagerConfigs;
  }

  public void setMetadataManagerConfigs(@Nullable Map<String, String> metadataManagerConfigs) {
    _metadataManagerConfigs = metadataManagerConfigs;
  }

  @Deprecated
  public boolean isEnableSnapshot() {
    return _enableSnapshot;
  }

  @Deprecated
  public void setEnableSnapshot(boolean enableSnapshot) {
    _enableSnapshot = enableSnapshot;
    if (enableSnapshot) {
      _snapshot = Enablement.ENABLE;
    }
  }

  @Deprecated
  public boolean isEnablePreload() {
    return _enablePreload;
  }

  @Deprecated
  public void setEnablePreload(boolean enablePreload) {
    _enablePreload = enablePreload;
    if (enablePreload) {
      _preload = Enablement.ENABLE;
    }
  }

  @Deprecated
  public boolean isAllowPartialUpsertConsumptionDuringCommit() {
    return _allowPartialUpsertConsumptionDuringCommit;
  }

  @Deprecated
  public void setAllowPartialUpsertConsumptionDuringCommit(boolean allowPartialUpsertConsumptionDuringCommit) {
    _allowPartialUpsertConsumptionDuringCommit = allowPartialUpsertConsumptionDuringCommit;
  }
}
