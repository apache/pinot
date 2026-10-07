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

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.utils.TimeUtils;


// TODO: Consider break this config into multiple configs
public class SegmentsValidationAndRetentionConfig extends BaseJsonConfig {
  private String _retentionTimeUnit;
  private String _retentionTimeValue;
  private String _retentionSize;
  private String _deletedSegmentsRetentionPeriod;
  private String _replacedSegmentsRetentionPeriod;
  private String _lineageEntryCleanupRetentionPeriod;
  @Deprecated
  private String _segmentPushFrequency; // DO NOT REMOVE, this is used in internal segment generation management
  @Deprecated
  private String _segmentPushType;
  private String _replication;
  @Deprecated // Use _replication instead
  private String _replicasPerPartition;
  private String _timeColumnName;
  private TimeUnit _timeType;
  @Deprecated  // Use SegmentAssignmentConfig instead
  private ReplicaGroupStrategyConfig _replicaGroupStrategyConfig;
  private CompletionConfig _completionConfig;
  private String _crypterClassName;
  @Deprecated
  private boolean _minimizeDataMovement;
  // Possible values can be http or https. If this field is set, a Pinot server can download segments from peer servers
  // using the specified download scheme. Both realtime tables and offline tables can set this field.
  // For more usage of this field, please refer to this design doc: https://tinyurl.com/f63ru4sb
  private String _peerSegmentDownloadScheme;

  private String _untrackedSegmentsDeletionBatchSize;
  private String _untrackedSegmentsRetentionTimeUnit;
  private String _untrackedSegmentsRetentionTimeValue;

  public String getTimeColumnName() {
    return _timeColumnName;
  }

  public void setTimeColumnName(String timeColumnName) {
    _timeColumnName = timeColumnName;
  }

  // TODO: Get field spec of _timeColumnName from Schema for the timeType
  @Deprecated
  public TimeUnit getTimeType() {
    return _timeType;
  }

  public void setTimeType(String timeType) {
    _timeType = TimeUtils.timeUnitFromString(timeType);
  }

  public String getRetentionTimeUnit() {
    return _retentionTimeUnit;
  }

  public void setRetentionTimeUnit(String retentionTimeUnit) {
    _retentionTimeUnit = retentionTimeUnit;
  }

  public String getRetentionTimeValue() {
    return _retentionTimeValue;
  }

  public void setRetentionTimeValue(String retentionTimeValue) {
    _retentionTimeValue = retentionTimeValue;
  }

  /// Returns the size retention limit, expressed as a positive size understood by
  /// [DataSizeUtils#toBytes(String)], for example `"100G"`.
  ///
  /// The limit applies to the sum of compressed archive bytes for active, live segments, counted once regardless of
  /// replication. Segments replaced by COMPLETED lineage entries are excluded. An IN_PROGRESS lineage entry, missing
  /// segment metadata, or an unknown completed segment size causes the entire size retention pass to be skipped.
  /// OFFLINE and REALTIME tables enforce their limits independently, including the two types of a hybrid table.
  ///
  /// Size retention removes the oldest eligible segments first, preserving the newest OFFLINE segment and the
  /// highest-sequence DONE LLC segment in each REALTIME partition group. Consuming segments are never removed by this
  /// policy. With lineage-exclusive deletion enabled, lineage-locked segments are also protected. The IN_PROGRESS
  /// lineage skip applies regardless of that setting. When hybrid retention is enabled and an OFFLINE counterpart
  /// exists, REALTIME size retention only removes segments whose end time is strictly below the OFFLINE time boundary;
  /// an unavailable boundary prevents REALTIME size retention. These protections can leave the table above the
  /// configured limit.
  ///
  /// With lineage-exclusive deletion disabled, a concurrent replacement that starts after the initial lineage
  /// snapshot can race with size eviction because the legacy deletion path does not recheck lineage before deletion.
  ///
  /// This is an asynchronous, best-effort retention policy, not a limit on decompressed server disk usage,
  /// memory usage, or ingestion. When time retention is also configured, a segment can be deleted by either criterion.
  /// A null value disables size retention.
  public String getRetentionSize() {
    return _retentionSize;
  }

  /// Sets the compressed segment archive size retention limit described by [#getRetentionSize()].
  /// Set to null to disable size retention.
  public void setRetentionSize(String retentionSize) {
    _retentionSize = retentionSize;
  }

  public String getDeletedSegmentsRetentionPeriod() {
    return _deletedSegmentsRetentionPeriod;
  }

  public void setDeletedSegmentsRetentionPeriod(String deletedSegmentsRetentionPeriod) {
    _deletedSegmentsRetentionPeriod = deletedSegmentsRetentionPeriod;
  }

  /// Returns the retention period for segments replaced by a REFRESH ingestion job. Only applies to tables with
  /// REFRESH ingestion type; for APPEND tables this setting is ignored and replaced segments are deleted immediately.
  ///
  /// When a lineage entry transitions to COMPLETED state, source segments are preserved for this duration before
  /// being scheduled for deletion, providing a rollback window. Consumers of this config (e.g. the lineage manager)
  /// treat a null or unparseable value as a 1 day default.
  ///
  /// Accepts a human-readable period string (e.g. `"7d"`, `"12h"`) as understood by
  /// `TimeUtils.convertPeriodToMillis`. Setting this value too low (e.g. `"0d"`) eliminates the rollback
  /// window; source segments will be deleted on the next retention pass after the lineage is COMPLETED.
  public String getReplacedSegmentsRetentionPeriod() {
    return _replacedSegmentsRetentionPeriod;
  }

  public void setReplacedSegmentsRetentionPeriod(String replacedSegmentsRetentionPeriod) {
    _replacedSegmentsRetentionPeriod = replacedSegmentsRetentionPeriod;
  }

  /// Returns the retention period before stale IN_PROGRESS or REVERTED lineage entries and their destination segments
  /// are cleaned up. Consumers of this config (e.g. the lineage manager) treat a null or unparseable value as a
  /// 1 day default.
  ///
  /// Accepts a human-readable period string (e.g. `"7d"`, `"12h"`) as understood by
  /// `TimeUtils.convertPeriodToMillis`.
  public String getLineageEntryCleanupRetentionPeriod() {
    return _lineageEntryCleanupRetentionPeriod;
  }

  public void setLineageEntryCleanupRetentionPeriod(String lineageEntryCleanupRetentionPeriod) {
    _lineageEntryCleanupRetentionPeriod = lineageEntryCleanupRetentionPeriod;
  }

  /// @deprecated Use `segmentIngestionFrequency` from
  ///     [org.apache.pinot.spi.config.table.ingestion.IngestionConfig#getBatchIngestionConfig()]
  @Deprecated
  public String getSegmentPushFrequency() {
    return _segmentPushFrequency;
  }

  @Deprecated
  public void setSegmentPushFrequency(String segmentPushFrequency) {
    _segmentPushFrequency = segmentPushFrequency;
  }

  /// @deprecated Use `segmentIngestionType` from
  ///     [org.apache.pinot.spi.config.table.ingestion.IngestionConfig#getBatchIngestionConfig()]
  @Deprecated
  public String getSegmentPushType() {
    return _segmentPushType;
  }

  @Deprecated
  public void setSegmentPushType(String segmentPushType) {
    _segmentPushType = segmentPushType;
  }

  /// Try to Use [TableConfig#getReplication()]
  public String getReplication() {
    return _replication;
  }

  public void setReplication(String replication) {
    _replication = replication;
  }

  /// Try to Use [TableConfig#getReplication()]
  /// @deprecated Use \_replication instead
  ///
  /// Will be deleted in future version of Pinot
  @Deprecated
  public String getReplicasPerPartition() {
    return _replicasPerPartition;
  }

  /// Try to Use [SegmentsValidationAndRetentionConfig#setReplication(String)]
  ///
  /// Will be deleted in future version of Pinot
  @Deprecated
  public void setReplicasPerPartition(String replicasPerPartition) {
    _replicasPerPartition = replicasPerPartition;
  }

  /// @deprecated Use [org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig] instead.
  @Deprecated
  public ReplicaGroupStrategyConfig getReplicaGroupStrategyConfig() {
    return _replicaGroupStrategyConfig;
  }

  @Deprecated
  public void setReplicaGroupStrategyConfig(ReplicaGroupStrategyConfig replicaGroupStrategyConfig) {
    _replicaGroupStrategyConfig = replicaGroupStrategyConfig;
  }

  public CompletionConfig getCompletionConfig() {
    return _completionConfig;
  }

  public void setCompletionConfig(CompletionConfig completionConfig) {
    _completionConfig = completionConfig;
  }

  public String getPeerSegmentDownloadScheme() {
    return _peerSegmentDownloadScheme;
  }

  public void setPeerSegmentDownloadScheme(String peerSegmentDownloadScheme) {
    _peerSegmentDownloadScheme = peerSegmentDownloadScheme;
  }

  public String getCrypterClassName() {
    return _crypterClassName;
  }

  public void setCrypterClassName(String crypterClassName) {
    _crypterClassName = crypterClassName;
  }

  /// @deprecated Use [org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig] instead
  @Deprecated
  public boolean isMinimizeDataMovement() {
    return _minimizeDataMovement;
  }

  @Deprecated
  public void setMinimizeDataMovement(boolean minimizeDataMovement) {
    _minimizeDataMovement = minimizeDataMovement;
  }

  public String getUntrackedSegmentsDeletionBatchSize() {
    return _untrackedSegmentsDeletionBatchSize;
  }

  public void setUntrackedSegmentsDeletionBatchSize(String untrackedSegmentsDeletionBatchSize) {
    _untrackedSegmentsDeletionBatchSize = untrackedSegmentsDeletionBatchSize;
  }

  public String getUntrackedSegmentsRetentionTimeUnit() {
    return _untrackedSegmentsRetentionTimeUnit;
  }

  public void setUntrackedSegmentsRetentionTimeUnit(String untrackedSegmentsRetentionTimeUnit) {
    _untrackedSegmentsRetentionTimeUnit = untrackedSegmentsRetentionTimeUnit;
  }

  public String getUntrackedSegmentsRetentionTimeValue() {
    return _untrackedSegmentsRetentionTimeValue;
  }

  public void setUntrackedSegmentsRetentionTimeValue(String untrackedSegmentsRetentionTimeValue) {
    _untrackedSegmentsRetentionTimeValue = untrackedSegmentsRetentionTimeValue;
  }
}
