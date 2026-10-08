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
  /// The limit sums compressed archive bytes for active, live completed segments, counted once regardless of
  /// replication. Live-segment lineage filtering excludes sources of COMPLETED entries and destinations of other
  /// entries to avoid counting shadow copies. OFFLINE APPEND and REALTIME limits are enforced independently, including
  /// hybrid tables.
  ///
  /// Segments are ordered ascending by the first nonnegative end time, creation time, or push time, in that priority,
  /// then by segment name. Eviction removes only an old prefix before the first active segment listed as a source or
  /// destination in any retained lineage entry. Every listed segment is protected until its lineage entry is removed,
  /// regardless of entry state, age, or the lineage-exclusive deletion setting. Listed shadow copies remain stop
  /// positions even when their bytes are excluded. Missing live metadata or unknown live completed-segment sizes cause
  /// the pass to skip safely. Missing metadata or timestamps for an active lineage stop prevent eviction when over cap.
  ///
  /// With the default lineage manager, COMPLETED sources wait for [#getReplacedSegmentsRetentionPeriod()] (4h for
  /// APPEND, 24h for REFRESH) before deletion. The entry is removed on a subsequent cleanup pass after sources leave
  /// IdealState. The retention schedule (6h by default) also delays release of the boundary. Stale IN_PROGRESS entries
  /// use [#getLineageEntryCleanupRetentionPeriod()] (1d by default); REVERTED entries are eligible immediately.
  /// These are cleanup eligibility windows, not deadlines for meeting the size cap. Continuous merge or rollup
  /// activity can keep a lineage boundary present and leave an over-cap table at `sizeRetentionBlocked=1` for hours
  /// or longer.
  ///
  /// The newest OFFLINE segment, highest-sequence DONE LLC segment per REALTIME partition group, and undated segments
  /// are preserved. Consuming segments are neither counted nor removed. When hybrid retention is enabled and an
  /// OFFLINE counterpart exists, REALTIME eviction requires an end time strictly below the OFFLINE time boundary;
  /// an unavailable boundary prevents REALTIME size retention. These protections can leave the table above its cap.
  ///
  /// Size retention rechecks the complete lineage entry snapshot under the local updater lock before deletion,
  /// regardless of the lineage-exclusive deletion setting. A changed snapshot aborts the batch for retry next cycle.
  /// This deliberately includes timestamp-only changes and entries wholly newer than the boundary, even when the
  /// selected prefix is unaffected.
  /// The check and deletion are not a global transaction across controllers.
  ///
  /// This is an asynchronous, best-effort retention policy, not a limit on decompressed server disk usage,
  /// memory usage, or ingestion. When time retention is also configured, a segment can be deleted by either criterion.
  /// The table-scoped `sizeRetentionBlocked` controller gauge is 1 after an unsafe, failed, or still-over-cap pass,
  /// 0 after a successful pass, and absent when the policy is disabled or unsupported.
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

  /// Returns the retention period for source segments replaced by a COMPLETED lineage entry. Applies to all ingestion
  /// types, including APPEND and REFRESH.
  ///
  /// When a lineage entry transitions to COMPLETED state, source segments are preserved for this duration before
  /// being scheduled for deletion. The default lineage manager uses 4 hours for APPEND and other ingestion types,
  /// and 24 hours for REFRESH, when the value is unset, empty, or unparseable.
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

  /// Returns the retention period before stale IN_PROGRESS lineage entries and their destination segments become
  /// eligible for cleanup. The default lineage manager uses 1 day when the value is unset, empty, or unparseable.
  /// REVERTED entries are eligible immediately. This period does not apply to COMPLETED entries: those are removed
  /// on a cleanup pass after their source segments leave IdealState.
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
