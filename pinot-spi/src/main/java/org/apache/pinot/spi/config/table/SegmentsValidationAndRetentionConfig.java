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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.utils.TimeUtils;


// TODO: Consider break this config into multiple configs
public class SegmentsValidationAndRetentionConfig extends BaseJsonConfig {
  private String _retentionTimeUnit;
  private String _retentionTimeValue;
  private String _deletedSegmentsRetentionPeriod;
  @Deprecated
  private String _segmentPushFrequency; // DO NOT REMOVE, this is used in internal segment generation management
  @Deprecated
  private String _segmentPushType;
  private String _replication;
  @Deprecated // Use _replication instead
  private String _replicasPerPartition;
  @Deprecated // Schema name should be the same as raw table name
  private String _schemaName;
  private String _timeColumnName;
  private TimeUnit _timeType;
  @Deprecated  // Use SegmentAssignmentConfig instead
  private String _segmentAssignmentStrategy;
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

  /**
   * @deprecated Use {@link InstanceAssignmentConfig} instead
   */
  @Deprecated
  public String getSegmentAssignmentStrategy() {
    return _segmentAssignmentStrategy;
  }

  @Deprecated
  public void setSegmentAssignmentStrategy(String segmentAssignmentStrategy) {
    _segmentAssignmentStrategy = segmentAssignmentStrategy;
  }

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

  public String getDeletedSegmentsRetentionPeriod() {
    return _deletedSegmentsRetentionPeriod;
  }

  public void setDeletedSegmentsRetentionPeriod(String deletedSegmentsRetentionPeriod) {
    _deletedSegmentsRetentionPeriod = deletedSegmentsRetentionPeriod;
  }

  /**
   * @deprecated Use {@code segmentIngestionFrequency} from {@link IngestionConfig#getBatchIngestionConfig()}
   */
  @Deprecated
  public String getSegmentPushFrequency() {
    return _segmentPushFrequency;
  }

  @Deprecated
  public void setSegmentPushFrequency(String segmentPushFrequency) {
    _segmentPushFrequency = segmentPushFrequency;
  }

  /**
   * @deprecated Use {@code segmentIngestionType} from {@link IngestionConfig#getBatchIngestionConfig()}
   */
  @Deprecated
  public String getSegmentPushType() {
    return _segmentPushType;
  }

  @Deprecated
  public void setSegmentPushType(String segmentPushType) {
    _segmentPushType = segmentPushType;
  }

  /**
   * Try to Use {@link TableConfig#getReplication()}
   */
  public String getReplication() {
    return _replication;
  }

  public void setReplication(String replication) {
    _replication = replication;
  }

  /**
   * Try to Use {@link TableConfig#getReplication()}
   * @deprecated Use _replication instead
   *
   * Will be deleted in future version of Pinot
   */
  @Deprecated
  public String getReplicasPerPartition() {
    return _replicasPerPartition;
  }

  /**
   * Try to Use {@link SegmentsValidationAndRetentionConfig#setReplication(String)}
   *
   * Will be deleted in future version of Pinot
   */
  @Deprecated
  public void setReplicasPerPartition(String replicasPerPartition) {
    _replicasPerPartition = replicasPerPartition;
  }

  /**
   * @deprecated Schema name should be the same as raw table name
   */
  @Deprecated
  public String getSchemaName() {
    return _schemaName;
  }

  @Deprecated
  public void setSchemaName(String schemaName) {
    _schemaName = schemaName;
  }

  /**
   * @deprecated Use {@link InstanceAssignmentConfig} instead.
   */
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

  /**
   * Try to Use {@link TableConfig#getReplication()}
   */
  @Deprecated
  @JsonIgnore
  public int getReplicationNumber() {
    return Integer.parseInt(_replication);
  }

  /**
   * Try to Use {@link TableConfig#getReplication()}
   *
   * Will be deleted in future version of Pinot
   */
  @Deprecated
  @JsonIgnore
  public int getReplicasPerPartitionNumber() {
    return Integer.parseInt(_replicasPerPartition);
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

  /**
   * @deprecated Use {@link InstanceAssignmentConfig} instead
   */
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
}
