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
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.utils.TimeUtils;


// TODO: Consider break this config into multiple configs
public class SegmentsValidationAndRetentionConfig extends BaseJsonConfig {
  private String _retentionTimeUnit;
  private String _retentionTimeValue;
  @Deprecated
  private String _segmentPushFrequency; // DO NOT REMOVE, this is used in internal segment generation management
  @Deprecated
  private String _segmentPushType;
  private String _replication;
  // For high-level consumers, the number of replicas should be same as num server instances
  private String _schemaName;
  private String _timeColumnName;
  private TimeUnit _timeType;
  private String _segmentAssignmentStrategy;
  private ReplicaGroupStrategyConfig _replicaGroupStrategyConfig;
  private CompletionConfig _completionConfig;
  private String _crypterClassName;
  // Possible values can be http or https. If this field is set, a Pinot server can download segments from peer servers
  // using the specified download scheme. Both realtime tables and offline tables can set this field.
  // For more usage of this field, please refer to this design doc:
  // https://cwiki.apache.org/confluence/display/PINOT/By-passing+deep-store+requirement+for+Realtime+segment+completion#By-passingdeep-storerequirementforRealtimesegmentcompletion-EnablebesteffortsegmentuploadinSplitSegmentCommiteranddownloadsegmentfrompeerservers.
  private String _peerSegmentDownloadScheme;

  // Number of replicas per partition of low-level consumers. This config is used for realtime tables only.
  private String _replicasPerPartition;

  public String getSegmentAssignmentStrategy() {
    return _segmentAssignmentStrategy;
  }

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

  /**
   * @deprecated Use {@code segmentIngestionFrequency} from {@link IngestionConfig#getBatchIngestionConfig()}
   */
  public String getSegmentPushFrequency() {
    return _segmentPushFrequency;
  }

  public void setSegmentPushFrequency(String segmentPushFrequency) {
    _segmentPushFrequency = segmentPushFrequency;
  }

  /**
   * @deprecated Use {@code segmentIngestionType} from {@link IngestionConfig#getBatchIngestionConfig()}
   */
  public String getSegmentPushType() {
    return _segmentPushType;
  }

  public void setSegmentPushType(String segmentPushType) {
    _segmentPushType = segmentPushType;
  }

  public String getReplication() {
    return _replication;
  }

  public void setReplication(String replication) {
    _replication = replication;
  }

  // Schema name should be the same as raw table name
  @Deprecated
  public String getSchemaName() {
    return _schemaName;
  }

  public void setSchemaName(String schemaName) {
    _schemaName = schemaName;
  }

  public String getReplicasPerPartition() {
    return _replicasPerPartition;
  }

  public void setReplicasPerPartition(String replicasPerPartition) {
    _replicasPerPartition = replicasPerPartition;
  }

  public ReplicaGroupStrategyConfig getReplicaGroupStrategyConfig() {
    return _replicaGroupStrategyConfig;
  }

  public void setReplicaGroupStrategyConfig(ReplicaGroupStrategyConfig replicaGroupStrategyConfig) {
    _replicaGroupStrategyConfig = replicaGroupStrategyConfig;
  }

  public CompletionConfig getCompletionConfig() {
    return _completionConfig;
  }

  public void setCompletionConfig(CompletionConfig completionConfig) {
    _completionConfig = completionConfig;
  }

  @JsonIgnore
  public int getReplicationNumber() {
    return Integer.parseInt(_replication);
  }

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
}
