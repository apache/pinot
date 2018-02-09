/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.startree.hll.HllConfig;
import java.lang.reflect.Field;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentsValidationAndRetentionConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentsValidationAndRetentionConfig.class);

  private String retentionTimeUnit;
  private String retentionTimeValue;
  private String segmentPushFrequency; // DO NOT REMOVE, this is used in internal segment generation management
  private String segmentPushType;
  private String replication; // For high-level kafka consumers, the number of replicas should be same as num server instances
  private String schemaName;
  private String timeColumnName;
  private String timeType;

  private String segmentAssignmentStrategy;
  private ReplicaGroupStrategyConfig replicaGroupStrategyConfig;
  private StarTreeIndexSpec starTreeConfig;
  private HllConfig hllConfig;

  // Number of replicas per partition of low-level kafka consumers. This config is used for realtime tables only.
  private String replicasPerPartition;

  public String getSegmentAssignmentStrategy() {
    return segmentAssignmentStrategy;
  }

  public void setSegmentAssignmentStrategy(String segmentAssignmentStrategy) {
    this.segmentAssignmentStrategy = segmentAssignmentStrategy;
  }

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public void setTimeColumnName(String timeColumnName) {
    this.timeColumnName = timeColumnName;
  }

  public String getTimeType() {
    return timeType;
  }

  public void setTimeType(String timeType) {
    this.timeType = timeType;
  }

  public String getRetentionTimeUnit() {
    return retentionTimeUnit;
  }

  public void setRetentionTimeUnit(String retentionTimeUnit) {
    this.retentionTimeUnit = retentionTimeUnit;
  }

  public String getRetentionTimeValue() {
    return retentionTimeValue;
  }

  public void setRetentionTimeValue(String retentionTimeValue) {
    this.retentionTimeValue = retentionTimeValue;
  }

  public String getSegmentPushFrequency() {
    return segmentPushFrequency;
  }

  public void setSegmentPushFrequency(String segmentPushFrequency) {
    this.segmentPushFrequency = segmentPushFrequency;
  }

  public String getSegmentPushType() {
    return segmentPushType;
  }

  public void setSegmentPushType(String segmentPushType) {
    this.segmentPushType = segmentPushType;
  }

  public String getReplication() {
    return replication;
  }

  public void setReplication(String replication) {
    this.replication = replication;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getReplicasPerPartition() {
    return replicasPerPartition;
  }

  public void setReplicasPerPartition(String replicasPerPartition) {
    this.replicasPerPartition = replicasPerPartition;
  }

  public ReplicaGroupStrategyConfig getReplicaGroupStrategyConfig() {
    return replicaGroupStrategyConfig;
  }

  public void setReplicaGroupStrategyConfig(ReplicaGroupStrategyConfig replicaGroupStrategyConfig) {
    this.replicaGroupStrategyConfig = replicaGroupStrategyConfig;
  }

  public StarTreeIndexSpec getStarTreeConfig() {
    return starTreeConfig;
  }

  public void setStarTreeConfig(StarTreeIndexSpec starTreeConfig) {
    this.starTreeConfig = starTreeConfig;
  }

  public HllConfig getHllConfig() {
    return hllConfig;
  }

  public void setHllConfig(HllConfig hllConfig) {
    this.hllConfig = hllConfig;
  }

  @JsonIgnore
  public int getReplicationNumber() {
    return Integer.parseInt(replication);
  }

  @JsonIgnore
  public int getReplicasPerPartitionNumber() {
    return Integer.parseInt(replicasPerPartition);
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    final String newLine = System.getProperty("line.separator");

    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newLine);

    //determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    //print field names paired with their values
    for (final Field field : fields) {
      result.append("  ");
      try {
        result.append(field.getName());
        result.append(": ");
        //requires access to private field:
        result.append(field.get(this));
      } catch (final IllegalAccessException ex) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Caught exception while processing field " + field, ex);
        }
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    SegmentsValidationAndRetentionConfig that = (SegmentsValidationAndRetentionConfig) o;

    return EqualityUtils.isEqual(retentionTimeUnit, that.retentionTimeUnit) &&
        EqualityUtils.isEqual(retentionTimeValue, that.retentionTimeValue) &&
        EqualityUtils.isEqual(segmentPushFrequency, that.segmentPushFrequency) &&
        EqualityUtils.isEqual(segmentPushType, that.segmentPushType) &&
        EqualityUtils.isEqual(replication, that.replication) &&
        EqualityUtils.isEqual(schemaName, that.schemaName) &&
        EqualityUtils.isEqual(timeColumnName, that.timeColumnName) &&
        EqualityUtils.isEqual(timeType, that.timeType) &&
        EqualityUtils.isEqual( segmentAssignmentStrategy, that.segmentAssignmentStrategy) &&
        EqualityUtils.isEqual(replicaGroupStrategyConfig, that.replicaGroupStrategyConfig) &&
        EqualityUtils.isEqual(starTreeConfig, that.starTreeConfig) &&
        EqualityUtils.isEqual(hllConfig, that.hllConfig) &&
        EqualityUtils.isEqual(replicasPerPartition, that.replicasPerPartition);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(retentionTimeUnit);
    result = EqualityUtils.hashCodeOf(result, retentionTimeValue);
    result = EqualityUtils.hashCodeOf(result, segmentPushFrequency);
    result = EqualityUtils.hashCodeOf(result, segmentPushType);
    result = EqualityUtils.hashCodeOf(result, replication);
    result = EqualityUtils.hashCodeOf(result, schemaName);
    result = EqualityUtils.hashCodeOf(result, timeColumnName);
    result = EqualityUtils.hashCodeOf(result, timeType);
    result = EqualityUtils.hashCodeOf(result, segmentAssignmentStrategy);
    result = EqualityUtils.hashCodeOf(result, replicaGroupStrategyConfig);
    result = EqualityUtils.hashCodeOf(result, starTreeConfig);
    result = EqualityUtils.hashCodeOf(result, hllConfig);
    result = EqualityUtils.hashCodeOf(result, replicasPerPartition);
    return result;
  }
}
