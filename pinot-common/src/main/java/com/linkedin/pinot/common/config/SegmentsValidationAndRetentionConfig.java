package com.linkedin.pinot.common.config;

import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentsValidationAndRetentionConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentsValidationAndRetentionConfig.class);

  private String retentionTimeUnit;
  private String retentionTimeValue;
  private String segmentPushFrequency;
  private String segmentPushType;
  private String replication;
  private String schemaName;
  private String timeColumnName;
  private String timeType;
  private String segmentAssignmentStrategy;

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

  public SegmentsValidationAndRetentionConfig() {
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
}
