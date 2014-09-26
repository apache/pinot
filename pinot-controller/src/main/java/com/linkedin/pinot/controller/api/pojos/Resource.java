package com.linkedin.pinot.controller.api.pojos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 26, 2014
 */

public class Resource {

  private final String resourceName;
  private final String tableName;
  private final String timeColumnName;
  private final String timeType;
  private final int numInstances;
  private final int numReplicas;
  private final String retentionTimeUnit;
  private final String retentionTimeValue;
  private final String pushFrequency;

  @JsonCreator
  public Resource(@JsonProperty("resourceName") String resourceName, @JsonProperty("tableName") String tableName,
      @JsonProperty("timeColumnName") String timeColumnName, @JsonProperty("timeType") String timeType,
      @JsonProperty("numInstances") int numInstances, @JsonProperty("numReplicas") int numReplicas,
      @JsonProperty("retentionTimeUnit") String retentionTimeUnit, @JsonProperty("retentionTimeValue") String retentionTimeValue,
      @JsonProperty("pushFrequency") String pushFrequency) {
    this.resourceName = resourceName;
    this.tableName = tableName;
    this.timeColumnName = timeColumnName;
    this.timeType = timeType;
    this.numInstances = numInstances;
    this.numReplicas = numReplicas;
    this.retentionTimeUnit = retentionTimeUnit;
    this.retentionTimeValue = retentionTimeValue;
    this.pushFrequency = pushFrequency;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public String getTimeType() {
    return timeType;
  }

  public int getNumInstances() {
    return numInstances;
  }

  public int getNumReplicas() {
    return numReplicas;
  }

  public String getRetentionTimeUnit() {
    return retentionTimeUnit;
  }

  public String getRetentionTimeValue() {
    return retentionTimeValue;
  }

  public String getPushFrequency() {
    return pushFrequency;
  }

  @Override
  public String toString() {
    final StringBuilder bld = new StringBuilder();
    bld.append("resourceName : " + resourceName + "\n");
    bld.append("tableName : " + tableName + "\n");
    bld.append("timeColumnName : " + timeColumnName + "\n");
    bld.append("timeType : " + timeType + "\n");
    bld.append("numInstances : " + numInstances + "\n");
    bld.append("numReplicas : " + numReplicas + "\n");
    bld.append("retentionTimeUnit : " + retentionTimeUnit + "\n");
    bld.append("retentionTimeValue : " + retentionTimeValue + "\n");
    bld.append("pushFrequency : " + pushFrequency + "\n");
    return bld.toString();
  }

  public static void main(String[] args) {
    final ObjectMapper mapper = new ObjectMapper();

  }
}
