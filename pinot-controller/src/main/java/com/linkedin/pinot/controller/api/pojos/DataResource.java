package com.linkedin.pinot.controller.api.pojos;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 26, 2014
 */

public class DataResource {

  private final String resourceName;
  private final String tableName;
  private final String timeColumnName;
  private final String timeType;
  private final int numInstances;
  private final int numReplicas;
  private final String retentionTimeUnit;
  private final String retentionTimeValue;
  private final String pushFrequency;
  private final String segmentAssignmentStrategy;

  @JsonCreator
  public DataResource(@JsonProperty("resourceName") String resourceName, @JsonProperty("tableName") String tableName,
      @JsonProperty("timeColumnName") String timeColumnName, @JsonProperty("timeType") String timeType,
      @JsonProperty("numInstances") int numInstances, @JsonProperty("numReplicas") int numReplicas,
      @JsonProperty("retentionTimeUnit") String retentionTimeUnit,
      @JsonProperty("retentionTimeValue") String retentionTimeValue,
      @JsonProperty("pushFrequency") String pushFrequency,
      @JsonProperty("segmentAssignmentStrategy") String segmentAssignmentStrategy) {
    this.resourceName = resourceName;
    this.tableName = tableName;
    this.timeColumnName = timeColumnName;
    this.timeType = timeType;
    this.numInstances = numInstances;
    this.numReplicas = numReplicas;
    this.retentionTimeUnit = retentionTimeUnit;
    this.retentionTimeValue = retentionTimeValue;
    this.pushFrequency = pushFrequency;
    this.segmentAssignmentStrategy = segmentAssignmentStrategy;
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

  public String getSegmentAssignmentStrategy() {
    return segmentAssignmentStrategy;
  }

  public static DataResource fromMap(Map<String, String> props) {
    return new DataResource(props.get("resourceName"), props.get("tableName"), props.get("timeColumnName"),
        props.get("timeType"), Integer.parseInt(props.get("numInstances")), Integer.parseInt(props.get("numReplicas")),
        props.get("retentionTimeUnit"), props.get("retentionTimeValue"), props.get("pushFrequency"),
        props.get("segmentAssignmentStrategy"));
  }

  public Map<String, String> toMap() {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("resourceName", resourceName);
    props.put("tableName", tableName);
    props.put("timeColumnName", timeColumnName);
    props.put("timeType", timeType);
    props.put("numInstances", String.valueOf(numInstances));
    props.put("numReplicas", String.valueOf(numReplicas));
    props.put("retentionTimeUnit", retentionTimeUnit);
    props.put("retentionTimeValue", retentionTimeValue);
    props.put("pushFrequency", pushFrequency);
    props.put("segmentAssignmentStrategy", segmentAssignmentStrategy);
    return props;
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
    bld.append("segmentAssignmentStrategy : " + segmentAssignmentStrategy + "\n");
    return bld.toString();
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put("resourceName", resourceName);
    ret.put("tableName", tableName);
    ret.put("timeColumnName", timeColumnName);
    ret.put("timeType", timeType);
    ret.put("numInstances", String.valueOf(numInstances));
    ret.put("numReplicas", String.valueOf(numReplicas));
    ret.put("retentionTimeUnit", retentionTimeUnit);
    ret.put("retentionTimeValue", retentionTimeValue);
    ret.put("pushFrequency", pushFrequency);
    ret.put("segmentAssignmentStrategy", segmentAssignmentStrategy);
    return ret;
  }

  public static void main(String[] args) {
    final ObjectMapper mapper = new ObjectMapper();

  }
}
