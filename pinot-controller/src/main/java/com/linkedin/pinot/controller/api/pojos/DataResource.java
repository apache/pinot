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
  private final int numInstancesPerReplica;
  private final int numReplicas;
  private final String retentionTimeUnit;
  private final String retentionTimeValue;
  private final String pushFrequency;
  private final String segmentAssignmentStrategy;

  @JsonCreator
  public DataResource(@JsonProperty("resourceName") String resourceName, @JsonProperty("tableName") String tableName,
      @JsonProperty("timeColumnName") String timeColumnName, @JsonProperty("timeType") String timeType,
      @JsonProperty("numInstances") int numInstances, @JsonProperty("numReplicas") int numReplicas,
      @JsonProperty("retentionTimeUnit") String retentionTimeUnit, @JsonProperty("retentionTimeValue") String retentionTimeValue,
      @JsonProperty("pushFrequency") String pushFrequency, @JsonProperty("segmentAssignmentStrategy") String segmentAssignmentStrategy) {
    this.resourceName = resourceName;
    this.tableName = tableName;
    this.timeColumnName = timeColumnName;
    this.timeType = timeType;
    numInstancesPerReplica = numInstances;
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

  public int getNumInstancesPerReplica() {
    return numInstancesPerReplica;
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
    return new DataResource(props.get("resourceName"), props.get("tableName"), props.get("timeColumnName"), props.get("timeType"),
        Integer.parseInt(props.get("numInstances")), Integer.parseInt(props.get("numReplicas")), props.get("retentionTimeUnit"),
        props.get("retentionTimeValue"), props.get("pushFrequency"), props.get("segmentAssignmentStrategy"));
  }

  /**
   *  returns true if and only if resource name matches and numInstancesPerReplica and numReplicas are the same
   */
  public boolean instancEequals(Object incoming) {
    if (!(incoming instanceof DataResource)) {
      return false;
    }

    if (((DataResource) incoming).getResourceName().equals(resourceName)
        && ((DataResource) incoming).getNumInstancesPerReplica() == numInstancesPerReplica
        && ((DataResource) incoming).getNumReplicas() == numReplicas) {
      return true;
    }

    return false;
  }

  /**
   *  returns true if all properties are the same
   */
  @Override
  public boolean equals(Object incoming) {
    if (!(incoming instanceof DataResource)) {
      return false;
    }

    final DataResource incomingDS = (DataResource) incoming;

    if (incomingDS.getResourceName().equals(resourceName) && incomingDS.getNumInstancesPerReplica() == numInstancesPerReplica
        && incomingDS.getNumReplicas() == numReplicas && incomingDS.getPushFrequency().equals(pushFrequency)
        && incomingDS.getRetentionTimeUnit().equals(retentionTimeUnit) && incomingDS.getRetentionTimeValue().equals(retentionTimeValue)
        && incomingDS.getSegmentAssignmentStrategy().equals(segmentAssignmentStrategy) && incomingDS.getTableName().equals(tableName)
        && incomingDS.getTimeColumnName().equals(timeColumnName) && incomingDS.getTimeType().equals(timeType)) {
      return true;
    }

    return false;
  }

  /**
   *  this compare to does the following
   *  returns 0 if numInstancesPerReplica and numReplicas are the same for this and incoming
   *  -1 contract replica set (numInstancesPerReplica > incoming)
   *  1 expand replica set (numInstancesPerReplica < incoming)
   *
   *  -2 reduce number of replicas (numReplicas > incoming)
   *  2 increase number of replicas (numReplicas < incoming)
   */
  public int compareInstancesPerReplica(DataResource incoming) {
    if (numInstancesPerReplica == incoming.getNumInstancesPerReplica()) {
      return 0;
    }
    if (numInstancesPerReplica > incoming.getNumInstancesPerReplica()) {
      return -1;
    }
    return 1;
  }

  /**
   *  this compare to does the following
   *  returns 0 if numInstancesPerReplica and numReplicas are the same for this and incoming
   *  -1 reduce number of replicas (numReplicas > incoming)
   *  1 increase number of replicas (numReplicas < incoming)
   */
  public int compareNumReplicas(DataResource incoming) {
    if (numReplicas == incoming.getNumReplicas()) {
      return 0;
    }
    if (numReplicas > incoming.getNumReplicas()) {
      return -1;
    }
    return 1;
  }

  public Map<String, String> toMap() {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("resourceName", resourceName);
    props.put("tableName", tableName);
    props.put("timeColumnName", timeColumnName);
    props.put("timeType", timeType);
    props.put("numInstances", String.valueOf(numInstancesPerReplica));
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
    bld.append("numInstances : " + numInstancesPerReplica + "\n");
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
    ret.put("numInstances", String.valueOf(numInstancesPerReplica));
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
