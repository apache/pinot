package com.linkedin.pinot.controller.api.pojos;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 26, 2014
 */

public class DataResource {

  private final String resourceName;
  private final String tableName;
  private final String timeColumnName;
  private final String timeType;
  private final int numberOfDataInstances;
  private final int numberOfCopies;
  private final String retentionTimeUnit;
  private final String retentionTimeValue;
  private final String pushFrequency;
  private final String segmentAssignmentStrategy;
  private final String brokerTagName;
  private final int numberOfBrokerInstances;

  // create data resources
  // broker tag name (can be new or already existing) and number of instances
  // if new then
  // create broker tag (check if empty instances exist)
  // create broker data resource
  // check if assignment is valid else create broker resource

  // failure scenario is revert everything (all or nothing)

  @JsonCreator
  public DataResource(@JsonProperty(V1Constants.Helix.DataSource.RESOURCE_NAME) String resourceName,
      @JsonProperty(V1Constants.Helix.DataSource.TABLE_NAME) String tableName,
      @JsonProperty(V1Constants.Helix.DataSource.TIME_COLUMN_NAME) String timeColumnName,
      @JsonProperty(V1Constants.Helix.DataSource.TIME_TYPE) String timeType,
      @JsonProperty(V1Constants.Helix.DataSource.NUMBER_OF_DATA_INSTANCES) int numberOfDataInstances,
      @JsonProperty(V1Constants.Helix.DataSource.NUMBER_OF_COPIES) int numberOfCopies,
      @JsonProperty(V1Constants.Helix.DataSource.RETENTION_TIME_UNIT) String retentionTimeUnit,
      @JsonProperty(V1Constants.Helix.DataSource.RETENTION_TIME_VALUE) String retentionTimeValue,
      @JsonProperty(V1Constants.Helix.DataSource.PUSH_FREQUENCY) String pushFrequency,
      @JsonProperty(V1Constants.Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY) String segmentAssignmentStrategy,
      @JsonProperty(V1Constants.Helix.DataSource.BROKER_TAG_NAME) String brokerTagName,
      @JsonProperty(V1Constants.Helix.DataSource.NUMBER_OF_BROKER_INSTANCES) int numberOfBrokerInstances) {

    this.resourceName = resourceName;
    this.tableName = tableName;
    this.timeColumnName = timeColumnName;
    this.timeType = timeType;
    this.numberOfDataInstances = numberOfDataInstances;
    this.numberOfCopies = numberOfCopies;
    this.retentionTimeUnit = retentionTimeUnit;
    this.retentionTimeValue = retentionTimeValue;
    this.pushFrequency = pushFrequency;
    this.segmentAssignmentStrategy = segmentAssignmentStrategy;
    this.brokerTagName = brokerTagName;
    this.numberOfBrokerInstances = numberOfBrokerInstances;
  }

  public int getNumberOfDataInstances() {
    return numberOfDataInstances;
  }

  public int getNumberOfCopies() {
    return numberOfCopies;
  }

  public String getBrokerTagName() {
    return V1Constants.Helix.PREFIX_OF_BROKER_RESOURCE_TAG + brokerTagName;
  }

  public int getNumberOfBrokerInstances() {
    return numberOfBrokerInstances;
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
    return new DataResource(props.get(V1Constants.Helix.DataSource.RESOURCE_NAME),
        props.get(V1Constants.Helix.DataSource.TABLE_NAME), props.get(V1Constants.Helix.DataSource.TIME_COLUMN_NAME),
        props.get(V1Constants.Helix.DataSource.TIME_TYPE), Integer.parseInt(props
            .get(V1Constants.Helix.DataSource.NUMBER_OF_DATA_INSTANCES)), Integer.parseInt(props
            .get(V1Constants.Helix.DataSource.NUMBER_OF_COPIES)),
        props.get(V1Constants.Helix.DataSource.RETENTION_TIME_UNIT),
        props.get(V1Constants.Helix.DataSource.RETENTION_TIME_VALUE),
        props.get(V1Constants.Helix.DataSource.PUSH_FREQUENCY),
        props.get(V1Constants.Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY),
        props.get(V1Constants.Helix.DataSource.BROKER_TAG_NAME), Integer.parseInt(props
            .get(V1Constants.Helix.DataSource.NUMBER_OF_BROKER_INSTANCES)));
  }

  /**
   *  returns true if and only if resource name matches and numInstancesPerReplica and numReplicas are the same
   */
  public boolean instancEequals(Object incoming) {
    if (!(incoming instanceof DataResource)) {
      return false;
    }

    if (((DataResource) incoming).getResourceName().equals(resourceName)
        && (((DataResource) incoming).getNumberOfDataInstances() == numberOfDataInstances)
        && (((DataResource) incoming).getNumberOfCopies() == numberOfCopies)) {
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

    if (incomingDS.getResourceName().equals(resourceName)
        && (incomingDS.getNumberOfDataInstances() == numberOfDataInstances)
        && (incomingDS.getNumberOfCopies() == numberOfCopies) && incomingDS.getPushFrequency().equals(pushFrequency)
        && incomingDS.getRetentionTimeUnit().equals(retentionTimeUnit)
        && incomingDS.getRetentionTimeValue().equals(retentionTimeValue)
        && incomingDS.getSegmentAssignmentStrategy().equals(segmentAssignmentStrategy)
        && incomingDS.getTableName().equals(tableName) && incomingDS.getTimeColumnName().equals(timeColumnName)
        && incomingDS.getTimeType().equals(timeType) && incomingDS.getBrokerTagName().equals(brokerTagName)
        && (incomingDS.getNumberOfCopies() == numberOfCopies)) {
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
    if (numberOfDataInstances == incoming.getNumberOfDataInstances()) {
      return 0;
    }
    if (numberOfDataInstances > incoming.getNumberOfDataInstances()) {
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
    if (numberOfCopies == incoming.getNumberOfCopies()) {
      return 0;
    }
    if (numberOfCopies > incoming.getNumberOfCopies()) {
      return -1;
    }
    return 1;
  }

  public Map<String, String> toMap() {
    final Map<String, String> ret = new HashMap<String, String>();
    ret.put(V1Constants.Helix.DataSource.RESOURCE_NAME, resourceName);
    ret.put(V1Constants.Helix.DataSource.TABLE_NAME, tableName);
    ret.put(V1Constants.Helix.DataSource.TIME_COLUMN_NAME, timeColumnName);
    ret.put(V1Constants.Helix.DataSource.TIME_TYPE, timeType);
    ret.put(V1Constants.Helix.DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numberOfDataInstances));
    ret.put(V1Constants.Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(numberOfCopies));
    ret.put(V1Constants.Helix.DataSource.RETENTION_TIME_UNIT, retentionTimeUnit);
    ret.put(V1Constants.Helix.DataSource.RETENTION_TIME_VALUE, retentionTimeValue);
    ret.put(V1Constants.Helix.DataSource.PUSH_FREQUENCY, pushFrequency);
    ret.put(V1Constants.Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, segmentAssignmentStrategy);
    ret.put(V1Constants.Helix.DataSource.BROKER_TAG_NAME, brokerTagName);
    ret.put(V1Constants.Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, String.valueOf(numberOfBrokerInstances));
    return ret;
  }

  @Override
  public String toString() {
    final StringBuilder bld = new StringBuilder();
    bld.append(V1Constants.Helix.DataSource.RESOURCE_NAME + " : " + resourceName + "\n");
    bld.append(V1Constants.Helix.DataSource.TABLE_NAME + " : " + tableName + "\n");
    bld.append(V1Constants.Helix.DataSource.TIME_COLUMN_NAME + " : " + timeColumnName + "\n");
    bld.append(V1Constants.Helix.DataSource.TIME_TYPE + " : " + timeType + "\n");
    bld.append(V1Constants.Helix.DataSource.NUMBER_OF_DATA_INSTANCES + " : " + numberOfDataInstances + "\n");
    bld.append(V1Constants.Helix.DataSource.NUMBER_OF_COPIES + " : " + numberOfCopies + "\n");
    bld.append(V1Constants.Helix.DataSource.RETENTION_TIME_UNIT + " : " + retentionTimeUnit + "\n");
    bld.append(V1Constants.Helix.DataSource.RETENTION_TIME_VALUE + " : " + retentionTimeValue + "\n");
    bld.append(V1Constants.Helix.DataSource.PUSH_FREQUENCY + " : " + pushFrequency + "\n");
    bld.append(V1Constants.Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY + " : " + segmentAssignmentStrategy + "\n");
    bld.append(V1Constants.Helix.DataSource.BROKER_TAG_NAME + " : " + brokerTagName + "\n");
    bld.append(V1Constants.Helix.DataSource.NUMBER_OF_BROKER_INSTANCES + " : " + numberOfBrokerInstances + "\n");
    return bld.toString();
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put(V1Constants.Helix.DataSource.RESOURCE_NAME, resourceName);
    ret.put(V1Constants.Helix.DataSource.TABLE_NAME, tableName);
    ret.put(V1Constants.Helix.DataSource.TIME_COLUMN_NAME, timeColumnName);
    ret.put(V1Constants.Helix.DataSource.TIME_TYPE, timeType);
    ret.put(V1Constants.Helix.DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numberOfDataInstances));
    ret.put(V1Constants.Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(numberOfCopies));
    ret.put(V1Constants.Helix.DataSource.RETENTION_TIME_UNIT, retentionTimeUnit);
    ret.put(V1Constants.Helix.DataSource.RETENTION_TIME_VALUE, retentionTimeValue);
    ret.put(V1Constants.Helix.DataSource.PUSH_FREQUENCY, pushFrequency);
    ret.put(V1Constants.Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, segmentAssignmentStrategy);
    ret.put(V1Constants.Helix.DataSource.BROKER_TAG_NAME, brokerTagName);
    ret.put(V1Constants.Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, String.valueOf(numberOfBrokerInstances));

    return ret;
  }

  public static void main(String[] args) {
    final ObjectMapper mapper = new ObjectMapper();

  }
}
