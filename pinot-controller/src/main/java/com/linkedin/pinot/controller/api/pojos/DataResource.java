/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.pojos;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Objects;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 26, 2014
 */

public class DataResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataResource.class);

  private final String requestType;
  private final String resourceName;
  private final String resourceType;
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
  private final BaseJsonNode metadata;

  // create data resources
  // broker tag name (can be new or already existing) and number of instances
  // if new then
  // create broker tag (check if empty instances exist)
  // create broker data resource
  // check if assignment is valid else create broker resource

  // failure scenario is revert everything (all or nothing)

  @JsonCreator
  public DataResource(@JsonProperty(Helix.DataSource.REQUEST_TYPE) String requestType,
      @JsonProperty(Helix.DataSource.RESOURCE_NAME) String resourceName,
      @JsonProperty(Helix.DataSource.RESOURCE_TYPE) String resourceType,
      @JsonProperty(Helix.DataSource.TABLE_NAME) String tableName,
      @JsonProperty(Helix.DataSource.TIME_COLUMN_NAME) String timeColumnName,
      @JsonProperty(Helix.DataSource.TIME_TYPE) String timeType,
      @JsonProperty(Helix.DataSource.NUMBER_OF_DATA_INSTANCES) int numberOfDataInstances,
      @JsonProperty(Helix.DataSource.NUMBER_OF_COPIES) int numberOfCopies,
      @JsonProperty(Helix.DataSource.RETENTION_TIME_UNIT) String retentionTimeUnit,
      @JsonProperty(Helix.DataSource.RETENTION_TIME_VALUE) String retentionTimeValue,
      @JsonProperty(Helix.DataSource.PUSH_FREQUENCY) String pushFrequency,
      @JsonProperty(Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY) String segmentAssignmentStrategy,
      @JsonProperty(Helix.DataSource.BROKER_TAG_NAME) String brokerTagName,
      @JsonProperty(CommonConstants.Helix.DataSource.NUMBER_OF_BROKER_INSTANCES) int numberOfBrokerInstances,
      @JsonProperty(CommonConstants.Helix.DataSource.METADATA) BaseJsonNode metadata) {

    this.requestType = requestType;
    this.resourceName = resourceName;
    this.resourceType = resourceType;
    this.tableName = tableName;
    this.timeColumnName = timeColumnName;
    this.timeType = timeType;
    this.numberOfDataInstances = numberOfDataInstances;
    this.numberOfCopies = numberOfCopies;
    this.retentionTimeUnit = retentionTimeUnit;
    this.retentionTimeValue = retentionTimeValue;
    this.pushFrequency = pushFrequency;
    this.segmentAssignmentStrategy = segmentAssignmentStrategy;
    if (brokerTagName != null) {
      if (brokerTagName.startsWith(Helix.PREFIX_OF_BROKER_RESOURCE_TAG)) {
        this.brokerTagName = brokerTagName;
      } else {
        this.brokerTagName = Helix.PREFIX_OF_BROKER_RESOURCE_TAG + brokerTagName;
      }
    } else {
      this.brokerTagName = null;
    }
    this.numberOfBrokerInstances = numberOfBrokerInstances;
    this.metadata = metadata;
  }

  public int getNumberOfDataInstances() {
    return numberOfDataInstances;
  }

  public int getNumberOfCopies() {
    return numberOfCopies;
  }

  public String getBrokerTagName() {
    return brokerTagName;
  }

  public int getNumberOfBrokerInstances() {
    return numberOfBrokerInstances;
  }

  public String getResourceName() {
    return resourceName;
  }

  public ResourceType getResourceType() {
    try {
      return ResourceType.valueOf(resourceType);
    } catch (Exception ex) {
      LOGGER.warn("Caught exception", ex);
      return null;
    }
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

  public String getRequestType() {
    return requestType;
  }

  public ObjectNode getMetadata() {
    if (metadata instanceof ObjectNode)
      return (ObjectNode) metadata;
    return null;
  }

  public boolean isCreatedDataResource() {
    return requestType.equalsIgnoreCase(Helix.DataSourceRequestType.CREATE);
  }

  public boolean isDataResourceUpdate() {
    return requestType.equalsIgnoreCase(Helix.DataSourceRequestType.UPDATE_DATA_RESOURCE);
  }

  public boolean isDataResourceConfigUpdate() {
    return requestType.equalsIgnoreCase(Helix.DataSourceRequestType.UPDATE_DATA_RESOURCE_CONFIG);
  }

  public boolean isDataTableAdd() {
    return requestType.equalsIgnoreCase(Helix.DataSourceRequestType.ADD_TABLE_TO_RESOURCE);
  }

  public boolean isDataTableRemove() {
    return requestType.equalsIgnoreCase(Helix.DataSourceRequestType.REMOVE_TABLE_FROM_RESOURCE);
  }

  public boolean isBrokerResourceUpdate() {
    return requestType.equalsIgnoreCase(Helix.DataSourceRequestType.UPDATE_BROKER_RESOURCE);
  }

  public static DataResource fromMap(Map<String, String> props) {
    if (Helix.DataSourceRequestType.CREATE.equalsIgnoreCase(props
        .get(Helix.DataSource.REQUEST_TYPE))) {
      return new DataResource(props.get(Helix.DataSource.REQUEST_TYPE),
          props.get(Helix.DataSource.RESOURCE_NAME),
          props.get(Helix.DataSource.RESOURCE_TYPE),
          props.get(Helix.DataSource.TABLE_NAME),
          props.get(Helix.DataSource.TIME_COLUMN_NAME),
          props.get(Helix.DataSource.TIME_TYPE), Integer.parseInt(props
              .get(Helix.DataSource.NUMBER_OF_DATA_INSTANCES)), Integer.parseInt(props
              .get(Helix.DataSource.NUMBER_OF_COPIES)),
          props.get(Helix.DataSource.RETENTION_TIME_UNIT),
          props.get(Helix.DataSource.RETENTION_TIME_VALUE),
          props.get(Helix.DataSource.PUSH_FREQUENCY),
          props.get(Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY),
          props.get(Helix.DataSource.BROKER_TAG_NAME), Integer.parseInt(props
              .get(Helix.DataSource.NUMBER_OF_BROKER_INSTANCES)), null);
    }
    if (Helix.DataSourceRequestType.UPDATE_DATA_RESOURCE.equalsIgnoreCase(props
        .get(Helix.DataSource.REQUEST_TYPE))) {
      return new DataResource(props.get(Helix.DataSource.REQUEST_TYPE),
          props.get(Helix.DataSource.RESOURCE_NAME), props.get(Helix.DataSource.RESOURCE_TYPE),
          props.get(Helix.DataSource.TABLE_NAME), null, null, Integer.parseInt(props
              .get(Helix.DataSource.NUMBER_OF_DATA_INSTANCES)), Integer.parseInt(props
              .get(Helix.DataSource.NUMBER_OF_COPIES)), null, null, null, null, null, -1, null);
    }
    if (Helix.DataSourceRequestType.UPDATE_BROKER_RESOURCE.equalsIgnoreCase(props
        .get(Helix.DataSource.REQUEST_TYPE))) {
      return new DataResource(props.get(Helix.DataSource.REQUEST_TYPE),
          props.get(Helix.DataSource.RESOURCE_NAME), props.get(Helix.DataSource.RESOURCE_TYPE),
          props.get(Helix.DataSource.TABLE_NAME), null, null, -1, -1, null, null, null, null,
          props.get(Helix.DataSource.BROKER_TAG_NAME), Integer.parseInt(props
              .get(Helix.DataSource.NUMBER_OF_BROKER_INSTANCES)), null);
    }
    if (CommonConstants.Helix.DataSourceRequestType.ADD_TABLE_TO_RESOURCE.equalsIgnoreCase(
        props.get(CommonConstants.Helix.DataSource.REQUEST_TYPE))) {
      return new DataResource(
          props.get(CommonConstants.Helix.DataSource.REQUEST_TYPE),
          props.get(CommonConstants.Helix.DataSource.RESOURCE_NAME), props.get(Helix.DataSource.RESOURCE_TYPE),
          props.get(CommonConstants.Helix.DataSource.TABLE_NAME),
          null, null, -1, -1, null, null, null, null, null, -1, null);
    }
    throw new UnsupportedOperationException("Don't support Request type: "
        + props.get(Helix.DataSource.REQUEST_TYPE));
  }

  /**
   *  returns true if and only if resource name, number of instances and number of data replicas are the same
   */
  public boolean instanceEquals(Object incoming) {
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
   *  returns true if and only if broker tag and number of brokers are the same
   */
  public boolean brokerEequals(Object incoming) {
    if (!(incoming instanceof DataResource)) {
      return false;
    }

    if (((DataResource) incoming).getResourceName().equals(resourceName)
        && (((DataResource) incoming).getNumberOfBrokerInstances() == numberOfBrokerInstances)
        && (((DataResource) incoming).getBrokerTagName().equals(brokerTagName))) {
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

    if ((incomingDS.getRequestType().equals(requestType)) && (incomingDS.getResourceName().equals(resourceName))
        && (incomingDS.getResourceType().equals(resourceType))
        && (incomingDS.getNumberOfDataInstances() == numberOfDataInstances)
        && (incomingDS.getNumberOfCopies() == numberOfCopies) && incomingDS.getPushFrequency().equals(pushFrequency)
        && incomingDS.getRetentionTimeUnit().equals(retentionTimeUnit)
        && incomingDS.getRetentionTimeValue().equals(retentionTimeValue)
        && incomingDS.getSegmentAssignmentStrategy().equals(segmentAssignmentStrategy)
        && incomingDS.getTableName().equals(tableName) && incomingDS.getTimeColumnName().equals(timeColumnName)
        && incomingDS.getTimeType().equals(timeType) && incomingDS.getBrokerTagName().equals(brokerTagName)) {
      return true;
    }
    return false;
  }

  public int hashCode() {
    return Objects.hashCode(
        requestType,
        resourceName,
        resourceType,
        numberOfDataInstances,
        numberOfCopies,
        pushFrequency,
        retentionTimeUnit,
        retentionTimeValue,
        segmentAssignmentStrategy,
        tableName,
        timeColumnName,
        timeType,
        brokerTagName);
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
    ret.put(Helix.DataSource.REQUEST_TYPE, requestType);
    ret.put(Helix.DataSource.RESOURCE_NAME, resourceName);
    ret.put(Helix.DataSource.RESOURCE_TYPE, resourceType);
    ret.put(Helix.DataSource.TABLE_NAME, tableName);
    ret.put(Helix.DataSource.TIME_COLUMN_NAME, timeColumnName);
    ret.put(Helix.DataSource.TIME_TYPE, timeType);
    ret.put(Helix.DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numberOfDataInstances));
    ret.put(Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(numberOfCopies));
    ret.put(Helix.DataSource.RETENTION_TIME_UNIT, retentionTimeUnit);
    ret.put(Helix.DataSource.RETENTION_TIME_VALUE, retentionTimeValue);
    ret.put(Helix.DataSource.PUSH_FREQUENCY, pushFrequency);
    ret.put(Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, segmentAssignmentStrategy);
    ret.put(Helix.DataSource.BROKER_TAG_NAME, brokerTagName);
    ret.put(Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, String.valueOf(numberOfBrokerInstances));
    setMetaToConfigMap(ret);
    return ret;
  }

  @Override
  public String toString() {
    final StringBuilder bld = new StringBuilder();
    bld.append(Helix.DataSource.REQUEST_TYPE + " : " + requestType + "\n");
    bld.append(Helix.DataSource.RESOURCE_NAME + " : " + resourceName + "\n");
    bld.append(Helix.DataSource.RESOURCE_TYPE + " : " + resourceType + "\n");
    bld.append(Helix.DataSource.TABLE_NAME + " : " + tableName + "\n");
    bld.append(Helix.DataSource.TIME_COLUMN_NAME + " : " + timeColumnName + "\n");
    bld.append(Helix.DataSource.TIME_TYPE + " : " + timeType + "\n");
    bld.append(Helix.DataSource.NUMBER_OF_DATA_INSTANCES + " : " + numberOfDataInstances + "\n");
    bld.append(Helix.DataSource.NUMBER_OF_COPIES + " : " + numberOfCopies + "\n");
    bld.append(Helix.DataSource.RETENTION_TIME_UNIT + " : " + retentionTimeUnit + "\n");
    bld.append(Helix.DataSource.RETENTION_TIME_VALUE + " : " + retentionTimeValue + "\n");
    bld.append(Helix.DataSource.PUSH_FREQUENCY + " : " + pushFrequency + "\n");
    bld.append(Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY + " : " + segmentAssignmentStrategy + "\n");
    bld.append(Helix.DataSource.BROKER_TAG_NAME + " : " + brokerTagName + "\n");
    bld.append(Helix.DataSource.NUMBER_OF_BROKER_INSTANCES + " : " + numberOfBrokerInstances + "\n");
    bld.append(Helix.DataSource.METADATA + " : " + metadata + "\n");
    return bld.toString();
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put(Helix.DataSource.REQUEST_TYPE, requestType);
    ret.put(Helix.DataSource.RESOURCE_NAME, resourceName);
    ret.put(Helix.DataSource.RESOURCE_TYPE, resourceType);
    ret.put(Helix.DataSource.TABLE_NAME, tableName);
    ret.put(Helix.DataSource.TIME_COLUMN_NAME, timeColumnName);
    ret.put(Helix.DataSource.TIME_TYPE, timeType);
    ret.put(Helix.DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numberOfDataInstances));
    ret.put(Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(numberOfCopies));
    ret.put(Helix.DataSource.RETENTION_TIME_UNIT, retentionTimeUnit);
    ret.put(Helix.DataSource.RETENTION_TIME_VALUE, retentionTimeValue);
    ret.put(Helix.DataSource.PUSH_FREQUENCY, pushFrequency);
    ret.put(Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, segmentAssignmentStrategy);
    ret.put(Helix.DataSource.BROKER_TAG_NAME, brokerTagName);
    ret.put(Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, String.valueOf(numberOfBrokerInstances));
    ret.put(Helix.DataSource.METADATA, metadata);

    return ret;
  }

  public static void main(String[] args) throws JSONException {
    final ObjectMapper mapper = new ObjectMapper();
    JSONObject metadata = new JSONObject();
    metadata.put("d2.name", "xlntBetaPinot");
    DataResource dataResource =
        new DataResource("requestType", "resourceName", "resourceType", "tableName", "timeColumnName", "timeType", 1, 2,
            "retentionTimeUnit", "rentionTimeValue", "pushFrequency", "segmentAssignmentStrategy", "brokerTagName", 3,
            null);
    System.out.println(dataResource);
    System.out.println(dataResource.toJSON());

  }

  public Map<String, String> toResourceConfigMap() {
    if (requestType.equalsIgnoreCase(Helix.DataSourceRequestType.CREATE)) {
      final Map<String, String> ret = new HashMap<String, String>();
      ret.put(Helix.DataSource.RESOURCE_NAME, resourceName);
      ret.put(Helix.DataSource.RESOURCE_TYPE, resourceType);
      ret.put(Helix.DataSource.TABLE_NAME, tableName);
      ret.put(Helix.DataSource.TIME_COLUMN_NAME, timeColumnName);
      ret.put(Helix.DataSource.TIME_TYPE, timeType);
      ret.put(Helix.DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numberOfDataInstances));
      ret.put(Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(numberOfCopies));
      ret.put(Helix.DataSource.RETENTION_TIME_UNIT, retentionTimeUnit);
      ret.put(Helix.DataSource.RETENTION_TIME_VALUE, retentionTimeValue);
      ret.put(Helix.DataSource.PUSH_FREQUENCY, pushFrequency);
      ret.put(Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, segmentAssignmentStrategy);
      ret.put(Helix.DataSource.BROKER_TAG_NAME, brokerTagName);
      ret.put(Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, String.valueOf(numberOfBrokerInstances));
      setMetaToConfigMap(ret);
      return ret;
    } else if (requestType.equalsIgnoreCase(Helix.DataSourceRequestType.UPDATE_DATA_RESOURCE)) {
      final Map<String, String> ret = new HashMap<String, String>();
      ret.put(Helix.DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numberOfDataInstances));
      ret.put(Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(numberOfCopies));
      return ret;
    } else if (requestType.equalsIgnoreCase(Helix.DataSourceRequestType.UPDATE_DATA_RESOURCE_CONFIG)) {
      final Map<String, String> ret = new HashMap<String, String>();
      if (timeColumnName != null) {
        ret.put(Helix.DataSource.TIME_COLUMN_NAME, timeColumnName);
      }
      if (timeType != null) {
        ret.put(Helix.DataSource.TIME_TYPE, timeType);
      }
      if (retentionTimeUnit != null) {
        ret.put(Helix.DataSource.RETENTION_TIME_UNIT, retentionTimeUnit);
      }
      if (retentionTimeValue != null) {
        ret.put(Helix.DataSource.RETENTION_TIME_VALUE, retentionTimeValue);
      }
      if (pushFrequency != null) {
        ret.put(Helix.DataSource.PUSH_FREQUENCY, pushFrequency);
      }
      if (segmentAssignmentStrategy != null) {
        ret.put(Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, segmentAssignmentStrategy);
      }
      setMetaToConfigMap(ret);
      return ret;
    } else if (requestType.equalsIgnoreCase(Helix.DataSourceRequestType.UPDATE_BROKER_RESOURCE)) {
      final Map<String, String> ret = new HashMap<String, String>();
      ret.put(Helix.DataSource.BROKER_TAG_NAME, brokerTagName);
      ret.put(Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, String.valueOf(numberOfBrokerInstances));
      return ret;
    }

    throw new RuntimeException("Not support request type: " + requestType);
  }

  private void setMetaToConfigMap(Map<String, String> ret) {
    if (metadata != null) {
      System.out.println(metadata);
      for (Iterator iterator = metadata.fieldNames(); iterator.hasNext();) {
        String key = (String) iterator.next();
        System.out.println("key : " + key + " : value : " + metadata.get(key));
        ret.put(Helix.DataSource.METADATA + "." + key, metadata.get(key).textValue());
      }
    }
  }

}
