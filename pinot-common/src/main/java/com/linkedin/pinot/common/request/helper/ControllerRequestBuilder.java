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
package com.linkedin.pinot.common.request.helper;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.utils.TenantRole;


public class ControllerRequestBuilder {

  private final static String TENANT_ROLE = "role";
  private final static String TENANT_NAME = "name";
  private final static String NUMBER_OF_INSTANCES = "numberOfInstances";
  private final static String OFFLINE_INSTANCES = "offlineInstances";
  private final static String REALTIME_INSTANCES = "realtimeInstances";

  public static JSONObject buildBrokerTenantCreateRequestJSON(String tenantName, int numberOfInstances)
      throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put(TENANT_ROLE, TenantRole.BROKER.toString());
    ret.put(TENANT_NAME, tenantName);
    ret.put(NUMBER_OF_INSTANCES, numberOfInstances);
    return ret;
  }

  public static JSONObject buildServerTenantCreateRequestJSON(String tenantName, int numberOfInstances,
      int offlineInstances, int realtimeInstances) throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put(TENANT_ROLE, TenantRole.SERVER);
    ret.put(TENANT_NAME, tenantName);
    ret.put(NUMBER_OF_INSTANCES, numberOfInstances);
    ret.put(OFFLINE_INSTANCES, offlineInstances);
    ret.put(REALTIME_INSTANCES, realtimeInstances);
    return ret;
  }

  public static JSONObject addOfflineTableRequest(String tableName, String serverTenant, String brokerTenant,
      int numReplicas) throws JSONException {
    return buildCreateOfflineTableV2JSON(tableName, serverTenant, brokerTenant, "timeColumnName", "timeType", "DAYS",
        "700", numReplicas, "BalanceNumSegmentAssignmentStrategy");
  }

  public static JSONObject buildCreateOfflineTableV2JSON(String tableName, String serverTenant, String brokerTenant,
      String timeColumnName, String timeType, String retentionTimeUnit, String retentionTimeValue, int numReplicas,
      String segmentAssignmentStrategy) throws JSONException {
    JSONObject creationRequest = new JSONObject();
    creationRequest.put("tableName", tableName);

    JSONObject segmentsConfig = new JSONObject();
    segmentsConfig.put("retentionTimeUnit", retentionTimeUnit);
    segmentsConfig.put("retentionTimeValue", retentionTimeValue);
    segmentsConfig.put("segmentPushFrequency", "daily");
    segmentsConfig.put("segmentPushType", "APPEND");
    segmentsConfig.put("replication", numReplicas);
    segmentsConfig.put("schemaName", "tableSchema");
    segmentsConfig.put("timeColumnName", timeColumnName);
    segmentsConfig.put("timeType", timeType);
    segmentsConfig.put("segmentAssignmentStrategy", segmentAssignmentStrategy);
    creationRequest.put("segmentsConfig", segmentsConfig);
    JSONObject tableIndexConfig = new JSONObject();
    JSONArray invertedIndexColumns = new JSONArray();
    invertedIndexColumns.put("column1");
    invertedIndexColumns.put("column2");
    tableIndexConfig.put("invertedIndexColumns", invertedIndexColumns);
    tableIndexConfig.put("loadMode", "HEAP");
    tableIndexConfig.put("lazyLoad", "false");
    creationRequest.put("tableIndexConfig", tableIndexConfig);
    JSONObject tenants = new JSONObject();
    tenants.put("broker", brokerTenant);
    tenants.put("server", serverTenant);
    creationRequest.put("tenants", tenants);
    creationRequest.put("tableType", "OFFLINE");
    JSONObject metadata = new JSONObject();
    JSONObject customConfigs = new JSONObject();
    customConfigs.put("d2Name", "xlntBetaPinot");
    metadata.put("customConfigs", customConfigs);
    creationRequest.put("metadata", metadata);
    return creationRequest;
  }

  public static JSONObject buildCreateRealtimeTableV2JSON(String tableName, String serverTenant, String brokerTenant,
      String timeColumnName, String timeType, String retentionTimeUnit, String retentionTimeValue, int numReplicas,
      String segmentAssignmentStrategy, JSONObject streamConfigs, String schemaName) throws JSONException {
    JSONObject creationRequest = new JSONObject();
    creationRequest.put("tableName", tableName);

    JSONObject segmentsConfig = new JSONObject();
    segmentsConfig.put("retentionTimeUnit", retentionTimeUnit);
    segmentsConfig.put("retentionTimeValue", retentionTimeValue);
    segmentsConfig.put("segmentPushFrequency", "daily");
    segmentsConfig.put("segmentPushType", "APPEND");
    segmentsConfig.put("replication", numReplicas);
    segmentsConfig.put("schemaName", schemaName);
    segmentsConfig.put("timeColumnName", timeColumnName);
    segmentsConfig.put("timeType", timeType);
    segmentsConfig.put("segmentAssignmentStrategy", segmentAssignmentStrategy);
    creationRequest.put("segmentsConfig", segmentsConfig);
    JSONObject tableIndexConfig = new JSONObject();
    JSONArray invertedIndexColumns = new JSONArray();
    invertedIndexColumns.put("column1");
    invertedIndexColumns.put("column2");
    tableIndexConfig.put("invertedIndexColumns", invertedIndexColumns);
    tableIndexConfig.put("loadMode", "HEAP");
    tableIndexConfig.put("lazyLoad", "false");
    tableIndexConfig.put("streamConfigs", streamConfigs);
    creationRequest.put("tableIndexConfig", tableIndexConfig);
    JSONObject tenants = new JSONObject();
    tenants.put("broker", brokerTenant);
    tenants.put("server", serverTenant);
    creationRequest.put("tenants", tenants);
    creationRequest.put("tableType", "REALTIME");
    JSONObject metadata = new JSONObject();
    JSONObject customConfigs = new JSONObject();
    customConfigs.put("d2Name", "xlntBetaPinot");
    metadata.put("customConfigs", customConfigs);
    creationRequest.put("metadata", metadata);
    return creationRequest;
  }

  public static JSONObject buildCreateOfflineTableV2JSON(String tableName, String serverTenant, String brokerTenant,
      int numReplicas, String segmentAssignmentStrategy) throws JSONException {
    return buildCreateOfflineTableV2JSON(tableName, serverTenant, brokerTenant, "timeColumnName", "timeType", "DAYS",
        "700", numReplicas, segmentAssignmentStrategy);
  }

}
