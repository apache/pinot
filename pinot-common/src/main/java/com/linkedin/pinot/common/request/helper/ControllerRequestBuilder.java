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
package com.linkedin.pinot.common.request.helper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.config.Tenant.TenantBuilder;
import com.linkedin.pinot.common.utils.TenantRole;


public class ControllerRequestBuilder {

  public static JSONObject buildBrokerTenantCreateRequestJSON(String tenantName, int numberOfInstances)
      throws JSONException {
    Tenant tenant =
        new TenantBuilder(tenantName).setRole(TenantRole.BROKER).setTotalInstances(numberOfInstances).build();
    return tenant.toJSON();
  }

  public static JSONObject buildServerTenantCreateRequestJSON(String tenantName, int numberOfInstances,
      int offlineInstances, int realtimeInstances) throws JSONException {
    Tenant tenant = new TenantBuilder(tenantName).setRole(TenantRole.SERVER).setTotalInstances(numberOfInstances)
        .setOfflineInstances(offlineInstances).setRealtimeInstances(realtimeInstances).build();
    return tenant.toJSON();
  }

  public static JSONObject addOfflineTableRequest(String tableName, String serverTenant, String brokerTenant,
      int numReplicas) throws JSONException {
    return buildCreateOfflineTableJSON(tableName, serverTenant, brokerTenant, "timeColumnName", "timeType", "DAYS",
        "700", numReplicas, "BalanceNumSegmentAssignmentStrategy");
  }

  public static JSONObject buildCreateOfflineTableJSON(String tableName, String serverTenant, String brokerTenant,
      String timeColumnName, String timeType, String retentionTimeUnit, String retentionTimeValue, int numReplicas,
      String segmentAssignmentStrategy) throws JSONException {
    List<String> invertedIndexColumns = Collections.emptyList();
    return buildCreateOfflineTableJSON(tableName, serverTenant, brokerTenant, timeColumnName, timeType,
        retentionTimeUnit, retentionTimeValue, numReplicas, segmentAssignmentStrategy, invertedIndexColumns, null, "v1");
  }

  public static JSONObject buildCreateOfflineTableJSON(String tableName, String serverTenant, String brokerTenant,
      String timeColumnName, String timeType, String retentionTimeUnit, String retentionTimeValue, int numReplicas,
      String segmentAssignmentStrategy, List<String> invertedIndexColumns, String loadMode, String segmentVersion) throws JSONException {
    JSONObject creationRequest = new JSONObject();
    creationRequest.put("tableName", tableName);

    JSONObject segmentsConfig = new JSONObject();
    segmentsConfig.put("retentionTimeUnit", retentionTimeUnit);
    segmentsConfig.put("retentionTimeValue", retentionTimeValue);
    segmentsConfig.put("segmentPushFrequency", "daily");
    segmentsConfig.put("segmentPushType", "APPEND");
    segmentsConfig.put("replication", numReplicas);
    segmentsConfig.put("schemaName", "baseball");
    segmentsConfig.put("timeColumnName", timeColumnName);
    segmentsConfig.put("timeType", timeType);
    segmentsConfig.put("segmentAssignmentStrategy", segmentAssignmentStrategy);
    creationRequest.put("segmentsConfig", segmentsConfig);
    JSONObject tableIndexConfig = new JSONObject();
    tableIndexConfig.put("invertedIndexColumns", invertedIndexColumns);
    tableIndexConfig.put("segmentFormatVersion", segmentVersion);
    if (loadMode != null && loadMode.equals("MMAP")) {
      tableIndexConfig.put("loadMode", "MMAP");
    } else {
      tableIndexConfig.put("loadMode", "HEAP");
    }
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
    customConfigs.put("messageBasedRefresh", "true");
    metadata.put("customConfigs", customConfigs);
    creationRequest.put("metadata", metadata);
    return creationRequest;
  }

  public static JSONObject buildCreateRealtimeTableJSON(String tableName, String serverTenant, String brokerTenant,
      String timeColumnName, String timeType, String retentionTimeUnit, String retentionTimeValue, int numReplicas,
      String segmentAssignmentStrategy, JSONObject streamConfigs, String schemaName, String sortedColumn)
      throws JSONException {
    List<String> invertedIndexColumns = Collections.emptyList();
    return buildCreateRealtimeTableJSON(tableName, serverTenant, brokerTenant, timeColumnName, timeType, retentionTimeUnit,
        retentionTimeValue, numReplicas, segmentAssignmentStrategy, streamConfigs, schemaName, sortedColumn,
        invertedIndexColumns, null, true, null);
  }

  public static JSONObject buildCreateRealtimeTableJSON(String tableName, String serverTenant, String brokerTenant,
      String timeColumnName, String timeType, String retentionTimeUnit, String retentionTimeValue, int numReplicas,
      String segmentAssignmentStrategy, JSONObject streamConfigs, String schemaName, String sortedColumn,
      List<String> invertedIndexColumns, String loadMode, boolean isHighLevel)
          throws JSONException {
    return buildCreateRealtimeTableJSON(tableName, serverTenant, brokerTenant, timeColumnName, timeType, retentionTimeUnit,
        retentionTimeValue, numReplicas, segmentAssignmentStrategy, streamConfigs, schemaName, sortedColumn,
        invertedIndexColumns, loadMode, isHighLevel, null);
  }

  public static JSONObject buildCreateRealtimeTableJSON(String tableName, String serverTenant, String brokerTenant,
        String timeColumnName, String timeType, String retentionTimeUnit, String retentionTimeValue, int numReplicas,
    String segmentAssignmentStrategy, JSONObject streamConfigs, String schemaName, String sortedColumn,
        List<String> invertedIndexColumns, String loadMode, boolean isHighLevel, List<String> noDictionaryColumns)
      throws JSONException {
    return buildCreateRealtimeTableJSON(tableName, serverTenant, brokerTenant, timeColumnName, timeType,
        retentionTimeUnit, retentionTimeValue, numReplicas, segmentAssignmentStrategy, streamConfigs, schemaName,
        sortedColumn, invertedIndexColumns, loadMode, isHighLevel, null, null);
  }

  public static JSONObject buildCreateRealtimeTableJSON(String tableName, String serverTenant, String brokerTenant,
      String timeColumnName, String timeType, String retentionTimeUnit, String retentionTimeValue, int numReplicas,
      String segmentAssignmentStrategy, JSONObject streamConfigs, String schemaName, String sortedColumn,
      List<String> invertedIndexColumns, String loadMode, boolean isHighLevel, List<String> noDictionaryColumns,
      Map<String, String> partitioners)
      throws JSONException {
    JSONObject creationRequest = new JSONObject();
    creationRequest.put("tableName", tableName);

    JSONObject segmentsConfig = new JSONObject();
    segmentsConfig.put("retentionTimeUnit", retentionTimeUnit);
    segmentsConfig.put("retentionTimeValue", retentionTimeValue);
    segmentsConfig.put("segmentPushFrequency", "daily");
    segmentsConfig.put("segmentPushType", "APPEND");
    segmentsConfig.put("replication", numReplicas);
    if (!isHighLevel) {
      segmentsConfig.put("replicasPerPartition", numReplicas);
    }
    segmentsConfig.put("schemaName", schemaName);
    segmentsConfig.put("timeColumnName", timeColumnName);
    segmentsConfig.put("timeType", timeType);
    segmentsConfig.put("segmentAssignmentStrategy", segmentAssignmentStrategy);
    creationRequest.put("segmentsConfig", segmentsConfig);
    JSONObject tableIndexConfig = new JSONObject();
    tableIndexConfig.put("invertedIndexColumns", invertedIndexColumns);
    if (loadMode != null && loadMode.equals("MMAP")) {
      tableIndexConfig.put("loadMode", "MMAP");
    } else {
      tableIndexConfig.put("loadMode", "HEAP");
    }
    tableIndexConfig.put("lazyLoad", "false");
    tableIndexConfig.put("streamConfigs", streamConfigs);
    JSONArray sortedColumns = new JSONArray();
    if (sortedColumn != null) {
      sortedColumns.put(sortedColumn);
    }
    tableIndexConfig.put("sortedColumn", sortedColumns);
    if (noDictionaryColumns != null) {
      tableIndexConfig.put("noDictionaryColumns", noDictionaryColumns);
    }
    if (partitioners != null) {
      tableIndexConfig.put("partitioners", partitioners);
    }
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

  public static JSONObject buildCreateOfflineTableJSON(String tableName, String serverTenant, String brokerTenant,
      int numReplicas, String segmentAssignmentStrategy) throws JSONException {
    return buildCreateOfflineTableJSON(tableName, serverTenant, brokerTenant, "timeColumnName", "timeType", "DAYS",
        "700", numReplicas, segmentAssignmentStrategy);
  }

}
