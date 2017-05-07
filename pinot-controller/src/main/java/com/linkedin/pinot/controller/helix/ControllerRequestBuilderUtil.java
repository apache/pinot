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
package com.linkedin.pinot.controller.helix;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.config.Tenant.TenantBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.TenantRole;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static com.linkedin.pinot.common.utils.CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE;
import static com.linkedin.pinot.common.utils.CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE;


public class ControllerRequestBuilderUtil {

  public static JSONObject buildInstanceCreateRequestJSON(String host, String port, String tag)
      throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put("host", host);
    ret.put("port", port);
    ret.put("tag", tag);
    return ret;
  }

  public static JSONArray buildBulkInstanceCreateRequestJSON(int start, int end)
      throws JSONException {
    final JSONArray ret = new JSONArray();
    for (int i = start; i <= end; i++) {
      final JSONObject ins = new JSONObject();
      ins.put("host", "localhost");
      ins.put("port", i);
      ins.put("tag", UNTAGGED_SERVER_INSTANCE);
      ret.put(ins);
    }

    return ret;
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances)
      throws Exception {
    addFakeBrokerInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, false);
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant)
      throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String brokerId = "Broker_localhost_" + i;
      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServer);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new EmptyBrokerOnlineOfflineStateModelFactory();
      stateMachineEngine
          .registerStateModelFactory(EmptyBrokerOnlineOfflineStateModelFactory.getStateModelDef(), stateModelFactory);
      helixZkManager.connect();
      if (isSingleTenant) {
        helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId,
            ControllerTenantNameBuilder.getBrokerTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
      } else {
        helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId, UNTAGGED_BROKER_INSTANCE);
      }
      Thread.sleep(1000);
    }
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances)
      throws Exception {
    addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, false, 8097);
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant)
      throws Exception {
    addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, zkServer, numInstances, isSingleTenant, 8097);
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances, boolean isSingleTenant, int adminPort)
      throws Exception {

    for (int i = 0; i < numInstances; ++i) {
      final String instanceId = "Server_localhost_" + i;

      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new EmptySegmentOnlineOfflineStateModelFactory();
      stateMachineEngine
          .registerStateModelFactory(EmptySegmentOnlineOfflineStateModelFactory.getStateModelDef(), stateModelFactory);
      helixZkManager.connect();
      if (isSingleTenant) {
        helixZkManager.getClusterManagmentTool()
            .addInstanceTag(helixClusterName, instanceId,
                TableNameBuilder.OFFLINE.tableNameWithType(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
        helixZkManager.getClusterManagmentTool()
            .addInstanceTag(helixClusterName, instanceId,
                TableNameBuilder.REALTIME.tableNameWithType(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
      } else {
        helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId, UNTAGGED_SERVER_INSTANCE);
      }
      HelixConfigScope scope =
          new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, helixClusterName)
              .forParticipant(instanceId).build();
      Map<String, String> props = new HashMap<>();
      props.put(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, String.valueOf(adminPort + i));
      helixZkManager.getClusterManagmentTool().setConfig(scope, props);
    }
  }

  public static JSONObject buildBrokerTenantCreateRequestJSON(String tenantName, int numberOfInstances)
      throws JSONException {
    Tenant tenant = new TenantBuilder(tenantName).setRole(TenantRole.BROKER).setTotalInstances(numberOfInstances)
        .setOfflineInstances(0).setRealtimeInstances(0).build();
    return tenant.toJSON();
  }

  public static JSONObject buildServerTenantCreateRequestJSON(String tenantName, int numberOfInstances,
      int offlineInstances, int realtimeInstances)
      throws JSONException {
    Tenant tenant = new TenantBuilder(tenantName).setRole(TenantRole.SERVER).setTotalInstances(numberOfInstances)
        .setOfflineInstances(offlineInstances).setRealtimeInstances(realtimeInstances).build();
    return tenant.toJSON();
  }

  public static JSONObject buildCreateOfflineTableJSON(String tableName, String serverTenant, String brokerTenant,
      int numReplicas)
      throws JSONException {
    return buildCreateOfflineTableJSON(tableName, serverTenant, brokerTenant, "timeColumnName", "timeType", "DAYS",
        "700", numReplicas, "BalanceNumSegmentAssignmentStrategy");
  }

  public static JSONObject buildCreateOfflineTableJSON(String tableName, String serverTenant, String brokerTenant,
      String timeColumnName, String timeType, String retentionTimeUnit, String retentionTimeValue, int numReplicas,
      String segmentAssignmentStrategy)
      throws JSONException {
    return buildCreateOfflineTableJSON(tableName, serverTenant, brokerTenant, timeColumnName, timeType,
        retentionTimeUnit, retentionTimeValue, numReplicas, segmentAssignmentStrategy,
        Lists.newArrayList("coulmn1", "column2"));
  }

  public static JSONObject buildCreateOfflineTableJSON(String tableName, String serverTenant, String brokerTenant,
      String timeColumnName, String timeType, String retentionTimeUnit, String retentionTimeValue, int numReplicas,
      String segmentAssignmentStrategy, List<String> invertedIndexColumnList)
      throws JSONException {
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
    for (String columnToIndex : invertedIndexColumnList) {
      invertedIndexColumns.put(columnToIndex);
    }
    tableIndexConfig.put("invertedIndexColumns", invertedIndexColumns);
    tableIndexConfig.put("loadMode", "HEAP");
    tableIndexConfig.put("lazyLoad", "false");
    creationRequest.put("tableIndexConfig", tableIndexConfig);
    JSONObject tenants = new JSONObject();
    if (brokerTenant != null) {
      tenants.put("broker", brokerTenant);
    }
    if (serverTenant != null) {
      tenants.put("server", serverTenant);
    }
    creationRequest.put("tenants", tenants);
    creationRequest.put("tableType", "OFFLINE");
    JSONObject metadata = new JSONObject();
    creationRequest.put("metadata", metadata);
    return creationRequest;
  }

  public static JSONObject buildCreateOfflineTableJSON(String tableName, String serverTenant, String brokerTenant,
      int numReplicas, String segmentAssignmentStrategy)
      throws JSONException {
    return buildCreateOfflineTableJSON(tableName, serverTenant, brokerTenant, "timeColumnName", "daysSinceEpoch",
        "DAYS", "700", numReplicas, segmentAssignmentStrategy);
  }
}
