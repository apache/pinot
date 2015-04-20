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
package com.linkedin.pinot.controller.helix;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static com.linkedin.pinot.common.utils.CommonConstants.Helix.*;
import static com.linkedin.pinot.common.utils.CommonConstants.*;
import com.linkedin.pinot.controller.api.pojos.BrokerDataResource;
import com.linkedin.pinot.controller.api.pojos.BrokerTagResource;
import com.linkedin.pinot.controller.api.pojos.DataResource;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 29, 2014
 */

public class ControllerRequestBuilderUtil {

  public static JSONObject buildCreateOfflineResourceJSON(String resourceName, int numInstances, int numReplicas)
      throws JSONException {
    DataResource dataSource =
        createOfflineClusterCreationConfig(numInstances, numReplicas, resourceName,
            "BalanceNumSegmentAssignmentStrategy");

    return dataSource.toJSON();
  }

  public static JSONObject buildCreateRealtimeResourceJSON(String resourceName, int numInstances, int numReplicas)
      throws JSONException {
    DataResource dataSource =
        createRealtimeClusterCreationConfig(numInstances, numReplicas, resourceName,
            "BalanceNumSegmentAssignmentStrategy");

    return dataSource.toJSON();
  }

  public static JSONObject buildUpdateDataResourceJSON(String resourceName, int numInstances, int numReplicas)
      throws JSONException {
    DataResource dataSource = createOfflineClusterDataResourceUpdateConfig(numInstances, numReplicas, resourceName);

    return dataSource.toJSON();
  }

  public static JSONObject buildUpdateBrokerResourceJSON(String resourceName, int numInstances) throws JSONException {
    DataResource dataSource = createOfflineClusterBrokerResourceUpdateConfig(numInstances, resourceName);

    return dataSource.toJSON();
  }

  public static JSONObject buildInstanceCreateRequestJSON(String host, String port, String tag) throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put("host", host);
    ret.put("port", port);
    ret.put("tag", tag);
    return ret;
  }

  public static JSONArray buildBulkInstanceCreateRequestJSON(int start, int end) throws JSONException {
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

  public static DataResource createOfflineClusterCreationConfig(int numInstances, int numReplicas, String resourceName,
      String segmentAssignmentStrategy) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(DataSource.REQUEST_TYPE, DataSourceRequestType.CREATE);
    props.put(DataSource.RESOURCE_NAME, resourceName);
    props.put(DataSource.RESOURCE_TYPE, ResourceType.OFFLINE.toString());
    props.put(DataSource.TABLE_NAME, resourceName);
    props.put(DataSource.TIME_COLUMN_NAME, "days");
    props.put(DataSource.TIME_TYPE, "daysSinceEpoch");
    props.put(DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numInstances));
    props.put(DataSource.NUMBER_OF_COPIES, String.valueOf(numReplicas));
    props.put(DataSource.RETENTION_TIME_UNIT, "DAYS");
    props.put(DataSource.RETENTION_TIME_VALUE, "30");
    props.put(DataSource.PUSH_FREQUENCY, "daily");
    props.put(DataSource.SEGMENT_ASSIGNMENT_STRATEGY, segmentAssignmentStrategy);
    props.put(DataSource.BROKER_TAG_NAME, resourceName);
    props.put(DataSource.NUMBER_OF_BROKER_INSTANCES, "1");
    return DataResource.fromMap(props);
  }

  public static DataResource createRealtimeClusterCreationConfig(int numInstances, int numReplicas, String resourceName,
      String segmentAssignmentStrategy) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(DataSource.REQUEST_TYPE, DataSourceRequestType.CREATE);
    props.put(DataSource.RESOURCE_NAME, resourceName);
    props.put(DataSource.RESOURCE_TYPE, ResourceType.REALTIME.toString());
    props.put(DataSource.TABLE_NAME, resourceName);
    props.put(DataSource.TIME_COLUMN_NAME, "days");
    props.put(DataSource.TIME_TYPE, "daysSinceEpoch");
    props.put(DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numInstances));
    props.put(DataSource.NUMBER_OF_COPIES, String.valueOf(numReplicas));
    props.put(DataSource.RETENTION_TIME_UNIT, "DAYS");
    props.put(DataSource.RETENTION_TIME_VALUE, "30");
    props.put(DataSource.PUSH_FREQUENCY, "daily");
    props.put(DataSource.SEGMENT_ASSIGNMENT_STRATEGY, segmentAssignmentStrategy);
    props.put(DataSource.BROKER_TAG_NAME, resourceName);
    props.put(DataSource.NUMBER_OF_BROKER_INSTANCES, "1");
    return DataResource.fromMap(props);
  }

  public static DataResource createOfflineClusterDataResourceUpdateConfig(int numInstances, int numReplicas,
      String resourceName) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(DataSource.REQUEST_TYPE, DataSourceRequestType.UPDATE_DATA_RESOURCE);
    props.put(DataSource.RESOURCE_NAME, resourceName);
    props.put(DataSource.RESOURCE_TYPE, ResourceType.OFFLINE.toString());
    props.put(DataSource.TABLE_NAME, resourceName);
    props.put(DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numInstances));
    props.put(DataSource.NUMBER_OF_COPIES, String.valueOf(numReplicas));
    return DataResource.fromMap(props);
  }

  public static DataResource createOfflineClusterBrokerResourceUpdateConfig(int numInstances, String resourceName) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(DataSource.REQUEST_TYPE, DataSourceRequestType.UPDATE_BROKER_RESOURCE);
    props.put(DataSource.RESOURCE_NAME, resourceName);
    props.put(DataSource.RESOURCE_TYPE, ResourceType.OFFLINE.toString());
    props.put(DataSource.TABLE_NAME, resourceName);
    props.put(DataSource.BROKER_TAG_NAME, resourceName);
    props.put(DataSource.NUMBER_OF_BROKER_INSTANCES, String.valueOf(numInstances));
    return DataResource.fromMap(props);
  }

  public static DataResource createOfflineClusterAddTableToResource(String resourceName, String tableName) {
    final Map<String, String> props = new HashMap<String, String>();

    props.put(DataSource.REQUEST_TYPE, DataSourceRequestType.ADD_TABLE_TO_RESOURCE);
    props.put(DataSource.RESOURCE_NAME, resourceName);
    props.put(DataSource.RESOURCE_TYPE, ResourceType.OFFLINE.toString());
    props.put(DataSource.TABLE_NAME, tableName);

    return DataResource.fromMap(props);
  }

  public static DataResource createRealtimeClusterAddTableToResource(String resourceName, String tableName) {
    final Map<String, String> props = new HashMap<String, String>();

    props.put(DataSource.REQUEST_TYPE, DataSourceRequestType.ADD_TABLE_TO_RESOURCE);
    props.put(DataSource.NUMBER_OF_DATA_INSTANCES, "1");
    props.put(DataSource.RESOURCE_NAME, resourceName);
    props.put(DataSource.RESOURCE_TYPE, ResourceType.REALTIME.toString());
    props.put(DataSource.TABLE_NAME, tableName);

    return DataResource.fromMap(props);
  }

  public static BrokerDataResource createBrokerDataResourceConfig(String resourceName, int numInstances, String tag) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(Broker.DataResource.RESOURCE_NAME, resourceName);
    props.put(Broker.DataResource.NUM_BROKER_INSTANCES, numInstances + "");
    props.put(Broker.DataResource.TAG, tag);
    return BrokerDataResource.fromMap(props);
  }

  public static BrokerTagResource createBrokerTagResourceConfig(int numInstances, String tag) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(Broker.TagResource.TAG, tag);
    props.put(Broker.TagResource.NUM_BROKER_INSTANCES, numInstances + "");
    return BrokerTagResource.fromMap(props);
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances) throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String brokerId = "Broker_localhost_" + i;
      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServer);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new EmptyBrokerOnlineOfflineStateModelFactory();
      stateMachineEngine.registerStateModelFactory(EmptyBrokerOnlineOfflineStateModelFactory.getStateModelDef(),
          stateModelFactory);
      helixZkManager.connect();
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId, UNTAGGED_BROKER_INSTANCE);
      Thread.sleep(1000);
    }
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances) throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String instanceId = "Server_localhost_" + i;

      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new EmptySegmentOnlineOfflineStateModelFactory();
      stateMachineEngine.registerStateModelFactory(EmptySegmentOnlineOfflineStateModelFactory.getStateModelDef(),
          stateModelFactory);
      helixZkManager.connect();
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId, UNTAGGED_SERVER_INSTANCE);
    }
  }

}
