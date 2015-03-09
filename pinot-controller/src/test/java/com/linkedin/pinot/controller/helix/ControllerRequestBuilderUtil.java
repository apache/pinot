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

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.controller.api.pojos.BrokerDataResource;
import com.linkedin.pinot.controller.api.pojos.BrokerTagResource;
import com.linkedin.pinot.controller.api.pojos.DataResource;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 29, 2014
 */

public class ControllerRequestBuilderUtil {

  public static JSONObject buildCreateResourceJSON(String resourceName, int numInstances, int numReplicas)
      throws JSONException {
    DataResource dataSource =
        createOfflineClusterCreationConfig(numInstances, numReplicas, resourceName,
            "BalanceNumSegmentAssignmentStrategy");
    final JSONObject ret = dataSource.toJSON();

    return ret;
  }

  public static JSONObject buildUpdateDataResourceJSON(String resourceName, int numInstances, int numReplicas)
      throws JSONException {
    DataResource dataSource = createOfflineClusterDataResourceUpdateConfig(numInstances, numReplicas, resourceName);
    final JSONObject ret = dataSource.toJSON();

    return ret;
  }

  public static JSONObject buildUpdateBrokerResourceJSON(String resourceName, int numInstances) throws JSONException {
    DataResource dataSource = createOfflineClusterBrokerResourceUpdateConfig(numInstances, resourceName);
    final JSONObject ret = dataSource.toJSON();

    return ret;
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
      ins.put("tag", CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      ret.put(ins);
    }

    return ret;
  }

  public static DataResource createOfflineClusterCreationConfig(int numInstances, int numReplicas, String resourceName,
      String segmentAssignmentStrategy) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Helix.DataSource.REQUEST_TYPE, CommonConstants.Helix.DataSourceRequestType.CREATE);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_TYPE, Helix.ResourceType.OFFLINE.toString());
    props.put(CommonConstants.Helix.DataSource.TABLE_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.TIME_COLUMN_NAME, "days");
    props.put(CommonConstants.Helix.DataSource.TIME_TYPE, "daysSinceEpoch");
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numInstances));
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(numReplicas));
    props.put(CommonConstants.Helix.DataSource.RETENTION_TIME_UNIT, "DAYS");
    props.put(CommonConstants.Helix.DataSource.RETENTION_TIME_VALUE, "30");
    props.put(CommonConstants.Helix.DataSource.PUSH_FREQUENCY, "daily");
    props.put(CommonConstants.Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, segmentAssignmentStrategy);
    props.put(CommonConstants.Helix.DataSource.BROKER_TAG_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, "1");
    final DataResource res = DataResource.fromMap(props);
    return res;
  }

  public static DataResource createOfflineClusterDataResourceUpdateConfig(int numInstances, int numReplicas,
      String resourceName) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Helix.DataSource.REQUEST_TYPE, CommonConstants.Helix.DataSourceRequestType.UPDATE_DATA_RESOURCE);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_TYPE, Helix.ResourceType.OFFLINE.toString());
    props.put(CommonConstants.Helix.DataSource.TABLE_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numInstances));
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(numReplicas));
    final DataResource res = DataResource.fromMap(props);
    return res;
  }

  public static DataResource createOfflineClusterBrokerResourceUpdateConfig(int numInstances, String resourceName) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Helix.DataSource.REQUEST_TYPE, CommonConstants.Helix.DataSourceRequestType.UPDATE_BROKER_RESOURCE);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_TYPE, Helix.ResourceType.OFFLINE.toString());
    props.put(CommonConstants.Helix.DataSource.TABLE_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.BROKER_TAG_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, String.valueOf(numInstances));
    final DataResource res = DataResource.fromMap(props);
    return res;
  }

  public static DataResource createOfflineClusterAddTableToResource(String resourceName, String tableName) {
    final Map<String, String> props = new HashMap<String, String>();

    props.put(CommonConstants.Helix.DataSource.REQUEST_TYPE,
        CommonConstants.Helix.DataSourceRequestType.ADD_TABLE_TO_RESOURCE);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_TYPE, Helix.ResourceType.OFFLINE.toString());
    props.put(CommonConstants.Helix.DataSource.TABLE_NAME, tableName);

    return DataResource.fromMap(props);
  }

  public static BrokerDataResource createBrokerDataResourceConfig(String resourceName, int numInstances, String tag) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Broker.DataResource.RESOURCE_NAME, resourceName);
    props.put(CommonConstants.Broker.DataResource.NUM_BROKER_INSTANCES, numInstances + "");
    props.put(CommonConstants.Broker.DataResource.TAG, tag);
    final BrokerDataResource res = BrokerDataResource.fromMap(props);
    return res;
  }

  public static BrokerTagResource createBrokerTagResourceConfig(int numInstances, String tag) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Broker.TagResource.TAG, tag);
    props.put(CommonConstants.Broker.TagResource.NUM_BROKER_INSTANCES, numInstances + "");
    final BrokerTagResource res = BrokerTagResource.fromMap(props);
    return res;
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
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
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
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId,
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
  }

}
