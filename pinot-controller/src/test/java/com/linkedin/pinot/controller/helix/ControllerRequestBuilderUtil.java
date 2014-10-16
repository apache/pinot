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
import com.linkedin.pinot.controller.api.pojos.BrokerDataResource;
import com.linkedin.pinot.controller.api.pojos.BrokerTagResource;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.server.starter.helix.SegmentOnlineOfflineStateModelFactory;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 29, 2014
 */

public class ControllerRequestBuilderUtil {

  public static JSONObject buildCreateResourceJSON(String resourceName, int numInstances, int numReplicas)
      throws JSONException {
    final JSONObject ret =
        new JSONObject(createOfflineClusterConfig(numInstances, numReplicas, resourceName,
            "BalanceNumSegmentAssignmentStrategy"));

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

  public static DataResource createOfflineClusterConfig(int numInstances, int numReplicas, String resourceName,
      String segmentAssignmentStrategy) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("resourceName", resourceName);
    props.put("tableName", resourceName);
    props.put("timeColumnName", "days");
    props.put("timeType", "daysSinceEpoch");
    props.put("numberOfDataInstances", String.valueOf(numInstances));
    props.put("numberOfCopies", String.valueOf(numReplicas));
    props.put("retentionTimeUnit", "DAYS");
    props.put("retentionTimeValue", "30");
    props.put("pushFrequency", "daily");
    props.put("segmentAssignmentStrategy", segmentAssignmentStrategy);
    props.put("brokerTagName", resourceName);
    props.put("numberOfBrokerInstances", "1");
    final DataResource res = DataResource.fromMap(props);
    return res;
  }

  public static BrokerDataResource createBrokerDataResourceConfig(String resourceName, int numInstances, String tag) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("resourceName", resourceName);
    props.put("numBrokerInstances", numInstances + "");
    props.put("tag", tag);
    final BrokerDataResource res = BrokerDataResource.fromMap(props);
    return res;
  }

  public static BrokerTagResource createBrokerTagResourceConfig(int numInstances, String tag) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("tag", tag);
    props.put("numBrokerInstances", numInstances + "");
    final BrokerTagResource res = BrokerTagResource.fromMap(props);
    return res;
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances) throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String brokerId = "Broker_localhost_" + i;
      final HelixManager helixManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServer);
      helixManager.connect();
      helixManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId,
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
      final StateModelFactory<?> stateModelFactory = new SegmentOnlineOfflineStateModelFactory();
      stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelDef(),
          stateModelFactory);
      helixZkManager.connect();
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId,
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
  }

}
