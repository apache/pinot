package com.linkedin.pinot.controller.helix.core;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.controller.helix.api.request.ReplicaRequest;
import com.linkedin.pinot.controller.helix.api.request.ToggleInstancesRequest;
import com.linkedin.pinot.controller.helix.api.request.ToggleResourceRequest;
import com.linkedin.pinot.controller.helix.api.request.UpdateInstanceConfigUpdateRequest;
import com.linkedin.pinot.controller.helix.api.request.UpdateResourceConfigUpdateRequest;


public class UpdateRequestJSONTransformer {
  public static final Logger logger = Logger.getLogger(UpdateRequestJSONTransformer.class);

  public enum Action {
    replicaRequest,
    updateInstanceConfig, // you want to update configs for one or more instances
    updateResourceConfig, // you want to update configs for all the instances in a cluster
    enableInstances,
    disableInstances,
    enableResouce,
    disableResource,
    deleteInstanceConfig,
    resetInstance,
    deleteResourceConfig,
    updateIdealState;
  }

  public static ReplicaRequest transformReplicaRequest(JSONObject request) throws JSONException {
    ReplicaRequest.Type type = ReplicaRequest.Type.valueOf(request.getString("replicaRequestType"));
    ReplicaRequest objectifiedRequest = new ReplicaRequest(type, request.getString("clusterName"));
    JSONArray instances = request.getJSONArray("replicaSet");

    for (int i = 0; i < instances.length(); i++) {
      objectifiedRequest.addInstanceToSet(instances.getJSONObject(i).getString("partitionName"), instances
          .getJSONObject(i).getString("instanceName"));
    }

    if (type == ReplicaRequest.Type.boostrap) {
      JSONObject properties = request.getJSONObject("properties");
      objectifiedRequest.putInOptionalMap("d2ClusterName", properties.getString("d2ClusterName"));
      objectifiedRequest.putInOptionalMap("clusterName", properties.getString("clusterName"));
    }
    return objectifiedRequest;
  }

  public static ToggleResourceRequest transformEnableResourceRequest(JSONObject request) throws JSONException {
    String resourceName = request.getString("clusterName");
    return new ToggleResourceRequest(resourceName, true);
  }

  public static ToggleResourceRequest transformDisableResourceRequest(JSONObject request) throws JSONException {
    String resourceName = request.getString("clusterName");
    return new ToggleResourceRequest(resourceName, false);
  }

  /*
   * enable instances
   */
  public static ToggleInstancesRequest transformEnableInstancesRequest(JSONObject request) throws JSONException {
    JSONArray instanceNamesArray = request.getJSONArray("instances");
    Set<String> instanceNameSet = new HashSet<String>();
    for (int i = 0; i < instanceNamesArray.length(); i++) {
      JSONObject instance = instanceNamesArray.getJSONObject(i);
      instanceNameSet.add(instance.getString("name"));
    }
    return new ToggleInstancesRequest(true, instanceNameSet);
  }

  /*
   * 
   * disable instances
   */
  public static ToggleInstancesRequest transformDisableInstancesRequest(JSONObject request) throws JSONException {
    JSONArray instanceNamesArray = request.getJSONArray("instances");
    Set<String> instanceNameSet = new HashSet<String>();

    for (int i = 0; i < instanceNamesArray.length(); i++) {
      JSONObject instance = instanceNamesArray.getJSONObject(i);
      instanceNameSet.add(instance.getString("name"));
    }

    return new ToggleInstancesRequest(false, instanceNameSet);
  }

  /*
   * Update Instance Configs, will only update one instance at a time
   */
  public static UpdateInstanceConfigUpdateRequest transformUpdateInstanceConfigRequest(JSONObject incomingUpdateRequest)
      throws JSONException {
    UpdateInstanceConfigUpdateRequest updateInstanceConfig =
        new UpdateInstanceConfigUpdateRequest(incomingUpdateRequest.getString("instanceName"));
    updateInstanceConfig.setBounceService(incomingUpdateRequest.getBoolean("bounce"));

    JSONArray configs = incomingUpdateRequest.getJSONArray("config");

    for (int i = 0; i < configs.length(); i++) {
      JSONObject config = configs.getJSONObject(i);
      updateInstanceConfig.addPropertyToUpdate(config.getString("key"), config.getString("value"));
    }

    return updateInstanceConfig;
  }

  /*
   * Update Resource Configs, will apply to all instances in a given resource
   */
  public static UpdateResourceConfigUpdateRequest transformUpdateResourceConfigUpdateRequest(
      JSONObject incomingUpdateRequest) throws JSONException {
    UpdateResourceConfigUpdateRequest updateResourceConfig =
        new UpdateResourceConfigUpdateRequest(incomingUpdateRequest.getString("clusterName"));
    updateResourceConfig.setBounceService(incomingUpdateRequest.getBoolean("bounce"));

    JSONArray configs = incomingUpdateRequest.getJSONArray("config");

    for (int i = 0; i < configs.length(); i++) {
      JSONObject config = configs.getJSONObject(i);
      updateResourceConfig.addProperty(config.getString("key"), config.getString("value"));
    }

    return updateResourceConfig;
  }

}
