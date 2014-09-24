package com.linkedin.pinot.controller.helix.util;

import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.controller.helix.api.request.ReplicaRequest;
import com.linkedin.pinot.controller.helix.core.UpdateRequestJSONTransformer;


public class ClusterUpdateJSONUtils {
  private static String UPDATE_REQUEST_METADATA = "updateRequestMetadata";
  private static String UPDATE_TYPE = "updateType";
  private static String REPLICA_REQUEST_TYPE = "replicaRequestType";
  private static String REPLICA_SET = "replicaSet";

  public static JSONObject constructAddReplicaRequest(String resourceName, Map<String, String> instanceMap)
      throws JSONException {
    JSONObject json = new JSONObject();
    json.put(UPDATE_TYPE, UpdateRequestJSONTransformer.Action.replicaRequest.toString());
    JSONObject metadataJSON = new JSONObject();
    metadataJSON.put(REPLICA_REQUEST_TYPE, ReplicaRequest.Type.add.toString());
    metadataJSON.put("clusterName", resourceName);
    JSONArray replicaSet = contructReplicaSet(instanceMap);
    metadataJSON.put(REPLICA_SET, replicaSet);
    json.put(UPDATE_REQUEST_METADATA, metadataJSON);
    return json;
  }

  public static JSONObject constructRemoveReplicaRequest(String resourceName, Map<String, String> instanceMap)
      throws JSONException {
    JSONObject json = new JSONObject();
    json.put(UPDATE_TYPE, UpdateRequestJSONTransformer.Action.replicaRequest.toString());
    JSONObject metadataJSON = new JSONObject();
    metadataJSON.put(REPLICA_REQUEST_TYPE, ReplicaRequest.Type.remove.toString());
    metadataJSON.put("clusterName", resourceName);
    JSONArray replicaSet = contructReplicaSet(instanceMap);
    metadataJSON.put(REPLICA_SET, replicaSet);
    json.put(UPDATE_REQUEST_METADATA, metadataJSON);
    return json;
  }

  public static JSONObject constructBootstrapReplicaRequest(String resourceName, Map<String, String> instanceMap)
      throws JSONException {
    JSONObject json = new JSONObject();
    json.put(UPDATE_TYPE, UpdateRequestJSONTransformer.Action.replicaRequest.toString());

    JSONObject metadataJSON = new JSONObject();
    metadataJSON.put(REPLICA_REQUEST_TYPE, ReplicaRequest.Type.boostrap.toString());
    metadataJSON.put("clusterName", resourceName);

    JSONArray replicaSet = contructReplicaSet(instanceMap);
    metadataJSON.put(REPLICA_SET, replicaSet);

    JSONObject properties = new JSONObject();
    properties.put("d2ClusterName", "bootstrapD2");
    properties.put("clusterName", "bootstrap" + resourceName);
    metadataJSON.put("properties", properties);

    json.put(UPDATE_REQUEST_METADATA, metadataJSON);
    return json;
  }

  private static JSONArray contructReplicaSet(Map<String, String> replicaSetMap) throws JSONException {
    JSONArray replicaSet = new JSONArray();
    for (String key : replicaSetMap.keySet()) {
      JSONObject instance = new JSONObject();
      instance.put("partitionName", key);
      instance.put("instanceName", replicaSetMap.get(key));
      replicaSet.put(instance);
    }
    return replicaSet;
  }

  public static JSONObject constructDisableInstanceRequest(List<String> instanceNames, String resourceName)
      throws JSONException {
    JSONObject ret = new JSONObject();
    ret.put(UPDATE_TYPE, UpdateRequestJSONTransformer.Action.disableInstances.toString());
    JSONObject metadataJSON = new JSONObject();
    JSONArray instancesArrayJSON = new JSONArray();
    for (String instance : instanceNames) {
      JSONObject instanceJSON = new JSONObject();
      instanceJSON.put("name", instance);
      instancesArrayJSON.put(instanceJSON);
    }
    metadataJSON.put("instances", instancesArrayJSON);

    ret.put(UPDATE_REQUEST_METADATA, metadataJSON);

    return ret;
  }

  public static JSONObject constructEnableInstanceRequest(List<String> instanceNames, String resourceName)
      throws JSONException {
    JSONObject ret = new JSONObject();
    ret.put(UPDATE_TYPE, UpdateRequestJSONTransformer.Action.enableInstances.toString());
    JSONObject metadataJSON = new JSONObject();
    JSONArray instancesArrayJSON = new JSONArray();
    for (String instance : instanceNames) {
      JSONObject instanceJSON = new JSONObject();
      instanceJSON.put("name", instance);
      instancesArrayJSON.put(instanceJSON);
    }
    metadataJSON.put("instances", instancesArrayJSON);

    ret.put(UPDATE_REQUEST_METADATA, metadataJSON);
    return ret;
  }

  public static JSONObject constructDisableResourceRequest(String resourceName) throws JSONException {
    JSONObject ret = new JSONObject();
    ret.put(UPDATE_TYPE, UpdateRequestJSONTransformer.Action.disableResource.toString());
    JSONObject metadataJSON = new JSONObject();
    metadataJSON.put("clusterName", resourceName);
    ret.put(UPDATE_REQUEST_METADATA, metadataJSON);
    return ret;
  }

  public static JSONObject constructEnableResourceRequest(String resourceName) throws JSONException {
    JSONObject ret = new JSONObject();
    ret.put(UPDATE_TYPE, UpdateRequestJSONTransformer.Action.enableResouce.toString());
    JSONObject metadataJSON = new JSONObject();
    metadataJSON.put("clusterName", resourceName);
    ret.put(UPDATE_REQUEST_METADATA, metadataJSON);
    return ret;
  }

  public static JSONObject constructUpdateInstanceConfigRequest(String instanceName, String resourceName,
      String... keys) throws JSONException {
    if ((keys.length % 2) != 0) {
      throw new RuntimeException("expecting keys to be even");
    }

    JSONObject ret = new JSONObject();
    ret.put(UPDATE_TYPE, UpdateRequestJSONTransformer.Action.updateInstanceConfig.toString());
    JSONObject metadataJSON = new JSONObject();
    metadataJSON.put("clusterName", resourceName);
    metadataJSON.put("instanceName", instanceName);
    JSONObject config = new JSONObject();
    JSONObject external = new JSONObject();

    for (int i = 0; i < (keys.length - 1); i = i + 2) {
      String key = keys[i];
      String value = keys[i + 1];
      if (key.startsWith("external")) {
        external.put(key, value);
      } else {
        config.put(key, value);
      }
    }
    metadataJSON.put("config", config);
    metadataJSON.put("external", external);

    ret.put(UPDATE_REQUEST_METADATA, metadataJSON);

    return ret;
  }

  public static JSONObject constructUpdateResourceConfigRequest(String resourceName, String... keys)
      throws JSONException {
    if ((keys.length % 2) != 0) {
      throw new RuntimeException("expecting keys to be even");
    }

    JSONObject ret = new JSONObject();
    ret.put(UPDATE_TYPE, UpdateRequestJSONTransformer.Action.updateResourceConfig.toString());
    JSONObject metadataJSON = new JSONObject();
    metadataJSON.put("clusterName", resourceName);
    JSONObject config = new JSONObject();
    JSONObject external = new JSONObject();

    for (int i = 0; i < (keys.length - 1); i = i + 2) {
      String key = keys[i];
      String value = keys[i + 1];
      if (key.startsWith("external")) {
        external.put(key, value);
      } else {
        config.put(key, value);
      }
    }
    metadataJSON.put("config", config);
    metadataJSON.put("external", external);

    ret.put(UPDATE_REQUEST_METADATA, metadataJSON);

    return ret;
  }

  public static void main(String[] args) throws JSONException {
    System.out.println(constructUpdateInstanceConfigRequest("instance", "resource", "1", "2", "1", "2", "1", "2")
        .toString(1));
    System.out.println(constructUpdateResourceConfigRequest("resource", "1", "2", "1", "2", "1", "2").toString(1));
  }
}
