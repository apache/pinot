package com.linkedin.pinot.controller.helix.api.request;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.controller.helix.api.PinotInstance;


public class ReplicaRequest {

  private final Type type;
  private String resourceName;
  private Map<String, String> replicaSetMap;
  private Map<String, String> optionalProperties;

  public ReplicaRequest(Type type, String resourceName) {
    this.type = type;
    this.resourceName = resourceName;
    this.replicaSetMap = new HashMap<String, String>();
    if (type == Type.boostrap) {
      optionalProperties = new HashMap<String, String>();
    }

  }

  public String getString() {
    return resourceName;
  }

  public void addInstanceToSet(String partitionName, String instanceName) {
    replicaSetMap.put(partitionName, instanceName);
  }

  public void putInOptionalMap(String key, String value) {
    this.optionalProperties.put(key, value);
  }

  public Type getType() {
    return type;
  }

  public Map<String, String> getReplicaSetMap() {
    return replicaSetMap;
  }

  public String getResourceName() {
    return this.resourceName;
  }

  public String getD2ClusterName() {
    return optionalProperties.get("d2ClusterName");
  }

  public String getNewResourceName() {
    return optionalProperties.get("clusterName");
  }

  public Map<String, String> getNewResourceReplicaSetMap() {
    Map<String, String> ret = new HashMap<String, String>();
    for (String oldPartitionName : this.replicaSetMap.keySet()) {
      String newPartitionName = this.getNewResourceName() + "_" + oldPartitionName.split("_")[1];
      ret.put(newPartitionName, this.getReplicaSetMap().get(oldPartitionName));
    }
    return ret;
  }

  public static PinotInstance getPinotInstanceFor(String partitionName, String instanceName, String nodeID) {

    String host = instanceName.split("_")[0];
    String port = instanceName.split("_")[1];
    String senseiPartitions = partitionName.split("_")[1];
    PinotInstance pinotInstance = new PinotInstance();
    pinotInstance.setHost(host);
    pinotInstance.setPort(port);
    pinotInstance.getInstanceConfigs().put("sensei.node.id", String.valueOf(nodeID));
    pinotInstance.getInstanceConfigs().put("sensei.node.partitions", senseiPartitions);
    return pinotInstance;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("type : " + type.toString() + "\n");
    builder.append("resourceName : " + resourceName + "\n");
    for (String key : replicaSetMap.keySet()) {
      builder.append("key : " + key + " , value : " + replicaSetMap.get(key) + " \n");
    }

    if (optionalProperties != null) {
      for (String key : optionalProperties.keySet()) {
        builder.append("key : " + key + " , value : " + optionalProperties.get(key) + " \n");
      }
    }

    return builder.toString();
  }

  public enum Type {
    boostrap,
    remove,
    add;
  }
}
