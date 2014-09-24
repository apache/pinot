package com.linkedin.pinot.controller.helix.api;

import java.util.Map;

import com.linkedin.pinot.controller.helix.properties.OfflineProperties;
import com.linkedin.pinot.controller.helix.properties.RealtimeProperties;


public class PinotStandaloneResource extends PinotResource {
  private Map<String, String> resourceProperties;
  private StandaloneType _resourceType;

  private int _numReplicas;
  private int _numInstancesPerReplica;

  public enum StandaloneType {
    offline,
    realtime;
  }

  public PinotStandaloneResource(StandaloneType type, String numReplicas, String numInstancesPerReplica) {
    this._resourceType = type;
    this._numReplicas = Integer.parseInt(numReplicas);
    this._numInstancesPerReplica = Integer.parseInt(numInstancesPerReplica);
  }

  @Override
  ResourceType getType() {
    return ResourceType.SINGULAR;
  }

  public int getNumInstancesPerReplica() {
    return _numInstancesPerReplica;
  }

  public String getResourceName() {
    if (_resourceType == StandaloneType.offline) {
      return resourceProperties.get(OfflineProperties.RESOURCE_LEVEL.pinot_resource_name.toString());
    } else if (_resourceType == StandaloneType.realtime) {
      return resourceProperties.get(RealtimeProperties.RESOURCE_LEVEL.sensei_cluster_name.toString());
    }
    throw new UnsupportedOperationException("Not Support Resource Type: " + _resourceType);
  }

  public int getNumReplicas() {
    return this._numReplicas;
  }

  public Map<String, String> getResourceProperties() {
    return resourceProperties;
  }

  public void setResourceProperties(Map<String, String> resourceProperties) {
    this.resourceProperties = resourceProperties;
  }

  public StandaloneType getResourceType() {
    return _resourceType;
  }

  public String getTag() {
    return getResourceName() + "_" + getResourceType();
  }

  public void print() {
    System.out.println("*********************************************************************");
    System.out.println("type : " + _resourceType.toString());
    System.out.println("*********************** Resource Properties Start *************************");
    for (String key : resourceProperties.keySet()) {
      System.out.println(key + " : " + resourceProperties.get(key));
    }

    System.out.println("*********************** Resource Properties  End  *************************");
  }
}
