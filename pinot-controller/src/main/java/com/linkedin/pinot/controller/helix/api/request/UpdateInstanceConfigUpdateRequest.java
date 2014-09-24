package com.linkedin.pinot.controller.helix.api.request;

import java.util.HashMap;
import java.util.Map;


public class UpdateInstanceConfigUpdateRequest {
  private static final String EXTERNAL_PROPS_KEY_PREFIX = "external.";
  private String instanceName;
  private boolean bounceService;
  private Map<String, String> propertiesToUpdate;

  public UpdateInstanceConfigUpdateRequest(String instanceName) {
    this.instanceName = instanceName;
    this.propertiesToUpdate = new HashMap<String, String>();
    this.bounceService = false;
  }

  public void addPropertyToUpdate(String key, String value) {
    this.propertiesToUpdate.put(key, value);
  }

  public String getInstanceName() {
    return instanceName;

  }

  public Map<String, String> getAllProperties() {
    return this.propertiesToUpdate;
  }

  public boolean isBounceService() {
    return bounceService;
  }

  public void setBounceService(boolean bounceService) {
    this.bounceService = bounceService;
  }

}
