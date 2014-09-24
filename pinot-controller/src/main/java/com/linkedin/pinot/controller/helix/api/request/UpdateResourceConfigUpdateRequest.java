package com.linkedin.pinot.controller.helix.api.request;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class UpdateResourceConfigUpdateRequest {
  private static final String EXTERNAL_PROPS_KEY_PREFIX = "external.";

  private String _resourceName;
  private boolean _bounceService = false;

  private Map<String, String> _propertiesMapToUpdate;
  private Set<String> _propertiesKeySetToDelete;

  public UpdateResourceConfigUpdateRequest(String resourceName) {
    this._resourceName = resourceName;
    this._propertiesMapToUpdate = new HashMap<String, String>();
    this._propertiesKeySetToDelete = new HashSet<String>();
  }

  public boolean isBounceService() {
    return _bounceService;
  }

  public void setBounceService(boolean bounceService) {
    this._bounceService = bounceService;
  }

  public void addProperty(String key, String value) {
    this._propertiesMapToUpdate.put(key, value);
  }

  public void removeProperty(String key) {
    this._propertiesKeySetToDelete.add(key);
  }

  public String getResourceName() {
    return _resourceName;
  }

  public Map<String, String> getPropertiesMapToUpdate() {
    return _propertiesMapToUpdate;
  }

  public Set<String> getPropertiesKeySetToDelete() {
    return _propertiesKeySetToDelete;
  }

}
