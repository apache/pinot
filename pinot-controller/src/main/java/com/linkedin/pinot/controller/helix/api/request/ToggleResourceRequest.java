package com.linkedin.pinot.controller.helix.api.request;

public class ToggleResourceRequest {
  private final String resourceName;
  private final boolean toggle;

  public ToggleResourceRequest(String clusterName, boolean toggle) {
    this.resourceName = clusterName;
    this.toggle = toggle;
  }

  public String getResourceName() {
    return resourceName;
  }

  public boolean isToggle() {
    return toggle;
  }

}
