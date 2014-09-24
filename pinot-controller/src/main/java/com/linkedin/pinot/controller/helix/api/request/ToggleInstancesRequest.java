package com.linkedin.pinot.controller.helix.api.request;

import java.util.Set;


public class ToggleInstancesRequest {
  private final boolean toggle;
  private final Set<String> pinotInstance;

  public ToggleInstancesRequest(boolean status, Set<String> pinotInstance) {
    this.toggle = status;
    this.pinotInstance = pinotInstance;
  }

  public boolean isToggle() {
    return toggle;
  }

  public Set<String> getPinotInstance() {
    return pinotInstance;
  }

}
