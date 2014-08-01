package com.linkedin.pinot.routing;

/**
 * Routing table lookup request. Future filtering parameters for lookup needs to be added here.
 * 
 * @author bvaradar
 *
 */
public class RoutingTableLookupRequest {

  private final String resourceName;

  public String getResourceName() {
    return resourceName;
  }

  public RoutingTableLookupRequest(String resourceName) {
    super();
    this.resourceName = resourceName;
  }
}
