package org.apache.pinot.thirdeye.resources;

import javax.inject.Inject;
import javax.ws.rs.Path;

public class ApiResource {

  private final AuthResource authResource;
  private final ApplicationResource applicationResource;

  @Inject
  public ApiResource(final AuthResource authResource,
      final ApplicationResource applicationResource) {
    this.authResource = authResource;
    this.applicationResource = applicationResource;
  }

  @Path("auth")
  public AuthResource getAuthResource() {
    return authResource;
  }

  @Path("applications")
  public ApplicationResource getApplicationResource() {
    return applicationResource;
  }
}
