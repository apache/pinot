package org.apache.pinot.thirdeye.resources;

import io.swagger.annotations.Api;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/", tags = "Root")
public class RootResource {

  private final ApiResource apiResource;

  @Inject
  public RootResource(final ApiResource apiResource) {
    this.apiResource = apiResource;
  }

  @GET
  public Response home() {
    return Response
        .ok("ThirdEye Coordinator is up and running.")
        .build();
  }

  @Path("api")
  public ApiResource getApiResource() {
    return apiResource;
  }
}
