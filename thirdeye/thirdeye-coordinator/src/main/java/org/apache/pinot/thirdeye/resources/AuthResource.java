package org.apache.pinot.thirdeye.resources;

import com.codahale.metrics.annotation.Timed;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

public class AuthResource {

  @Timed
  @Path("/login")
  @POST
  public Response login() {
    return Response.ok().build();
  }

  @Timed
  @Path("/logout")
  @POST
  public Response logout() {
    return Response.ok().build();
  }
}
