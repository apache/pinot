package com.linkedin.thirdeye.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * A resource that simply returns "GOOD" to notify any tooling that it is alive.
 */
@Path("/admin")
@Produces(MediaType.TEXT_PLAIN)
public class PingResource
{
  @GET
  public String sayGood()
  {
    return "GOOD";
  }
}
