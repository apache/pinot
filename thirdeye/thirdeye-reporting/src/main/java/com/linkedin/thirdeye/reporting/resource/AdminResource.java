package com.linkedin.thirdeye.reporting.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;



@Path("/")
@Produces(MediaType.TEXT_PLAIN)
public class AdminResource
{

  @GET
  public Response returnNoResponse()
  {
    return Response.noContent().build();
  }

  @GET
  @Path("/admin")
  public String sayGood()
  {
    return "GOOD";
  }

}
