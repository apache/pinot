package com.linkedin.thirdeye.resource;

import java.net.URI;

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
  public Response returnDefaultDashboard()
  {
    return Response.seeOther(URI.create("/dashboard")).build();
  }

}
