package org.apache.pinot.thirdeye.resources;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

public class ApplicationResource {

  @GET
  public Response getAll() {
    return Response
        .ok("List all applications.")
        .build();
  }

  @POST
  public Response createMultiple() {
    return Response
        .ok("Create multiple applications.")
        .build();
  }

  @PUT
  public Response editMultiple() {
    return Response
        .ok("Edit multiple applications.")
        .build();
  }

  @GET
  @Path("{id}")
  public Response get(@PathParam("id") Integer id) {
    return Response
        .ok("Get application: " + id)
        .build();
  }

  @DELETE
  @Path("{id}")
  public Response delete(@PathParam("id") Integer id) {
    return Response
        .ok("Delete application: " + id)
        .build();
  }
}
