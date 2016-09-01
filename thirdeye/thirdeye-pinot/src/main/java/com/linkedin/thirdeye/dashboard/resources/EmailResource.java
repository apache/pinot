package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.db.entity.EmailConfiguration;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("thirdeye/email")
@Produces(MediaType.APPLICATION_JSON)
public class EmailResource {

  @POST
  public Response createEmailConfig() {
    return null;
  }

  @GET
  @Path("{id}")
  public EmailConfiguration getEmailConfigById (@PathParam("id") Long id) {
    return null;
  }

  @GET
  public List<EmailConfiguration> getAllEmailconfigurations() {
    return null;
  }

  @POST
  @Path("{emailId}/{functionId}")
  public Response addFunctionInEmail() {
    return null;
  }

  @DELETE
  @Path("{emailId}/{functionId}")
  public Response removeFunctionFromEmail() {
    return null;
  }
}
