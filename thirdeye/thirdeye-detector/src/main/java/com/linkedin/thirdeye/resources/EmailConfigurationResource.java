package com.linkedin.thirdeye.resources;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.EmailConfiguration;
import com.linkedin.thirdeye.db.EmailConfigurationDAO;
import io.dropwizard.hibernate.UnitOfWork;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/email-configurations")
@Produces(MediaType.APPLICATION_JSON)
public class EmailConfigurationResource {
  private final EmailConfigurationDAO dao;

  public EmailConfigurationResource(EmailConfigurationDAO dao) {
    this.dao = dao;
  }

  @POST
  @Timed
  @UnitOfWork
  public Response create(EmailConfiguration configuration) {
    Long id = dao.create(configuration);
    return Response.ok(id).build();
  }

  @DELETE
  @Timed
  @UnitOfWork
  @Path("/{id}")
  public Response delete(@PathParam("id") Long id) {
    dao.delete(id);
    return Response.noContent().build();
  }

  @GET
  @Timed
  @UnitOfWork
  @Path("/{id}")
  public EmailConfiguration find(@PathParam("id") Long id) {
    EmailConfiguration configuration = dao.findById(id);
    if (configuration == null) {
      throw new NotFoundException();
    }
    return configuration;
  }

  @GET
  @Timed
  @UnitOfWork
  public List<EmailConfiguration> findAll() {
    return dao.findAll();
  }
}
