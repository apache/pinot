package com.linkedin.thirdeye.resources;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.EmailConfiguration;
import com.linkedin.thirdeye.db.EmailConfigurationDAO;
import com.linkedin.thirdeye.email.EmailReportJobManager;
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

@Path("/email-reports")
@Produces(MediaType.APPLICATION_JSON)
public class EmailReportResource {
  private final EmailConfigurationDAO dao;
  private final EmailReportJobManager manager;

  public EmailReportResource(EmailConfigurationDAO dao, EmailReportJobManager manager) {
    this.dao = dao;
    this.manager = manager;
  }

  @POST
  @Timed
  @UnitOfWork
  public Response create(EmailConfiguration configuration) {
    Long id = dao.create(configuration);
    return Response.ok(id).build();
  }

  @POST
  @Timed
  @UnitOfWork
  @Path("/{id}/ad-hoc")
  public Response sendAdHoc(@PathParam("id") Long id) throws Exception {
    manager.sendAdHoc(id);
    return Response.ok().build();
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
