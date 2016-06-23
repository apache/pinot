package com.linkedin.thirdeye.detector.resources;

import io.dropwizard.hibernate.UnitOfWork;

import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.detector.api.EmailConfiguration;
import com.linkedin.thirdeye.detector.db.EmailConfigurationDAO;
import com.linkedin.thirdeye.detector.email.EmailReportJobManager;

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
    Long id = dao.createOrUpdate(configuration);
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
