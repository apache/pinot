package com.linkedin.thirdeye.detector.resources;

import com.linkedin.thirdeye.db.dao.EmailConfigurationDAO;

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
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
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
  public Response create(EmailConfiguration configuration) {
    Long id = dao.save(configuration);
    return Response.ok(id).build();
  }

  @DELETE
  @Timed
  @Path("/{id}")
  public Response delete(@PathParam("id") Long id) {
    dao.deleteById(id);
    return Response.noContent().build();
  }

  @GET
  @Timed
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
  public List<EmailConfiguration> findAll() {
    return dao.findAll();
  }
}
