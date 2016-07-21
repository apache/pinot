package com.linkedin.thirdeye.detector.resources;

import io.dropwizard.hibernate.UnitOfWork;

import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.linkedin.thirdeye.detector.db.dao.EmailConfigurationDAO;
import com.linkedin.thirdeye.detector.email.EmailReportJobManager;

@Path("/email-jobs")
@Produces(MediaType.APPLICATION_JSON)
public class EmailReportJobResource {
  private final EmailReportJobManager manager;
  private final EmailConfigurationDAO configDAO;

  public EmailReportJobResource(EmailReportJobManager manager,
      EmailConfigurationDAO configDAO) {
    this.manager = manager;
    this.configDAO = configDAO;
  }

  @GET
  @UnitOfWork
  public List<Long> showActiveJobs() {
    return manager.getActiveJobs();
  }

  @POST
  @Path("/{id}")
  @UnitOfWork
  public Response enable(@PathParam("id") Long id) throws Exception {
    configDAO.toggleActive(id, true);
    manager.start(id);
    return Response.ok().build();
  }

  @POST
  @Path("/{id}/ad-hoc")
  @UnitOfWork
  public Response adHoc(@PathParam("id") Long id) throws Exception {
    manager.sendAdHoc(id);
    return Response.ok().build();
  }

  @POST
  @Path("/from-file")
  @UnitOfWork
  public Response adHocFromFile(@QueryParam("filePath") String filePath) throws Exception {
    manager.runAdhocFile(filePath);
    return Response.ok().build();
  }

  @DELETE
  @Path("/{id}")
  @UnitOfWork
  public Response disable(@PathParam("id") Long id) throws Exception {
    configDAO.toggleActive(id, false);
    manager.stop(id);
    return Response.ok().build();
  }
}
