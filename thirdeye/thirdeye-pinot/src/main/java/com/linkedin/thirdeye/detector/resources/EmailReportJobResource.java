package com.linkedin.thirdeye.detector.resources;

import com.linkedin.thirdeye.db.dao.EmailConfigurationDAO;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;

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
  public List<Long> showActiveJobs() {
    return manager.getActiveJobs();
  }

  @POST
  @Path("/{id}")
  public Response enable(@PathParam("id") Long id) throws Exception {
    EmailConfiguration emailConfiguration = configDAO.findById(id);
    emailConfiguration.setIsActive(true);
    configDAO.save(emailConfiguration);
    manager.start(id);
    return Response.ok().build();
  }

  @POST
  @Path("/{id}/ad-hoc")
  public Response adHoc(@PathParam("id") Long id) throws Exception {
    manager.sendAdHoc(id);
    return Response.ok().build();
  }

  @POST
  @Path("/from-file")
  public Response adHocFromFile(@QueryParam("filePath") String filePath) throws Exception {
    manager.runAdhocFile(filePath);
    return Response.ok().build();
  }

  @DELETE
  @Path("/{id}")
  public Response disable(@PathParam("id") Long id) throws Exception {
    EmailConfiguration emailConfiguration = configDAO.findById(id);
    emailConfiguration.setIsActive(false);
    configDAO.save(emailConfiguration);
    manager.stop(id);
    return Response.ok().build();
  }
}
