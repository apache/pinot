package com.linkedin.thirdeye.anomaly.alert;

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

import org.apache.commons.lang.NullArgumentException;
import org.quartz.SchedulerException;

@Path("/alert-job")
@Produces(MediaType.APPLICATION_JSON)
public class AlertJobResource {
  private final AlertJobScheduler alertJobScheduler;
  private final EmailConfigurationDAO emailConfigurationDAO;

  public AlertJobResource(AlertJobScheduler alertJobScheduler,
      EmailConfigurationDAO emailConfigurationDAO) {
    this.alertJobScheduler = alertJobScheduler;
    this.emailConfigurationDAO = emailConfigurationDAO;
  }

  @GET
  public List<String> showActiveJobs() throws SchedulerException {
    return alertJobScheduler.getActiveJobs();
  }

  @POST
  @Path("/{id}")
  public Response enable(@PathParam("id") Long id) throws Exception {
    toggleActive(id, true);
    alertJobScheduler.startJob(id);
    return Response.ok().build();
  }


  @POST
  @Path("/{id}/ad-hoc")
  public Response adHoc(@PathParam("id") Long id, @QueryParam("start") String start,
      @QueryParam("end") String end) throws Exception {
    alertJobScheduler.runAdHoc(id, start, end);
    return Response.ok().build();
  }

  @DELETE
  @Path("/{id}")
  public Response disable(@PathParam("id") Long id) throws Exception {
    toggleActive(id, false);
    alertJobScheduler.stopJob(id);
    return Response.ok().build();
  }

  private void toggleActive(Long id, boolean state) {
    EmailConfiguration alertConfig = emailConfigurationDAO.findById(id);
    if(alertConfig == null) {
      throw new NullArgumentException("Alert config not found");
    }
    alertConfig.setIsActive(state);
    emailConfigurationDAO.save(alertConfig);
  }
}
