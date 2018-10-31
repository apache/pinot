package com.linkedin.thirdeye.anomaly.alert;

import com.linkedin.thirdeye.anomaly.alert.v2.AlertJobSchedulerV2;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.NullArgumentException;
import org.quartz.SchedulerException;

@Path("/alert-job")
@Produces(MediaType.APPLICATION_JSON)
@Deprecated  // not used
public class AlertJobResource {
  private final AlertJobSchedulerV2 alertJobScheduler;
  private final AlertConfigManager alertConfigurationDAO;

  public AlertJobResource(AlertJobSchedulerV2 alertJobScheduler,
      AlertConfigManager alertConfigurationDAO) {
    this.alertJobScheduler = alertJobScheduler;
    this.alertConfigurationDAO = alertConfigurationDAO;
  }

  @GET
  public List<String> showActiveJobs() throws SchedulerException {
    return alertJobScheduler.getScheduledJobs();
  }

  @POST
  @Path("/{id}")
  public Response enable(@PathParam("id") Long id) throws Exception {
    toggleActive(id, true);
    alertJobScheduler.startJob(id);
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
    AlertConfigDTO alertConfig = alertConfigurationDAO.findById(id);
    if(alertConfig == null) {
      throw new NullArgumentException("Alert config not found");
    }
    alertConfig.setActive(state);
    alertConfigurationDAO.update(alertConfig);
  }

  @POST
  @Path("/{id}/restart")
  public Response restart(@PathParam("id") Long id) throws Exception {
    alertJobScheduler.stopJob(id);
    alertJobScheduler.startJob(id);
    return Response.ok().build();
  }
}
