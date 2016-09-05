package com.linkedin.thirdeye.anomaly.alert;

import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

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
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.SchedulerException;

@Path("/alert-job")
@Produces(MediaType.APPLICATION_JSON)
public class AlertJobResource {
  private final AlertJobScheduler alertJobScheduler;
  private final EmailConfigurationManager emailConfigurationDAO;

  public AlertJobResource(AlertJobScheduler alertJobScheduler,
      EmailConfigurationManager emailConfigurationDAO) {
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
  public Response adHoc(@PathParam("id") Long id, @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso) throws Exception {
    DateTime startTime = null;
    DateTime endTime = null;
    if (StringUtils.isNotBlank(startTimeIso)) {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    }
    if (StringUtils.isNotBlank(endTimeIso)) {
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
    }
    alertJobScheduler.runAdHoc(id, startTime, endTime);
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
    EmailConfigurationDTO alertConfig = emailConfigurationDAO.findById(id);
    if(alertConfig == null) {
      throw new NullArgumentException("Alert config not found");
    }
    alertConfig.setIsActive(state);
    emailConfigurationDAO.update(alertConfig);
  }
}
