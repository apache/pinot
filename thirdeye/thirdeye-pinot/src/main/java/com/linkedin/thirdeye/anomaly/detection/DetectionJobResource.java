package com.linkedin.thirdeye.anomaly.detection;

import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

@Path("/detection-job")
@Produces(MediaType.APPLICATION_JSON)
public class DetectionJobResource {
  private final DetectionJobScheduler detectionJobScheduler;
  private final AnomalyFunctionManager anomalyFunctionSpecDAO;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public DetectionJobResource(DetectionJobScheduler detectionJobScheduler) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.anomalyFunctionSpecDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
  }

  @GET
  public List<String> showScheduledJobs() throws SchedulerException {
    return detectionJobScheduler.getScheduledJobs();
  }

  @POST
  @Path("/{id}")
  public Response enable(@PathParam("id") Long id) throws Exception {
    toggleActive(id, true);
    detectionJobScheduler.startJob(id);
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
    detectionJobScheduler.runAdHoc(id, startTime, endTime);
    return Response.ok().build();
  }

  /**
   * Asynchronous call to a backfill procedure
   * @param id
   * @param startTimeIso
   * @param endTimeIso
   * @return
   * @throws Exception
   */
  @POST
  @Path("/{id}/backfill")
  public Response backfill(@PathParam("id") Long id, @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso, @DefaultValue("false") @QueryParam("force") boolean force) throws Exception {
    DateTime startTime = null;
    DateTime endTime = null;
    if (StringUtils.isNotBlank(startTimeIso)) {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    }
    if (StringUtils.isNotBlank(endTimeIso)) {
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
    }

    if (startTime != null && endTime != null) {
      DateTime innerStartTime = startTime;
      DateTime innerEndTime = endTime;

      new Thread(new Runnable() {
        @Override
        public void run() {
          detectionJobScheduler.runBackfill(id, innerStartTime, innerEndTime, force);
        }
      }).start();
    }

    return Response.ok().build();
  }

  @DELETE
  @Path("/{id}")
  public Response disable(@PathParam("id") Long id) throws Exception {
    toggleActive(id, false);
    detectionJobScheduler.stopJob(id);
    return Response.ok().build();
  }

  private void toggleActive(Long id, boolean state) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if(anomalyFunctionSpec == null) {
      throw new NullArgumentException("Function spec not found");
    }
    anomalyFunctionSpec.setIsActive(state);
    anomalyFunctionSpecDAO.update(anomalyFunctionSpec);
  }

  @POST
  @Path("/{id}/restart")
  public Response restart(@PathParam("id") Long id) throws Exception {
    detectionJobScheduler.stopJob(id);
    detectionJobScheduler.startJob(id);
    return Response.ok().build();
  }
}
