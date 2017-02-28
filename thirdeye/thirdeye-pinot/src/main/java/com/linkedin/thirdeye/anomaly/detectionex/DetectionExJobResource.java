package com.linkedin.thirdeye.anomaly.detectionex;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/detection-ex-job")
@Produces(MediaType.APPLICATION_JSON)
public class DetectionExJobResource {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionExJobResource.class);

  private final DetectionExJobScheduler detectionJobScheduler;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public DetectionExJobResource(DetectionExJobScheduler detectionJobScheduler) {
    this.detectionJobScheduler = detectionJobScheduler;
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
    detectionJobScheduler.runBackfill(id, startTime, endTime);
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
    AnomalyFunctionExDTO dto = DAO_REGISTRY.getAnomalyFunctionExDAO().findById(id);
    if(dto == null) {
      throw new NullArgumentException("Function spec not found");
    }
    dto.setActive(state);
    DAO_REGISTRY.getAnomalyFunctionExDAO().update(dto);
  }

  @POST
  @Path("/{id}/restart")
  public Response restart(@PathParam("id") Long id) throws Exception {
    detectionJobScheduler.stopJob(id);
    detectionJobScheduler.startJob(id);
    return Response.ok().build();
  }
}
