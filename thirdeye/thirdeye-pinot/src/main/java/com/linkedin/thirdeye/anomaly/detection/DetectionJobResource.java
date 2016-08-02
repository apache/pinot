package com.linkedin.thirdeye.anomaly.detection;

import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;

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

@Path("/detection-job")
@Produces(MediaType.APPLICATION_JSON)
public class DetectionJobResource {
  private final DetectionJobScheduler detectionJobScheduler;
  private final AnomalyFunctionDAO anomalyFunctionSpecDAO;

  public DetectionJobResource(DetectionJobScheduler detectionJobScheduler,
      AnomalyFunctionDAO anomalyFunctionSpecDAO) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.anomalyFunctionSpecDAO = anomalyFunctionSpecDAO;
  }

  @GET
  public List<String> showActiveJobs() throws SchedulerException {
    return detectionJobScheduler.getActiveJobs();
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
  public Response adHoc(@PathParam("id") Long id, @QueryParam("start") String start,
      @QueryParam("end") String end) throws Exception {
    detectionJobScheduler.runAdHoc(id, start, end);
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
    AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if(anomalyFunctionSpec == null) {
      throw new NullArgumentException("Function spec not found");
    }
    anomalyFunctionSpec.setIsActive(state);
    anomalyFunctionSpecDAO.save(anomalyFunctionSpec);
  }
}
