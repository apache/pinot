package com.linkedin.thirdeye.anomaly.detection;

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

import org.quartz.SchedulerException;

import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;

@Path("/detection-job")
@Produces(MediaType.APPLICATION_JSON)
public class DetectionJobResource {
  private final DetectionJobScheduler detectionJobScheduler;
  private final AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;

  public DetectionJobResource(DetectionJobScheduler detectionJobScheduler,
      AnomalyFunctionSpecDAO anomalyFunctionSpecDAO) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.anomalyFunctionSpecDAO = anomalyFunctionSpecDAO;
  }

  @GET
  @UnitOfWork
  public List<String> showActiveJobs() throws SchedulerException {
    return detectionJobScheduler.getActiveJobs();
  }

  @POST
  @Path("/{id}")
  @UnitOfWork
  public Response enable(@PathParam("id") Long id) throws Exception {
    anomalyFunctionSpecDAO.toggleActive(id, true);
    detectionJobScheduler.start(id);
    return Response.ok().build();
  }


  @POST
  @Path("/{id}/ad-hoc")
  @UnitOfWork
  public Response adHoc(@PathParam("id") Long id, @QueryParam("start") String start,
      @QueryParam("end") String end) throws Exception {
    detectionJobScheduler.runAdHoc(id, start, end);
    return Response.ok().build();
  }

  @DELETE
  @Path("/{id}")
  @UnitOfWork
  public Response disable(@PathParam("id") Long id) throws Exception {
    anomalyFunctionSpecDAO.toggleActive(id, false);
    detectionJobScheduler.stop(id);
    return Response.ok().build();
  }
}
