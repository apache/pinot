package com.linkedin.thirdeye.detector.resources;

import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.driver.AnomalyDetectionJobManager;

import io.dropwizard.hibernate.UnitOfWork;

@Path("/anomaly-jobs")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyDetectionJobResource {
  private final AnomalyDetectionJobManager manager;
  private final AnomalyFunctionSpecDAO specDAO;

  public AnomalyDetectionJobResource(AnomalyDetectionJobManager manager,
      AnomalyFunctionSpecDAO specDAO) {
    this.manager = manager;
    this.specDAO = specDAO;
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
    specDAO.toggleActive(id, true);
    manager.start(id);
    return Response.ok().build();
  }

  @POST
  @UnitOfWork
  @Path("/from-file")
  public Response testFile(AnomalyFunctionSpec spec, @QueryParam("id") Long existingFunctionId,
      @QueryParam("start") String start, @QueryParam("end") String end,
      @QueryParam("name") String name) throws Exception {
    if (existingFunctionId == null || specDAO.findById(existingFunctionId) == null) {
      throw new NotFoundException();
    }
    spec.setId(existingFunctionId);
    manager.runAdhocConfig(spec, start, end, name);
    return Response.ok(name).build();
  }

  @POST
  @Path("/{id}/ad-hoc")
  @UnitOfWork
  public Response adHoc(@PathParam("id") Long id, @QueryParam("start") String start,
      @QueryParam("end") String end) throws Exception {
    manager.runAdHoc(id, start, end);
    return Response.ok().build();
  }

  @POST
  @Path("/{id}/simulate-period")
  @UnitOfWork
  public Response simulatePeriod(@PathParam("id") Long id, @QueryParam("start") String start,
      @QueryParam("end") String end) throws Exception {
    manager.simulatePeriod(id, start, end);
    return Response.ok().build();
  }

  @DELETE
  @Path("/{id}")
  @UnitOfWork
  public Response disable(@PathParam("id") Long id) throws Exception {
    specDAO.toggleActive(id, false);
    manager.stop(id);
    return Response.ok().build();
  }
}
