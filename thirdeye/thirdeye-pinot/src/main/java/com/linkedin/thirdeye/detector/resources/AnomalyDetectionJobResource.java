package com.linkedin.thirdeye.detector.resources;

import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import io.dropwizard.hibernate.UnitOfWork;

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

import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.driver.AnomalyDetectionJobManager;

@Path("/anomaly-jobs")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyDetectionJobResource {
  private final AnomalyDetectionJobManager manager;
  private final AnomalyFunctionDAO specDAO;

  public AnomalyDetectionJobResource(AnomalyDetectionJobManager manager,
      AnomalyFunctionDAO specDAO) {
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
  public Response enable(@PathParam("id") Long id) throws Exception {
    AnomalyFunctionSpec function = specDAO.findById(id);
    function.setIsActive(true);
    specDAO.save(function);
    manager.start(id);
    return Response.ok().build();
  }

  @POST
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
  public Response adHoc(@PathParam("id") Long id, @QueryParam("start") String start,
      @QueryParam("end") String end) throws Exception {
    manager.runAdHoc(id, start, end);
    return Response.ok().build();
  }

  @DELETE
  @Path("/{id}")
  public Response disable(@PathParam("id") Long id) throws Exception {
    AnomalyFunctionSpec function = specDAO.findById(id);
    function.setIsActive(false);
    specDAO.save(function);
    manager.stop(id);
    return Response.ok().build();
  }
}
