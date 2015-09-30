package com.linkedin.thirdeye.resources;

import com.linkedin.thirdeye.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.driver.AnomalyDetectionJobManager;
import io.dropwizard.hibernate.UnitOfWork;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/anomaly-jobs")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyDetectionJobResource {
  private final AnomalyDetectionJobManager manager;
  private final AnomalyFunctionSpecDAO specDAO;

  public AnomalyDetectionJobResource(AnomalyDetectionJobManager manager, AnomalyFunctionSpecDAO specDAO) {
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
  public void enable(@PathParam("id") Long id) throws Exception {
    specDAO.toggleActive(id, true);
    manager.start(id);
  }

  @POST
  @Path("/{id}/ad-hoc")
  @UnitOfWork
  public void adHoc(@PathParam("id") Long id,
                    @QueryParam("start") String start,
                    @QueryParam("end") String end) throws Exception {
    manager.runAdHoc(id, start, end);
  }

  @DELETE
  @Path("/{id}")
  @UnitOfWork
  public void disable(@PathParam("id") Long id) throws Exception {
    specDAO.toggleActive(id, false);
    manager.stop(id);
  }
}
