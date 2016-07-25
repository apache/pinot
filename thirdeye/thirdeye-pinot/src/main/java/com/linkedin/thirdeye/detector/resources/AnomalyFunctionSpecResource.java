package com.linkedin.thirdeye.detector.resources;

import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;

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

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;

@Path("/anomaly-functions")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyFunctionSpecResource {
  private final AnomalyFunctionDAO dao;

  public AnomalyFunctionSpecResource(AnomalyFunctionDAO dao) {
    this.dao = dao;
  }

  @POST
  @Timed
  public Response create(AnomalyFunctionSpec anomalyFunctionSpec) {
    Long id = dao.save(anomalyFunctionSpec);
    return Response.ok(id).build();
  }

  @DELETE
  @Timed
  @Path("/{id}")
  public Response delete(@PathParam("id") Long id) {
    dao.deleteById(id);
    return Response.noContent().build();
  }

  @GET
  @Timed
  @Path("/{id}")
  public AnomalyFunctionSpec find(@PathParam("id") Long id) {
    AnomalyFunctionSpec anomalyFunctionSpec = dao.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new NotFoundException();
    }
    return anomalyFunctionSpec;
  }

  @GET
  @Timed
  public List<AnomalyFunctionSpec> findAll(@QueryParam("collection") String collection) {
    if (collection == null) {
      return dao.findAll();
    } else {
      return dao.findAllByCollection(collection);
    }
  }
}
