package com.linkedin.thirdeye.detector.resources;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.linkedin.thirdeye.detector.db.entity.AnomalyFunctionRelation;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;

import io.dropwizard.hibernate.UnitOfWork;

@Path("/anomaly-function-relations")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyFunctionRelationResource {
  private final AnomalyFunctionRelationDAO dao;

  public AnomalyFunctionRelationResource(AnomalyFunctionRelationDAO dao) {
    this.dao = dao;
  }

  @POST
  @UnitOfWork
  public Response create(AnomalyFunctionRelation relation) {
    dao.create(relation);
    return Response.ok().build();
  }

  @POST
  @UnitOfWork
  @Path("/{parentId}/{childId}")
  public Response create(@PathParam("parentId") Long parentId, @PathParam("childId") Long childId) {
    AnomalyFunctionRelation relation = new AnomalyFunctionRelation();
    relation.setParentId(parentId);
    relation.setChildId(childId);
    dao.create(relation);
    return Response.ok().build();
  }

  @DELETE
  @UnitOfWork
  @Path("/{parentId}")
  public Response deleteByParent(@PathParam("parentId") Long parentId) {
    dao.delete(parentId);
    return Response.noContent().build();
  }

  @DELETE
  @UnitOfWork
  @Path("/{parentId}/{childId}")
  public Response deleteByParent(@PathParam("parentId") Long parentId,
      @PathParam("childId") Long childId) {
    dao.delete(parentId, childId);
    return Response.noContent().build();
  }

  @GET
  @UnitOfWork
  public List<AnomalyFunctionRelation> find() {
    return dao.find();
  }

  @GET
  @UnitOfWork
  @Path("/{parentId}")
  public List<Long> findByParent(@PathParam("parentId") Long parentId) {
    List<AnomalyFunctionRelation> result = dao.findByParent(parentId);
    if (result.isEmpty()) {
      throw new NotFoundException();
    }

    List<Long> childIds = new ArrayList<>();
    for (AnomalyFunctionRelation relation : result) {
      childIds.add(relation.getChildId());
    }

    return childIds;
  }
}
