package com.linkedin.thirdeye.detector.resources;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.linkedin.thirdeye.detector.api.EmailFunctionDependency;
import com.linkedin.thirdeye.detector.db.EmailFunctionDependencyDAO;

import io.dropwizard.hibernate.UnitOfWork;

@Path("/email-function-dependencies")
@Produces(MediaType.APPLICATION_JSON)
public class EmailFunctionDependencyResource {
  private final EmailFunctionDependencyDAO dao;

  public EmailFunctionDependencyResource(EmailFunctionDependencyDAO dao) {
    this.dao = dao;
  }

  @POST
  @UnitOfWork
  public Response create(EmailFunctionDependency relation) {
    dao.create(relation);
    return Response.ok().build();
  }

  @POST
  @UnitOfWork
  @Path("/{emailId}/{functionId}")
  public Response create(@PathParam("emailId") Long emailId,
      @PathParam("functionId") Long functionId) {
    EmailFunctionDependency relation = new EmailFunctionDependency();
    relation.setEmailId(emailId);
    relation.setFunctionId(functionId);
    return create(relation);
  }

  @DELETE
  @UnitOfWork
  @Path("/{emailId}/{functionId}")
  public Response delete(@PathParam("emailId") Long emailId,
      @PathParam("functionId") Long functionId) {
    dao.delete(emailId, functionId);
    return Response.noContent().build();
  }

  @DELETE
  @UnitOfWork
  @Path("/email/{emailId}")
  public Response deleteByEmail(@PathParam("emailId") Long emailId) {
    dao.deleteByEmail(emailId);
    return Response.noContent().build();
  }

  @DELETE
  @UnitOfWork
  @Path("/function/{functionId}")
  public Response deleteByFunction(@PathParam("functionId") Long functionId) {
    dao.deleteByFunction(functionId);
    return Response.noContent().build();
  }

  @GET
  @UnitOfWork
  public List<EmailFunctionDependency> find() {
    return dao.find();
  }

  @GET
  @UnitOfWork
  @Path("/email/{emailId}")
  public List<Long> findByEmail(@PathParam("emailId") Long emailId) {
    List<EmailFunctionDependency> result = dao.findByEmail(emailId);
    List<Long> functionIds = new ArrayList<>();
    for (EmailFunctionDependency relation : result) {
      functionIds.add(relation.getFunctionId());
    }
    return functionIds;
  }

  @GET
  @UnitOfWork
  @Path("/function/{functionId}")
  public List<Long> findByFunction(@PathParam("functionId") Long functionId) {
    List<EmailFunctionDependency> result = dao.findByFunction(functionId);
    List<Long> emailIds = new ArrayList<>();
    for (EmailFunctionDependency relation : result) {
      emailIds.add(relation.getEmailId());
    }
    return emailIds;
  }

}
