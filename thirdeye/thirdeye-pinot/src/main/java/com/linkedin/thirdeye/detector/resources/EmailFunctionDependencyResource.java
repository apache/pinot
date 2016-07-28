package com.linkedin.thirdeye.detector.resources;

import com.linkedin.thirdeye.api.dto.EmailFunctionDependency;
import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.dao.EmailConfigurationDAO;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
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

@Path("/email-function-dependencies")
@Produces(MediaType.APPLICATION_JSON)
public class EmailFunctionDependencyResource {
  private final AnomalyFunctionDAO functionDAO;
  private final EmailConfigurationDAO emailDAO;

  public EmailFunctionDependencyResource(AnomalyFunctionDAO functionDAO,
      EmailConfigurationDAO emailConfigurationDAO) {
    this.functionDAO = functionDAO;
    this.emailDAO = emailConfigurationDAO;
  }

  @POST
  public Response create(EmailFunctionDependency relation) {
    AnomalyFunctionSpec function = functionDAO.findById(relation.getFunctionId());
    EmailConfiguration emailConfiguration = emailDAO.findById(relation.getEmailId());
    if (function != null && emailConfiguration != null) {
      if (!emailConfiguration.getFunctions().contains(function)) {
        emailConfiguration.getFunctions().add(function);
        emailDAO.save(emailConfiguration);
      }
    } else {
      throw new IllegalArgumentException(
          "function or email not found for input : " + relation.toString());
    }
    return Response.ok().build();
  }

  @POST
  @Path("/{emailId}/{functionId}")
  public Response create(@PathParam("emailId") Long emailId,
      @PathParam("functionId") Long functionId) {
    EmailFunctionDependency relation = new EmailFunctionDependency();
    relation.setEmailId(emailId);
    relation.setFunctionId(functionId);
    return create(relation);
  }

  /**
   * Removing email from function
   * @param emailId
   * @param functionId
   * @return
   */
  @DELETE
  @Path("/{emailId}/{functionId}")
  public Response delete(@PathParam("emailId") Long emailId,
      @PathParam("functionId") Long functionId) {
    AnomalyFunctionSpec function = functionDAO.findById(functionId);
    EmailConfiguration emailConfiguration = emailDAO.findById(emailId);
    if (function != null && emailConfiguration != null) {
      if (!emailConfiguration.getFunctions().contains(function)) {
        emailConfiguration.getFunctions().remove(function);
        emailDAO.save(emailConfiguration);
      }
    }
    return Response.noContent().build();
  }

  @DELETE
  @Path("/email/{emailId}")
  public Response deleteByEmail(@PathParam("emailId") Long emailId) {
    emailDAO.deleteById(emailId);
    return Response.noContent().build();
  }

  @DELETE
  @Path("/function/{functionId}")
  public Response deleteByFunction(@PathParam("functionId") Long functionId) {
    functionDAO.deleteById(functionId);
    return Response.noContent().build();
  }

  @GET
  @Deprecated
  /**
   * Deprecate use of the end point.
   */
  public List<EmailFunctionDependency> find() {
    List<EmailConfiguration> emailConfigurations = emailDAO.findAll();
    List<EmailFunctionDependency> dependencies = new ArrayList<>();
    for (EmailConfiguration emailConfiguration : emailConfigurations) {
      for (AnomalyFunctionSpec spec : emailConfiguration.getFunctions()) {
        EmailFunctionDependency dependency = new EmailFunctionDependency();
        dependency.setEmailId(emailConfiguration.getId());
        dependency.setFunctionId(spec.getId());
        dependencies.add(dependency);
      }
    }
    return dependencies;
  }

  @GET
  @Path("/email/{emailId}")
  public List<Long> findFunctionIdsByEmail(@PathParam("emailId") Long emailId) {
    EmailConfiguration emailConfiguration = emailDAO.findById(emailId);
    List<AnomalyFunctionSpec> functionSpecs = emailConfiguration.getFunctions();
    List<Long> functionIds = new ArrayList<>();
    for (AnomalyFunctionSpec functionSpec : functionSpecs) {
      functionIds.add(functionSpec.getId());
    }
    return functionIds;
  }

  @GET
  @Path("/function/{functionId}")
  public List<Long> findEmailIdsByFunction(@PathParam("functionId") Long functionId) {
    List<EmailConfiguration> emailConfigurations = emailDAO.findByFunctionId(functionId);
    List<Long> emailIds = new ArrayList<>();
    for (EmailConfiguration emailConfiguration : emailConfigurations) {

      emailIds.add(emailConfiguration.getId());
    }
    return emailIds;
  }

}
