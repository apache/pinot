package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("thirdeye/email")
@Produces(MediaType.APPLICATION_JSON)
public class EmailResource {

  private final AnomalyFunctionManager functionDAO;
  private final EmailConfigurationManager emailDAO;

  public EmailResource(AnomalyFunctionManager functionDAO,
      EmailConfigurationManager emailConfigurationDAO) {
    this.functionDAO = functionDAO;
    this.emailDAO = emailConfigurationDAO;
  }

  @POST
  public Response createEmailConfig(EmailConfigurationDTO emailConfiguration) {
    Long id = emailDAO.save(emailConfiguration);
    return Response.ok(id).build();
  }

  @GET
  @Path("{id}")
  public EmailConfigurationDTO getEmailConfigById (@PathParam("id") Long id) {
    return emailDAO.findById(id);
  }

  @GET
  public List<EmailConfigurationDTO> getAllEmailconfigurations() {
    return emailDAO.findAll();
  }

  @POST
  @Path("{emailId}/{functionId}")
  public Response addFunctionInEmail(@PathParam("emailId") Long emailId, @PathParam("functionId") Long functionId) {
    AnomalyFunctionDTO function = functionDAO.findById(functionId);
    EmailConfigurationDTO emailConfiguration = emailDAO.findById(emailId);
    if (function != null && emailConfiguration != null) {
      if (!emailConfiguration.getFunctions().contains(function)) {
        emailConfiguration.getFunctions().add(function);
        emailDAO.save(emailConfiguration);
      }
    } else {
      throw new IllegalArgumentException(
          "function or email not found for email : " + emailId + " function : " + functionId);
    }
    return Response.ok().build();
  }

  @DELETE
  @Path("{emailId}/{functionId}")
  public Response removeFunctionFromEmail(@PathParam("emailId") Long emailId,
      @PathParam("functionId") Long functionId) {
    AnomalyFunctionDTO function = functionDAO.findById(functionId);
    EmailConfigurationDTO emailConfiguration = emailDAO.findById(emailId);
    if (function != null && emailConfiguration != null) {
      if (emailConfiguration.getFunctions().contains(function)) {
        emailConfiguration.getFunctions().remove(function);
        emailDAO.save(emailConfiguration);
      }
    }
    return Response.ok().build();
  }

  @DELETE
  @Path("{emailId}")
  public Response deleteByEmail(@PathParam("emailId") Long emailId) {
    emailDAO.deleteById(emailId);
    return Response.ok().build();
  }

  @GET
  @Path("email-config/{functionId}")
  public List<EmailConfigurationDTO> findEmailIdsByFunction(@PathParam("functionId") Long functionId) {
    return emailDAO.findByFunctionId(functionId);
  }
}
