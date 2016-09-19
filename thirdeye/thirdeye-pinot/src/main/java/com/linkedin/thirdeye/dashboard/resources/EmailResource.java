package com.linkedin.thirdeye.dashboard.resources;

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

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import org.apache.commons.lang3.StringUtils;

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
    List<EmailConfigurationDTO> emails = emailDAO.findByCollectionMetric(emailConfiguration.getCollection(), emailConfiguration.getMetric());
    if (emails.size() > 0) {
      EmailConfigurationDTO base = emails.get(0);
      base.setActive(true);
      base.setToAddresses(emailConfiguration.getToAddresses());
      emailDAO.update(base);
      return Response.ok(base.getId()).build();
    }
    Long id = emailDAO.save(emailConfiguration);
    return Response.ok(id).build();
  }

  @GET
  @Path("{id}")
  public EmailConfigurationDTO getEmailConfigById (@PathParam("id") Long id) {
    return emailDAO.findById(id);
  }

  @GET
  public List<EmailConfigurationDTO> getEmailConfigurations(
      @QueryParam("collection") String collection, @QueryParam("metric") String metric) {
    if (StringUtils.isNotEmpty(collection) && StringUtils.isNotEmpty(metric)) {
      return emailDAO.findByCollectionMetric(collection, metric);
    }
    if (StringUtils.isNotEmpty(collection)) {
      return emailDAO.findByCollection(collection);
    }
    return emailDAO.findAll();
  }

  @POST
  @Path("{emailId}/add/{functionId}")
  public void addFunctionInEmail(@PathParam("emailId") Long emailId, @PathParam("functionId") Long functionId) {
    AnomalyFunctionDTO function = functionDAO.findById(functionId);
    EmailConfigurationDTO emailConfiguration = emailDAO.findById(emailId);
    List<EmailConfigurationDTO> emailConfigurationsWithFunction = emailDAO.findByFunctionId(functionId);

    for (EmailConfigurationDTO emailConfigurationDTO : emailConfigurationsWithFunction) {
      emailConfigurationDTO.getFunctions().remove(function);
      emailDAO.update(emailConfigurationDTO);
    }

    if (function != null && emailConfiguration != null) {
      if (!emailConfiguration.getFunctions().contains(function)) {
        emailConfiguration.getFunctions().add(function);
        emailDAO.update(emailConfiguration);
      }
    } else {
      throw new IllegalArgumentException(
          "function or email not found for email : " + emailId + " function : " + functionId);
    }
  }

  @POST
  @Path("{emailId}/delete/{functionId}")
  public void removeFunctionFromEmail(@PathParam("emailId") Long emailId,
      @PathParam("functionId") Long functionId) {
    AnomalyFunctionDTO function = functionDAO.findById(functionId);
    EmailConfigurationDTO emailConfiguration = emailDAO.findById(emailId);
    if (function != null && emailConfiguration != null) {
      if (emailConfiguration.getFunctions().contains(function)) {
        emailConfiguration.getFunctions().remove(function);
        emailDAO.update(emailConfiguration);
      }
    }
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
