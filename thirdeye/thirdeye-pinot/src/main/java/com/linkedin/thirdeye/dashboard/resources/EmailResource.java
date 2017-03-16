package com.linkedin.thirdeye.dashboard.resources;

import com.google.common.base.Strings;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.util.AnomalyReportGenerator;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.linkedin.thirdeye.client.DAORegistry;
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
  private final AlertConfigManager alertDAO;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public EmailResource() {
    this.functionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.emailDAO = DAO_REGISTRY.getEmailConfigurationDAO();
    this.alertDAO = DAO_REGISTRY.getAlertConfigDAO();
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


  @POST
  @Path("alert")
  public Response createAlertConfig(AlertConfigDTO alertConfigDTO) {
    Long id = alertDAO.save(alertConfigDTO);
    return Response.ok(id).build();
  }

  @GET
  @Path("alert/{id}")
  public AlertConfigDTO getAlertConfigById (@PathParam("id") Long id) {
    return alertDAO.findById(id);
  }

  @DELETE
  @Path("alert/{alertId}")
  public Response deleteByAlertId(@PathParam("alertId") Long alertId) {
    alertDAO.deleteById(alertId);
    return Response.ok().build();
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

  // TODO : add end points for AlertConfig


  @GET
  @Path("generate/datasets/{startTime}/{endTime}")
  public Response generateAndSendAlertForDatasets(@PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime, @QueryParam("datasets") String datasets,
      @QueryParam("from") String fromAddr, @QueryParam("to") String toAddr,
      @QueryParam("subject") String subject,
      @QueryParam("includeSentAnomaliesOnly") boolean includeSentAnomaliesOnly,
      @QueryParam("teHost") String teHost, @QueryParam("smtpHost") String smtpHost,
      @QueryParam("smtpPort") int smtpPort,
      @QueryParam("phantomJsPath") String phantomJsPath) {
    if (Strings.isNullOrEmpty(datasets)) {
      throw new WebApplicationException("datasets null or empty : " + datasets);
    }
    String [] dataSetArr = datasets.split(",");
    if (dataSetArr.length == 0) {
      throw new WebApplicationException("Datasets empty : " + datasets);
    }
    if (Strings.isNullOrEmpty(toAddr)) {
      throw new WebApplicationException("Empty : list of recipients" + toAddr);
    }
    if(Strings.isNullOrEmpty(teHost)) {
      throw new WebApplicationException("Invalid TE host" + teHost);
    }
    if (Strings.isNullOrEmpty(smtpHost)) {
      throw new WebApplicationException("invalid smtp host" + smtpHost);
    }
    AnomalyReportGenerator anomalyReportGenerator = AnomalyReportGenerator.getInstance();
    List<MergedAnomalyResultDTO> anomalies = anomalyReportGenerator
        .getAnomaliesForDatasets(Arrays.asList(dataSetArr), startTime, endTime);
    ThirdEyeAnomalyConfiguration configuration = new ThirdEyeAnomalyConfiguration();
    SmtpConfiguration smtpConfiguration = new SmtpConfiguration();
    smtpConfiguration.setSmtpHost(smtpHost);
    smtpConfiguration.setSmtpPort(smtpPort);

    configuration.setSmtpConfiguration(smtpConfiguration);
    configuration.setDashboardHost(teHost);
    configuration.setPhantomJsPath(phantomJsPath);
    String emailSub = Strings.isNullOrEmpty(subject) ? "Thirdeye Anomaly Report" : subject;

    anomalyReportGenerator
        .buildReport(startTime, endTime, anomalies, emailSub, configuration,
            includeSentAnomaliesOnly, toAddr, fromAddr, "UI-Generated", true);
    return Response.ok().build();
  }

  @GET
  @Path("generate/metrics/{startTime}/{endTime}")
  public Response generateAndSendAlertForMetrics(
      @PathParam("startTime") Long startTime, @PathParam("endTime") Long endTime,
      @QueryParam("metrics") String metrics, @QueryParam("from") String fromAddr,
      @QueryParam("to") String toAddr,@QueryParam("subject") String subject,
      @QueryParam("includeSentAnomaliesOnly") boolean includeSentAnomaliesOnly,
      @QueryParam("teHost") String teHost, @QueryParam("smtpHost") String smtpHost,
      @QueryParam("smtpPort") int smtpPort) {
    if (Strings.isNullOrEmpty(metrics)) {
      throw new WebApplicationException("metrics null or empty: " + metrics);
    }
    String [] metricsArr = metrics.split(",");
    if (metricsArr.length == 0) {
      throw new WebApplicationException("metrics empty : " + metricsArr);
    }
    if (Strings.isNullOrEmpty(toAddr)) {
      throw new WebApplicationException("Empty : list of recipients" + toAddr);
    }
    if(Strings.isNullOrEmpty(teHost)) {
      throw new WebApplicationException("Invalid TE host" + teHost);
    }
    if (Strings.isNullOrEmpty(smtpHost)) {
      throw new WebApplicationException("invalid smtp host" + smtpHost);
    }
    AnomalyReportGenerator anomalyReportGenerator = AnomalyReportGenerator.getInstance();
    List<MergedAnomalyResultDTO> anomalies = anomalyReportGenerator
        .getAnomaliesForMetrics(Arrays.asList(metricsArr), startTime, endTime);
    ThirdEyeAnomalyConfiguration configuration = new ThirdEyeAnomalyConfiguration();
    SmtpConfiguration smtpConfiguration = new SmtpConfiguration();
    smtpConfiguration.setSmtpHost(smtpHost);
    smtpConfiguration.setSmtpPort(smtpPort);

    configuration.setSmtpConfiguration(smtpConfiguration);
    configuration.setDashboardHost(teHost);
    String emailSub = Strings.isNullOrEmpty(subject) ? "Thirdeye Anomaly Report" : subject;
    anomalyReportGenerator
        .buildReport(startTime, endTime, anomalies, emailSub, configuration,
            includeSentAnomaliesOnly, toAddr, fromAddr, "UI-Generated", true);
    return Response.ok().build();
  }
}
