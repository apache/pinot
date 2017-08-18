package com.linkedin.thirdeye.dashboard.resources;

import com.google.common.base.Strings;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.alert.util.AnomalyReportGenerator;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datasource.DAORegistry;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;


@Path("thirdeye/email")
@Produces(MediaType.APPLICATION_JSON)
public class EmailResource {

  private final AlertConfigManager alertDAO;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private ThirdEyeConfiguration thirdeyeConfiguration = null;
  private AlertFilterFactory alertFilterFactory;

  public EmailResource(ThirdEyeConfiguration thirdEyeConfiguration) {
    this.alertDAO = DAO_REGISTRY.getAlertConfigDAO();
    this.thirdeyeConfiguration = thirdEyeConfiguration;
    this.alertFilterFactory = new AlertFilterFactory(this.thirdeyeConfiguration.getAlertFilterConfigPath());
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
  @Path("functions")
  public Map<Long, List<AlertConfigDTO>> getAlertToSubscriberMapping() {
    Map<Long, List<AlertConfigDTO>> mapping = new HashMap<>();
    List<AlertConfigDTO> subscriberGroups = alertDAO.findAll();
    for(AlertConfigDTO alertConfigDTO : subscriberGroups) {
      if (null != alertConfigDTO.getEmailConfig()) {
        for (Long alertFunctionId : alertConfigDTO.getEmailConfig().getFunctionIds()) {
          if (!mapping.containsKey(alertFunctionId)) {
            mapping.put(alertFunctionId, new ArrayList<AlertConfigDTO>());
          }
          mapping.get(alertFunctionId).add(alertConfigDTO);
        }
      }
    }
    return mapping;
  }

  /**
   * End point to send anomalies by datasets
   * @param startTime start time to generate anomalies
   * @param endTime end time of generate anomalies
   * @param datasets from which datasets anomalies are from. Multiple datasets can be processed. The input should follow format dataset1,dataset2,...
   * @param fromAddr from which email address. Multiple email addresses can be processed. The input should follow format address1,address2,...
   * @param toAddr to which email address. Multiple email addresses can be processed. The input should follow format address1,address2,...
   * @param subject Title of the report
   * @param includeSentAnomaliesOnly is only include sent anomalies (which notified flag = 1)
   * @param isApplyFilter is apply alert filter or not
   * @param teHost
   * @param smtpHost
   * @param smtpPort
   * @return
   */
  @GET
  @Path("generate/datasets/{startTime}/{endTime}")
  public Response generateAndSendAlertForDatasets(@PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime, @QueryParam("datasets") String datasets,
      @QueryParam("from") String fromAddr, @QueryParam("to") String toAddr,
      @QueryParam("subject") String subject,
      @QueryParam("includeSentAnomaliesOnly") boolean includeSentAnomaliesOnly,
      @QueryParam("isApplyFilter") boolean isApplyFilter,
      @QueryParam("teHost") String teHost, @QueryParam("smtpHost") String smtpHost,
      @QueryParam("smtpPort") Integer smtpPort) {
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

    SmtpConfiguration smtpConfiguration = thirdeyeConfiguration.getSmtpConfiguration();
    if (!Strings.isNullOrEmpty(smtpHost)) {
      smtpConfiguration.setSmtpHost(smtpHost);
    }
    if (smtpPort != null) {
      smtpConfiguration.setSmtpPort(smtpPort);
    }

    if(Strings.isNullOrEmpty(teHost)) {
      teHost = thirdeyeConfiguration.getDashboardHost();
    }
    AnomalyReportGenerator anomalyReportGenerator = AnomalyReportGenerator.getInstance();
    List<MergedAnomalyResultDTO> anomalies = anomalyReportGenerator
        .getAnomaliesForDatasets(Arrays.asList(dataSetArr), startTime, endTime);
    if (isApplyFilter) {
      anomalies = AlertFilterHelper.applyFiltrationRule(anomalies, alertFilterFactory);
    }
    ThirdEyeAnomalyConfiguration configuration = new ThirdEyeAnomalyConfiguration();
    configuration.setSmtpConfiguration(smtpConfiguration);
    configuration.setDashboardHost(teHost);
    configuration.setPhantomJsPath(thirdeyeConfiguration.getPhantomJsPath());
    configuration.setRootDir(thirdeyeConfiguration.getRootDir());
    String emailSub = Strings.isNullOrEmpty(subject) ? "Thirdeye Anomaly Report" : subject;

    anomalyReportGenerator
        .buildReport(startTime, endTime, null, null, anomalies, emailSub, configuration,
            includeSentAnomaliesOnly, toAddr, fromAddr, "Thirdeye Anomaly Report", true);
    return Response.ok().build();
  }


  /**
   * End point to send anomalies by metrics
   * @param startTime start time to generate anomalies
   * @param endTime end time of generate anomalies
   * @param metrics from which datasets anomalies are from. Multiple datasets can be processed. The input should follow format dataset1,dataset2,...
   * @param fromAddr from which email address. Multiple email addresses can be processed. The input should follow format address1,address2,...
   * @param toAddr to which email address. Multiple email addresses can be processed. The input should follow format address1,address2,...
   * @param subject Title of the report
   * @param includeSentAnomaliesOnly is only include sent anomalies (which notified flag = 1)
   * @param isApplyFilter is apply alert filter or not
   * @param teHost
   * @param smtpHost
   * @param smtpPort
   * @return
   */


  @GET
  @Path("generate/metrics/{startTime}/{endTime}")
  public Response generateAndSendAlertForMetrics(
      @PathParam("startTime") Long startTime, @PathParam("endTime") Long endTime,
      @QueryParam("metrics") String metrics, @QueryParam("from") String fromAddr,
      @QueryParam("to") String toAddr,@QueryParam("subject") String subject,
      @QueryParam("includeSentAnomaliesOnly") boolean includeSentAnomaliesOnly,
      @QueryParam("isApplyFilter") boolean isApplyFilter,
      @QueryParam("teHost") String teHost, @QueryParam("smtpHost") String smtpHost,
      @QueryParam("smtpPort") Integer smtpPort,
      @QueryParam("phantomJsPath") String phantomJsPath) {
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
    SmtpConfiguration smtpConfiguration = thirdeyeConfiguration.getSmtpConfiguration();
    if (!Strings.isNullOrEmpty(smtpHost)) {
      smtpConfiguration.setSmtpHost(smtpHost);
    }
    if (smtpPort != null) {
      smtpConfiguration.setSmtpPort(smtpPort);
    }

    if(Strings.isNullOrEmpty(teHost)) {
      teHost = thirdeyeConfiguration.getDashboardHost();
    }
    AnomalyReportGenerator anomalyReportGenerator = AnomalyReportGenerator.getInstance();
    List<MergedAnomalyResultDTO> anomalies = anomalyReportGenerator
        .getAnomaliesForMetrics(Arrays.asList(metricsArr), startTime, endTime);
    if(isApplyFilter){
      anomalies = AlertFilterHelper.applyFiltrationRule(anomalies, alertFilterFactory);
    }
    ThirdEyeAnomalyConfiguration configuration = new ThirdEyeAnomalyConfiguration();
    configuration.setSmtpConfiguration(smtpConfiguration);
    configuration.setDashboardHost(teHost);
    configuration.setPhantomJsPath(phantomJsPath);
    String emailSub = Strings.isNullOrEmpty(subject) ? "Thirdeye Anomaly Report" : subject;
    anomalyReportGenerator
        .buildReport(startTime, endTime, null, null, anomalies, emailSub, configuration,
            includeSentAnomaliesOnly, toAddr, fromAddr, "Thirdeye Anomaly Report", true);
    return Response.ok().build();
  }


  @GET
  @Path("generate/functions/{startTime}/{endTime}")
  public Response generateAndSendAlertForFunctions(
      @PathParam("startTime") Long startTime, @PathParam("endTime") Long endTime,
      @QueryParam("functions") String functions, @QueryParam("from") String fromAddr,
      @QueryParam("to") String toAddr,@QueryParam("subject") String subject,
      @QueryParam("includeSentAnomaliesOnly") boolean includeSentAnomaliesOnly,
      @QueryParam("isApplyFilter") boolean isApplyFilter,
      @QueryParam("teHost") String teHost, @QueryParam("smtpHost") String smtpHost,
      @QueryParam("smtpPort") Integer smtpPort,
      @QueryParam("phantomJsPath") String phantomJsPath) {
    if (Strings.isNullOrEmpty(functions)) {
      throw new WebApplicationException("metrics null or empty: " + functions);
    }
    List<Long> functionList = new ArrayList<>();
    for (String functionId : functions.split(",")) {
      functionList.add(Long.valueOf(functionId));
    }
    if (functionList.size() == 0) {
      throw new WebApplicationException("metrics empty : " + functionList);
    }
    if (Strings.isNullOrEmpty(toAddr)) {
      throw new WebApplicationException("Empty : list of recipients" + toAddr);
    }

    SmtpConfiguration smtpConfiguration = thirdeyeConfiguration.getSmtpConfiguration();

    if (!Strings.isNullOrEmpty(smtpHost)) {
      smtpConfiguration.setSmtpHost(smtpHost);
    }
    if (smtpPort != null) {
      smtpConfiguration.setSmtpPort(smtpPort);
    }

    if(Strings.isNullOrEmpty(teHost)) {
      teHost = thirdeyeConfiguration.getDashboardHost();
    }

    if (Strings.isNullOrEmpty(fromAddr)) {
      fromAddr = thirdeyeConfiguration.getFailureFromAddress();
    }

    AnomalyReportGenerator anomalyReportGenerator = AnomalyReportGenerator.getInstance();
    List<MergedAnomalyResultDTO> anomalies = anomalyReportGenerator
        .getAnomaliesForFunctions(functionList, startTime, endTime);
    if(isApplyFilter){
      anomalies = AlertFilterHelper.applyFiltrationRule(anomalies, alertFilterFactory);
    }
    ThirdEyeAnomalyConfiguration configuration = new ThirdEyeAnomalyConfiguration();
    configuration.setSmtpConfiguration(smtpConfiguration);
    configuration.setDashboardHost(teHost);
    configuration.setPhantomJsPath(phantomJsPath);
    String emailSub = Strings.isNullOrEmpty(subject) ? "Thirdeye Anomaly Report" : subject;
    anomalyReportGenerator
        .buildReport(startTime, endTime, null, null, anomalies, emailSub, configuration,
            includeSentAnomaliesOnly, toAddr, fromAddr, "Thirdeye Anomaly Report", true);
    return Response.ok().build();
  }


  @GET
  @Path("notification/")
  public Response sendEmailWithText(
      @QueryParam("from") String fromAddr,
      @QueryParam("to") String toAddr,
      @QueryParam("subject") String subject,
      @QueryParam("text") String text,
      @QueryParam("smtpHost") String smtpHost,
      @QueryParam("smtpPort") Integer smtpPort
      ){

    if (Strings.isNullOrEmpty(toAddr)) {
      throw new WebApplicationException("Empty : list of recipients" + toAddr);
    }

    if (Strings.isNullOrEmpty(smtpHost)) {
      smtpHost = thirdeyeConfiguration.getSmtpHost();
    }

    SmtpConfiguration smtpConfiguration = thirdeyeConfiguration.getSmtpConfiguration();
    if (smtpPort != null) {
      smtpConfiguration.setSmtpPort(smtpPort);
    }

    if (smtpHost != null) {
      smtpConfiguration.setSmtpHost(smtpHost);
    }

    if (Strings.isNullOrEmpty(fromAddr)) {
      fromAddr = thirdeyeConfiguration.getFailureFromAddress();
    }

    if (Strings.isNullOrEmpty(toAddr)) {
      toAddr = thirdeyeConfiguration.getFailureToAddress();
    }

    HtmlEmail email = new HtmlEmail();

    try {
      EmailHelper.sendEmailWithTextBody(email, smtpConfiguration, subject, text,
         fromAddr, toAddr
      );
    } catch (EmailException e) {
      return Response.ok("Exception in sending out message").build();
    }
    return Response.ok().build();
  }

}
