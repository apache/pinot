/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.dashboard.resources;

import com.google.common.base.Strings;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.alert.util.AnomalyReportGenerator;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.anomaly.utils.EmailUtils;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.ApplicationManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.ApplicationDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.wordnik.swagger.annotations.ApiOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

import static com.linkedin.thirdeye.anomaly.SmtpConfiguration.*;


@Path("thirdeye/email")
@Produces(MediaType.APPLICATION_JSON)
public class EmailResource {
  private static final Logger LOG = LoggerFactory.getLogger(EmailResource.class);

  private final AlertConfigManager alertDAO;
  private final AnomalyFunctionManager anomalyDAO;
  private final ApplicationManager appDAO;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private AlertFilterFactory alertFilterFactory;
  private String failureToAddress;
  private String failureFromAddress;
  private String phantonJsPath;
  private String rootDir;
  private String dashboardHost;
  private SmtpConfiguration smtpConfiguration;

  public EmailResource(ThirdEyeConfiguration thirdEyeConfig) {
    this(SmtpConfiguration.createFromProperties(thirdEyeConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY)),
        new AlertFilterFactory(thirdEyeConfig.getAlertFilterConfigPath()),
        thirdEyeConfig.getFailureFromAddress(), thirdEyeConfig.getFailureToAddress(), thirdEyeConfig.getDashboardHost(),
        thirdEyeConfig.getPhantomJsPath(), thirdEyeConfig.getRootDir());
  }

  public EmailResource(SmtpConfiguration smtpConfiguration, AlertFilterFactory alertFilterFactory,
      String failureFromAddress, String failureToAddress,
      String dashboardHost, String phantonJsPath, String rootDir) {
    this.smtpConfiguration = smtpConfiguration;
    this.alertDAO = DAO_REGISTRY.getAlertConfigDAO();
    this.anomalyDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.appDAO = DAO_REGISTRY.getApplicationDAO();
    this.alertFilterFactory = alertFilterFactory;
    this.failureFromAddress = failureFromAddress;
    this.failureToAddress = failureToAddress;
    this.dashboardHost = dashboardHost;
    this.phantonJsPath = phantonJsPath;
    this.rootDir = rootDir;
  }

  @DELETE
  @Path("alert/{alertId}")
  public Response deleteByAlertId(@PathParam("alertId") Long alertId) {
    alertDAO.deleteById(alertId);
    return Response.ok().build();
  }

  /**
   * update alert config's cron and activation by id
   * @param id alert config id
   * @param cron cron expression for alert
   * @param isActive activate or not
   * @return Response
   * @throws Exception
   */
  @PUT
  @Path("/alert/{id}")
  public Response updateAlertConfig(@NotNull @PathParam("id") Long id,
      @QueryParam("cron") String cron, @QueryParam("isActive") Boolean isActive) throws Exception {

    AlertConfigDTO alert = alertDAO.findById(id);
    if (alert == null) {
      throw new IllegalStateException("Alert Config with id " + id + " does not exist");
    }

    if (isActive != null) {
      alert.setActive(isActive);
    }

    if (StringUtils.isNotEmpty(cron)) {
      // validate cron
      if (!CronExpression.isValidExpression(cron)) {
        throw new IllegalArgumentException("Invalid cron expression for cron : " + cron);
      }
      alert.setCronExpression(cron);
    }

    alertDAO.update(alert);
    return Response.ok(id).build();
  }

  /**
   * update alert config's holiday cron and activation by id
   * if this cron is not null then holiday mode is activate
   * @param id id of the config
   * @param cron holiday cron expression
   * @return Response
   * @throws Exception
   */
  @PUT
  @Path("/alert/{id}/holiday-mode")
  public Response updateAlertConfigHolidayCron(@NotNull @PathParam("id") Long id,
      @QueryParam("cron") String cron) throws Exception {

    AlertConfigDTO alert = alertDAO.findById(id);
    if (alert == null) {
      throw new IllegalStateException("Alert Config with id " + id + " does not exist");
    }

    if (StringUtils.isNotEmpty(cron)) {
      // validate cron
      if (!CronExpression.isValidExpression(cron)) {
        throw new IllegalArgumentException("Invalid cron expression for cron : " + cron);
      }
      // as long as there is an valid holiday cron expression within the class
      // the holiday model is activate
      alert.setHolidayCronExpression(cron);
    } else {
      alert.setHolidayCronExpression(null);  // equivalent to deactivate holiday
    }

    alertDAO.update(alert);
    return Response.ok(id).build();
  }


  public Map<Long, List<AlertConfigDTO>> getAlertToSubscriberMapping() {
    // TODO：Clean deprecated Endpoint called by our own code
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

  @GET
  @Path("function/{id}")
  public List<AlertConfigDTO> getSubscriberList(@PathParam("id") Long alertFunctionId) {
    return getAlertToSubscriberMapping().get(alertFunctionId);
  }

  @POST
  @Path("{id}/subscribe")
  @ApiOperation("Endpoint for subscribing to an alert/alerts")
  public Response subscribeAlert(
      @PathParam("id") Long id,
      @QueryParam("functionId") String functionIds,
      @QueryParam("functionName") String functionName,
      @QueryParam("metric") String metric,
      @QueryParam("dataset") String dataset,
      @DefaultValue("") @QueryParam("functionPrefix") String functionPrefix) throws Exception {
    if (StringUtils.isBlank(functionIds) && StringUtils.isBlank(functionName)
        && (StringUtils.isBlank(metric) || StringUtils.isBlank(dataset))) {
      throw new IllegalArgumentException(String.format("Received null or emtpy String for the mandatory params. Please"
          + " specify either functionIds or functionName or (metric & dataset). dataset: %s, metric: %s,"
          + " functionName %s, functionIds: %s", dataset, metric, functionName, functionIds));
    }

    Map<String, String> responseMessage = new HashMap<>();

    AlertConfigDTO alertConfigDTO = alertDAO.findById(id);
    if (alertConfigDTO == null) {
      responseMessage.put("message", "cannot find the alert group configuration entry " + id + ".");
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    }

    final List<Long> functionIdList = new ArrayList<>();
    if (StringUtils.isNotBlank(functionIds)) {
      for (String functionId : functionIds.split(",")) {
        AnomalyFunctionDTO anomalyFunction = anomalyDAO.findById(Long.valueOf(functionId));
        if (anomalyFunction != null) {
          functionIdList.add(Long.valueOf(functionId));
        } else {
          responseMessage.put("skipped function " + functionId, "Cannot be found!");
        }
      }
    } else if (StringUtils.isNotBlank(functionName)) {
      AnomalyFunctionDTO anomalyFunctionDTO = anomalyDAO.findWhereNameEquals(functionName);
      if (anomalyFunctionDTO == null) {
        responseMessage.put("message", "function " + functionName + " cannot be found.");
        return Response.ok(responseMessage).build();
      }
    } else {
      Predicate predicate = Predicate.AND(
          Predicate.EQ("metric", metric),
          Predicate.EQ("collection", dataset));
      List<AnomalyFunctionDTO> anomalyFunctionDTOS = anomalyDAO.findByPredicate(predicate);
      if (anomalyFunctionDTOS == null) {
        responseMessage.put("message", "no function found on metric " + metric + " & dataset " + dataset);
        return Response.ok(responseMessage).build();
      }

      for (AnomalyFunctionDTO anomalyDTO : anomalyFunctionDTOS) {
        if (anomalyDTO.getFunctionName().startsWith(functionPrefix)) {
          functionIdList.add(anomalyDTO.getId());
        } else {
          responseMessage.put("skipped function " + anomalyDTO.getFunctionName(), "Doesn't match prefix."
              + functionPrefix);
        }
      }
    }

    AlertConfigBean.EmailConfig emailConfig = alertConfigDTO.getEmailConfig();
    if (emailConfig == null) {
      AlertConfigBean.EmailConfig emailConf = new AlertConfigBean.EmailConfig();
      emailConf.setFunctionIds(functionIdList);
      alertConfigDTO.setEmailConfig(emailConf);
    } else if (emailConfig.getFunctionIds() != null) {
      emailConfig.getFunctionIds().addAll(functionIdList);
    } else {
      emailConfig.setFunctionIds(functionIdList);
    }
    alertDAO.update(alertConfigDTO);

    responseMessage.put("message", "Alert group " + id + " successfully subscribed to following functions: "
        + functionIdList);
    return Response.ok(responseMessage).build();
  }

  /**
   * Generate an instance of SmtpConfiguration
   * This smtp configuration will take the default setting from thirdeye configuration first. If there is user defined
   * smtp setting, then use the user's definition.
   * @param smtpHost
   *    the host of the smtp server; no change if null
   * @param smtpPort
   *    the port of the smtp server; no change if null
   * @return
   *    an instance of smtp configuration with user defined host and port
   */
  private SmtpConfiguration getSmtpConfiguration(String smtpHost, Integer smtpPort) {
    SmtpConfiguration smtpConfiguration = new SmtpConfiguration();
    if (this.smtpConfiguration != null) {
      smtpConfiguration = this.smtpConfiguration;
    } else {
      if (Strings.isNullOrEmpty(smtpHost)) {
        return null;
      } else {
        smtpConfiguration.setSmtpHost(smtpHost);
      }
      // if smtpPort is null, set to be default value as 25
      smtpPort = smtpPort == null? 25 : smtpPort;
      smtpConfiguration.setSmtpPort(smtpPort);
    }

    return smtpConfiguration;
  }

  private Map<String, Map<String, Object>> getSmtpAlerterConfig(String smtpHost, Integer smtpPort) {
    Map<String, Map<String, Object>> alerterConf = new HashMap<>();
    Map<String, Object> smtpProp = new HashMap<>();

    smtpProp.put(SMTP_HOST_KEY, smtpHost);
    smtpProp.put(SMTP_PORT_KEY, smtpPort.toString());
    alerterConf.put(SMTP_CONFIG_KEY, smtpProp);

    return alerterConf;
  }

  private Response generateAnomalyReportForAnomalies(AnomalyReportGenerator anomalyReportGenerator,
      List<MergedAnomalyResultDTO> anomalies, boolean applyFilter, long startTime, long endTime, Long groupId,
      String groupName, String subject, boolean includeSentAnomaliesOnly, DetectionAlertFilterRecipients recipients,
      String fromAddr, String alertName, boolean includeSummary, String teHost, String smtpHost, int smtpPort) {
    if (recipients.getTo().isEmpty()) {
      throw new WebApplicationException("Empty : list of recipients" + recipients.getTo());
    }
    if(applyFilter){
      anomalies = AlertFilterHelper.applyFiltrationRule(anomalies, alertFilterFactory);
    }

    ThirdEyeAnomalyConfiguration configuration = new ThirdEyeAnomalyConfiguration();
    configuration.setAlerterConfiguration(getSmtpAlerterConfig(smtpHost, smtpPort));
    configuration.setDashboardHost(teHost);
    configuration.setPhantomJsPath(phantonJsPath);
    configuration.setRootDir(rootDir);

    AlertConfigDTO dummyAlertConfig = new AlertConfigDTO();
    dummyAlertConfig.setName(alertName);
    dummyAlertConfig.setFromAddress(fromAddr);

    String emailSub = Strings.isNullOrEmpty(subject) ? "Thirdeye Anomaly Report" : subject;
    anomalyReportGenerator
        .buildReport(startTime, endTime, groupId, groupName, anomalies, emailSub, configuration,
            includeSentAnomaliesOnly, recipients, alertName, dummyAlertConfig, includeSummary);
    return Response.ok("Request to generate report-email accepted ").build();
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
      @QueryParam("from") String fromAddr,
      @QueryParam("to") String toAddr, @QueryParam("cc") String ccAddr, @QueryParam("bcc") String bccAddr,
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

    SmtpConfiguration smtpConfiguration = getSmtpConfiguration(smtpHost, smtpPort);
    if (smtpConfiguration == null) {
      throw new WebApplicationException("Smtp configuration is empty or null");
    }

    if(Strings.isNullOrEmpty(teHost)) {
      teHost = dashboardHost;
    }
    AnomalyReportGenerator anomalyReportGenerator = AnomalyReportGenerator.getInstance();
    List<MergedAnomalyResultDTO> anomalies = anomalyReportGenerator
        .getAnomaliesForDatasets(Arrays.asList(dataSetArr), startTime, endTime);

    DetectionAlertFilterRecipients recipients = new DetectionAlertFilterRecipients(
        EmailUtils.getValidEmailAddresses(toAddr),
        EmailUtils.getValidEmailAddresses(ccAddr),
        EmailUtils.getValidEmailAddresses(bccAddr));
    return generateAnomalyReportForAnomalies(anomalyReportGenerator, anomalies, isApplyFilter, startTime, endTime, null,
        null, subject, includeSentAnomaliesOnly, recipients, fromAddr, "Thirdeye Anomaly Report", true,
        teHost, smtpHost, smtpPort);
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
      @QueryParam("to") String toAddr, @QueryParam("cc") String ccAddr, @QueryParam("bcc") String bccAddr,
      @QueryParam("subject") String subject,
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

    AnomalyReportGenerator anomalyReportGenerator = AnomalyReportGenerator.getInstance();
    List<MergedAnomalyResultDTO> anomalies = anomalyReportGenerator
        .getAnomaliesForMetrics(Arrays.asList(metricsArr), startTime, endTime);

    DetectionAlertFilterRecipients recipients = new DetectionAlertFilterRecipients(
        EmailUtils.getValidEmailAddresses(toAddr),
        EmailUtils.getValidEmailAddresses(ccAddr),
        EmailUtils.getValidEmailAddresses(bccAddr));
    return generateAnomalyReportForAnomalies(anomalyReportGenerator, anomalies, isApplyFilter, startTime, endTime, null,
        null, subject, includeSentAnomaliesOnly, recipients, fromAddr, "Thirdeye Anomaly Report", true,
        teHost, smtpHost, smtpPort);
  }


  @GET
  @Path("generate/functions/{startTime}/{endTime}")
  public Response generateAndSendAlertForFunctions(
      @PathParam("startTime") Long startTime, @PathParam("endTime") Long endTime,
      @QueryParam("functions") String functions, @QueryParam("from") String fromAddr,
      @QueryParam("to") String toAddr, @QueryParam("cc") String ccAddr, @QueryParam("bcc") String bccAddr,
      @QueryParam("subject") String subject,
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

    SmtpConfiguration smtpConfiguration = getSmtpConfiguration(smtpHost, smtpPort);
    if (smtpConfiguration == null) {
      throw new WebApplicationException("Smtp configuration is empty or null");
    }

    if(Strings.isNullOrEmpty(teHost)) {
      teHost = dashboardHost;
    }

    if (Strings.isNullOrEmpty(fromAddr)) {
      fromAddr = failureFromAddress;
    }

    AnomalyReportGenerator anomalyReportGenerator = AnomalyReportGenerator.getInstance();
    List<MergedAnomalyResultDTO> anomalies = anomalyReportGenerator
        .getAnomaliesForFunctions(functionList, startTime, endTime);

    DetectionAlertFilterRecipients recipients = new DetectionAlertFilterRecipients(
        EmailUtils.getValidEmailAddresses(toAddr),
        EmailUtils.getValidEmailAddresses(ccAddr),
        EmailUtils.getValidEmailAddresses(bccAddr));
    return generateAnomalyReportForAnomalies(anomalyReportGenerator, anomalies, isApplyFilter, startTime, endTime, null,
        null, subject, includeSentAnomaliesOnly, recipients, fromAddr, "Thirdeye Anomaly Report", true,
        teHost, smtpConfiguration.getSmtpHost(), smtpConfiguration.getSmtpPort());
  }


  @GET
  @Path("generate/app/{app}/{startTime}/{endTime}")
  public Response sendEmailForApp(@PathParam("app") String application, @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime, @QueryParam("from") String fromAddr,
      @QueryParam("to") String toAddr, @QueryParam("cc") String ccAddr, @QueryParam("bcc") String bccAddr,
      @QueryParam("subject") String subject,
      @QueryParam("includeSentAnomaliesOnly") boolean includeSentAnomaliesOnly,
      @QueryParam("isApplyFilter") boolean isApplyFilter,
      @QueryParam("teHost") String teHost, @QueryParam("smtpHost") String smtpHost,
      @QueryParam("smtpPort") int smtpPort) {

    if(Strings.isNullOrEmpty(application)) {
      throw new WebApplicationException("Application empty : " + application);
    }
    List<ApplicationDTO> apps = appDAO.findByName(application);
    if (apps.size() == 0) {
      throw new WebApplicationException("Application not found: " + application);
    }
    LOG.info("Generating report for application [{}]", application);
    Set<Long> anomalyFunctions = new HashSet<>();

    for (ApplicationDTO applicationDTO : apps) {
      List<AlertConfigDTO> alertConfigDTOS = alertDAO.findWhereApplicationLike(applicationDTO.getApplication());
      if (alertConfigDTOS.size() > 0) {
        for (AlertConfigDTO alertConfigDTO : alertConfigDTOS) {
          if (alertConfigDTO.getEmailConfig() != null && alertConfigDTO.getEmailConfig().getFunctionIds() != null) {
            anomalyFunctions.addAll(alertConfigDTO.getEmailConfig().getFunctionIds());
          }
        }
      }
    }

    LOG.info("Generating report for application: {}, functions {}", application, anomalyFunctions);


    List<Long> functionList = new ArrayList<>();
    functionList.addAll(anomalyFunctions);
    if (functionList.size() == 0) {
      throw new WebApplicationException("metrics empty : " + functionList);
    }

    AnomalyReportGenerator anomalyReportGenerator = AnomalyReportGenerator.getInstance();
    List<MergedAnomalyResultDTO> anomalies = anomalyReportGenerator
        .getAnomaliesForFunctions(functionList, startTime, endTime);

    DetectionAlertFilterRecipients recipients = new DetectionAlertFilterRecipients(
        EmailUtils.getValidEmailAddresses(toAddr),
        EmailUtils.getValidEmailAddresses(ccAddr),
        EmailUtils.getValidEmailAddresses(bccAddr));
    return generateAnomalyReportForAnomalies(anomalyReportGenerator, anomalies, isApplyFilter, startTime, endTime, null,
        null, subject, includeSentAnomaliesOnly, recipients, fromAddr, application, true,
        teHost, smtpHost, smtpPort);
  }

  // TODO: Deprecated Endpoint called by our own code
  public Response sendEmailWithText(
      @QueryParam("from") String fromAddr,
      @QueryParam("to") String toAddr,
      @QueryParam("cc") String ccAddr,
      @QueryParam("bcc") String bccAddr,
      @QueryParam("subject") String subject,
      @QueryParam("text") String text,
      @QueryParam("smtpHost") String smtpHost,
      @QueryParam("smtpPort") Integer smtpPort
      ){

    if (Strings.isNullOrEmpty(toAddr)) {
      throw new WebApplicationException("Empty : list of recipients" + toAddr);
    }

    SmtpConfiguration smtpConfiguration = getSmtpConfiguration(smtpHost, smtpPort);
    if (smtpConfiguration == null) {
      throw new WebApplicationException("Smtp configuration is empty or null");
    }

    if (Strings.isNullOrEmpty(fromAddr)) {
      fromAddr = failureFromAddress;
    }

    if (Strings.isNullOrEmpty(toAddr)) {
      toAddr = failureToAddress;
    }

    HtmlEmail email = new HtmlEmail();

    try {
      EmailHelper.sendEmailWithTextBody(email, smtpConfiguration, subject, text, fromAddr,
          new DetectionAlertFilterRecipients(
              EmailUtils.getValidEmailAddresses(toAddr),
              EmailUtils.getValidEmailAddresses(ccAddr),
              EmailUtils.getValidEmailAddresses(bccAddr))
      );
    } catch (EmailException e) {
      return Response.ok("Exception in sending out message").build();
    }
    return Response.ok().build();
  }
}
