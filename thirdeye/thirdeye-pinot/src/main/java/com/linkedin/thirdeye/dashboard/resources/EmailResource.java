package com.linkedin.thirdeye.dashboard.resources;

import com.google.common.base.Strings;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.alert.util.AnomalyReportGenerator;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.ApplicationManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.ApplicationDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
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


@Path("thirdeye/email")
@Produces(MediaType.APPLICATION_JSON)
public class EmailResource {
  private static final Logger LOG = LoggerFactory.getLogger(EmailResource.class);

  private final AlertConfigManager alertDAO;
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
    this(thirdEyeConfig.getSmtpConfiguration(), new AlertFilterFactory(thirdEyeConfig.getAlertFilterConfigPath()),
        thirdEyeConfig.getFailureFromAddress(), thirdEyeConfig.getFailureToAddress(), thirdEyeConfig.getDashboardHost(),
        thirdEyeConfig.getPhantomJsPath(), thirdEyeConfig.getRootDir());
  }

  public EmailResource(SmtpConfiguration smtpConfiguration, AlertFilterFactory alertFilterFactory,
      String failureFromAddress, String failureToAddress,
      String dashboardHost, String phantonJsPath, String rootDir) {
    this.smtpConfiguration = smtpConfiguration;
    this.alertDAO = DAO_REGISTRY.getAlertConfigDAO();
    this.appDAO = DAO_REGISTRY.getApplicationDAO();
    this.alertFilterFactory = alertFilterFactory;
    this.failureFromAddress = failureFromAddress;
    this.failureToAddress = failureToAddress;
    this.dashboardHost = dashboardHost;
    this.phantonJsPath = phantonJsPath;
    this.rootDir = rootDir;
  }

  @POST
  @Path("alert")
  public Response createAlertConfig(AlertConfigDTO alertConfigDTO) {
    if (Strings.isNullOrEmpty(alertConfigDTO.getFromAddress())) {
      alertConfigDTO.setFromAddress(failureToAddress);
    }
    if (Strings.isNullOrEmpty(alertConfigDTO.getRecipients())) {
      LOG.error("Unable to proceed user request with empty recipients: {}", alertConfigDTO);
      return Response.status(Response.Status.BAD_REQUEST).entity("Empty field on recipients").build();
    }
    if (Strings.isNullOrEmpty(alertConfigDTO.getCronExpression())) {
      LOG.error("Unable to proceed user request with empty cron: {}", alertConfigDTO);
      return Response.status(Response.Status.BAD_REQUEST).entity("Empty field on cron").build();
    }
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

  @GET
  @Path("function/{id}")
  public List<AlertConfigDTO> getSubscriberList(@PathParam("id") Long alertFunctionId) {
    return getAlertToSubscriberMapping().get(alertFunctionId);
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

  private Response generateAnomalyReportForAnomalies(AnomalyReportGenerator anomalyReportGenerator,
      List<MergedAnomalyResultDTO> anomalies, boolean applyFilter, long startTime, long endTime, Long groupId, String groupName,
      String subject, boolean includeSentAnomaliesOnly, String toAddr, String fromAddr, String alertName,
      boolean includeSummary, String teHost, String smtpHost, int smtpPort) {
    if (Strings.isNullOrEmpty(toAddr)) {
      throw new WebApplicationException("Empty : list of recipients" + toAddr);
    }
    if(applyFilter){
      anomalies = AlertFilterHelper.applyFiltrationRule(anomalies, alertFilterFactory);
    }

    ThirdEyeAnomalyConfiguration configuration = new ThirdEyeAnomalyConfiguration();
    configuration.setSmtpConfiguration(getSmtpConfiguration(smtpHost, smtpPort));
    configuration.setDashboardHost(teHost);
    configuration.setPhantomJsPath(phantonJsPath);
    configuration.setRootDir(rootDir);

    AlertConfigDTO dummyAlertConfig = new AlertConfigDTO();
    dummyAlertConfig.setName(alertName);
    dummyAlertConfig.setFromAddress(fromAddr);

    String emailSub = Strings.isNullOrEmpty(subject) ? "Thirdeye Anomaly Report" : subject;
    anomalyReportGenerator
        .buildReport(startTime, endTime, groupId, groupName, anomalies, emailSub, configuration,
            includeSentAnomaliesOnly, toAddr, alertName, dummyAlertConfig, includeSummary);
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

    return generateAnomalyReportForAnomalies(anomalyReportGenerator, anomalies, isApplyFilter, startTime, endTime, null,
        null, subject, includeSentAnomaliesOnly, toAddr, fromAddr, "Thirdeye Anomaly Report", true,
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

    AnomalyReportGenerator anomalyReportGenerator = AnomalyReportGenerator.getInstance();
    List<MergedAnomalyResultDTO> anomalies = anomalyReportGenerator
        .getAnomaliesForMetrics(Arrays.asList(metricsArr), startTime, endTime);

    return generateAnomalyReportForAnomalies(anomalyReportGenerator, anomalies, isApplyFilter, startTime, endTime, null,
        null, subject, includeSentAnomaliesOnly, toAddr, fromAddr, "Thirdeye Anomaly Report", true,
        teHost, smtpHost, smtpPort);
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

    return generateAnomalyReportForAnomalies(anomalyReportGenerator, anomalies, isApplyFilter, startTime, endTime, null,
        null, subject, includeSentAnomaliesOnly, toAddr, fromAddr, "Thirdeye Anomaly Report", true,
        teHost, smtpConfiguration.getSmtpHost(), smtpConfiguration.getSmtpPort());
  }


  @GET
  @Path("generate/app/{app}/{startTime}/{endTime}")
  public Response sendEmailForApp(@PathParam("app") String application, @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime, @QueryParam("from") String fromAddr,
      @QueryParam("to") String toAddr,@QueryParam("subject") String subject,
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

    return generateAnomalyReportForAnomalies(anomalyReportGenerator, anomalies, isApplyFilter, startTime, endTime, null,
        null, subject, includeSentAnomaliesOnly, toAddr, fromAddr, application, true,
        teHost, smtpHost, smtpPort);
  }

  @GET
  @Path("notification")
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
      EmailHelper.sendEmailWithTextBody(email, smtpConfiguration, subject, text,
         fromAddr, toAddr
      );
    } catch (EmailException e) {
      return Response.ok("Exception in sending out message").build();
    }
    return Response.ok().build();
  }
}
