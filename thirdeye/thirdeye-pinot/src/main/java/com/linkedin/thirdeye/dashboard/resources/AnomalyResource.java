package com.linkedin.thirdeye.dashboard.resources;

import io.dropwizard.hibernate.UnitOfWork;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.api.EmailConfiguration;
import com.linkedin.thirdeye.detector.api.EmailFunctionDependency;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.db.EmailConfigurationDAO;
import com.linkedin.thirdeye.detector.db.EmailFunctionDependencyDAO;
import com.linkedin.thirdeye.detector.driver.AnomalyDetectionJobManager;
import com.linkedin.thirdeye.detector.email.EmailReportJobManager;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

@Path(value = "/dashboard")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry
      .getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String DEFAULT_SMTP_PORT = "25";

  private QueryCache queryCache;
  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private AnomalyResultDAO anomalyResultDAO;
  private EmailConfigurationDAO emailConfigurationDAO;
  private EmailFunctionDependencyDAO emailFunctionDependencyDAO;
  private AnomalyDetectionJobManager anomalyDetectionJobManager;
  private EmailReportJobManager emailReportJobManager;


  public AnomalyResource(AnomalyDetectionJobManager anomalyDetectionJobManager,
      EmailReportJobManager emailReportJobManager,
      AnomalyFunctionSpecDAO anomalyFunctionSpecDAO,
      AnomalyResultDAO anomalyResultDAO,
      EmailConfigurationDAO emailConfigurationDAO,
      EmailFunctionDependencyDAO emailFunctionDependencyDAO) {

    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.anomalyDetectionJobManager = anomalyDetectionJobManager;
    this.emailReportJobManager = emailReportJobManager;
    this.anomalyFunctionSpecDAO = anomalyFunctionSpecDAO;
    this.anomalyResultDAO = anomalyResultDAO;
    this.emailConfigurationDAO = emailConfigurationDAO;
    this.emailFunctionDependencyDAO = emailFunctionDependencyDAO;
  }

  public AnomalyResource() {
  }

  /************** CRUD for anomalies of a collection ********************************************************/

  // View anomalies for collection
  @GET
  @UnitOfWork
  @Path("/anomalies/view")
  public List<AnomalyResult> viewAnomaliesInRange(@QueryParam("dataset") String dataset,
      @QueryParam("startTimeIso") String startTimeIso,
      @QueryParam("endTimeIso") String endTimeIso,
      @QueryParam("metric") String metric,
      @QueryParam("dimensions") String dimensions) {

    List<AnomalyResult> anomalyResults = new ArrayList<>();
    List<AnomalyResult> anomalies = null;
    try {
      DateTime endTime = DateTime.now();
      if (StringUtils.isNotEmpty(endTimeIso)) {
        endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
      }
      DateTime startTime = endTime.minusDays(7);
      if (StringUtils.isNotEmpty(startTimeIso)) {
        startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
      }
      System.out.println(dataset + " " + startTimeIso + " " + startTime + " " + endTimeIso + " " + endTime + " " + metric);
      if (StringUtils.isEmpty(metric)) {
        anomalyResults = anomalyResultDAO.findAllByCollectionAndTime(dataset, startTime, endTime);
      } else {
        anomalyResults = anomalyResultDAO.findAllByCollectionTimeAndMetric(dataset, metric, startTime, endTime);
      }
      anomalies = anomalyResults;

      System.out.println(dimensions);
      if (StringUtils.isNotEmpty(dimensions)) {

        dimensions = URLDecoder.decode(dimensions, "UTF-8");
        Multimap<String, String> dimensionsMap = ThirdEyeUtils.convertToMultiMap(dimensions);

        System.out.println(dimensions);
        anomalies = new ArrayList<>();
        for (AnomalyResult anomalyResult : anomalyResults) {
          String filters = anomalyResult.getFilters();
          String dimensionString = anomalyResult.getDimensions();
          if (StringUtils.isNotEmpty(filters) && StringUtils.isNotEmpty(dimensionString)
              && StringUtils.isNotEmpty(dimensionString = dimensionString.replaceAll("\\*|,", ""))) {
            String dimensionName = filters.split("=")[0];
            if (dimensionsMap.containsKey(dimensionName) && dimensionsMap.get(dimensionName).contains(dimensionString)) {
              anomalies.add(anomalyResult);
            }
          }
        }
      }
      System.out.println(anomalies.size());

    } catch (Exception e) {
      LOG.error("Exception in fetching anomalies", e);
    }
    return anomalies;
  }

  /************* CRUD for anomaly functions of collection **********************************************/

  // View all anomaly functions
  @GET
  @UnitOfWork
  @Path("/anomaly-function/view")
  public List<AnomalyFunctionSpec> viewAnomalyFunctions(@QueryParam("dataset") String dataset, @QueryParam("metric") String metric) {

    List<AnomalyFunctionSpec> anomalyFunctionSpecs = anomalyFunctionSpecDAO.findAllByCollection(dataset);

    List<AnomalyFunctionSpec> anomalyFunctions = anomalyFunctionSpecs;

    if (StringUtils.isNotEmpty(metric)) {
      anomalyFunctions = new ArrayList<>();
      for (AnomalyFunctionSpec anomalyFunctionSpec : anomalyFunctionSpecs) {
        if (metric.equals(anomalyFunctionSpec.getMetric())) {
          anomalyFunctions.add(anomalyFunctionSpec);
        }
      }
    }
    return anomalyFunctions;
  }

  // Add anomaly function
  @POST
  @UnitOfWork
  @Path("/anomaly-function/create")
  public Response createAnomalyFunction(@QueryParam("dataset") String dataset,
      @QueryParam("functionName") String functionName,
      @QueryParam("metric") String metric,
      @QueryParam("type") String type,
      @QueryParam("windowSize") String windowSize,
      @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") String windowDelay,
      @QueryParam("windowDelayUnit") String windowDelayUnit,
      @QueryParam("scheduleMinute") String scheduleMinute,
      @QueryParam("scheduleHour") String scheduleHour,
      @QueryParam("repeatEvery") String repeatEvery,
      @QueryParam("exploreDimension") String exploreDimensions,
      @QueryParam("properties") String properties,
      @QueryParam("isActive") boolean isActive)
          throws Exception {

    if (StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionName) || StringUtils.isEmpty(metric)
        || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit) || StringUtils.isEmpty(properties)) {
      throw new UnsupportedOperationException("Received null for one of the mandatory params: "
          + "dataset " + dataset + ", functionName " + functionName + ", metric " + metric
          + ", windowSize " + windowSize + ", windowUnit " + windowUnit + ", properties" + properties);
    }

    CollectionSchema schema = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache().get(dataset);
    TimeGranularity dataGranularity = schema.getTime().getDataGranularity();

    AnomalyFunctionSpec anomalyFunctionSpec = new AnomalyFunctionSpec();
    anomalyFunctionSpec.setIsActive(isActive);
    anomalyFunctionSpec.setCollection(dataset);
    anomalyFunctionSpec.setFunctionName(functionName);
    anomalyFunctionSpec.setMetric(metric);
    if (StringUtils.isEmpty(type)) {
      type = "USER_RULE";
    }
    anomalyFunctionSpec.setType(type);
    anomalyFunctionSpec.setWindowSize(Integer.valueOf(windowSize));
    anomalyFunctionSpec.setWindowUnit(TimeUnit.valueOf(windowUnit));

    TimeUnit windowDelayTimeUnit = TimeUnit.valueOf(windowUnit);
    if (StringUtils.isNotEmpty(windowDelayUnit)) {
      windowDelayTimeUnit = TimeUnit.valueOf(windowDelayUnit);
    }
    int windowDelayTime;
    if (StringUtils.isNotEmpty(windowDelay)) {
      windowDelayTime = Integer.valueOf(windowDelay);
    } else {
      Long maxDateTime = CACHE_REGISTRY_INSTANCE.getCollectionMaxDataTimeCache().get(dataset);
      windowDelayTime = (int) windowDelayTimeUnit.convert(System.currentTimeMillis() - maxDateTime, TimeUnit.MILLISECONDS);
    }
    anomalyFunctionSpec.setWindowDelayUnit(windowDelayTimeUnit);
    anomalyFunctionSpec.setWindowDelay(windowDelayTime);

    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());

    anomalyFunctionSpec.setExploreDimensions(exploreDimensions);
    anomalyFunctionSpec.setProperties(properties);

    String cron = "";
    if (StringUtils.isNotEmpty(repeatEvery)) {
      cron = constructCron(scheduleMinute, scheduleHour, repeatEvery);
    }
    anomalyFunctionSpec.setCron(cron);

    Long id = anomalyFunctionSpecDAO.createOrUpdate(anomalyFunctionSpec);

    if (isActive) {
      anomalyDetectionJobManager.start(id);
    }

    return Response.ok(id).build();
  }

  private String constructCron(String scheduleMinute, String scheduleHour, String repeatEvery) {

    String minute = "0";
    String hour = "0";
    String cron = "0 0 0 * * ?";
    if (repeatEvery.equals(TimeUnit.DAYS.toString())) {
      minute = StringUtils.isEmpty(scheduleMinute) ? minute : scheduleMinute;
      hour = StringUtils.isEmpty(scheduleHour) ? hour : scheduleHour;
    } else if (repeatEvery.equals(TimeUnit.HOURS.toString())) {
      minute = StringUtils.isEmpty(scheduleMinute) ? minute : scheduleMinute;
      hour = "*";
    }
    cron = String.format("0 %s %s * * ?", minute, hour);
    return cron;

  }

  // Edit anomaly function
  @POST
  @UnitOfWork
  @Path("/anomaly-function/update")
  public Response updateAnomalyFunction(@QueryParam("id") Long id,
      @QueryParam("dataset") String dataset,
      @QueryParam("functionName") String functionName,
      @QueryParam("metric") String metric,
      @QueryParam("type") String type,
      @QueryParam("windowSize") String windowSize,
      @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") String windowDelay,
      @QueryParam("windowDelayUnit") String windowDelayUnit,
      @QueryParam("scheduleMinute") String scheduleMinute,
      @QueryParam("scheduleHour") String scheduleHour,
      @QueryParam("repeatEvery") String repeatEvery,
      @QueryParam("exploreDimension") String exploreDimensions,
      @QueryParam("properties") String properties,
      @QueryParam("isActive") boolean isActive) throws ExecutionException {

    if (id == null || StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionName)
        || StringUtils.isEmpty(metric) || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit)
        || StringUtils.isEmpty(windowDelay) || StringUtils.isEmpty(properties)) {
      throw new UnsupportedOperationException("Received null for one of the mandatory params: "
          + "id " + id + ",dataset " + dataset + ", functionName " + functionName + ", metric " + metric
          + ", windowSize " + windowSize + ", windowUnit " + windowUnit + ", windowDelay " + windowDelay
          + ", properties" + properties);
    }

    CollectionSchema schema = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache().get(dataset);
    TimeGranularity dataGranularity = schema.getTime().getDataGranularity();

    AnomalyFunctionSpec anomalyFunctionSpec = new AnomalyFunctionSpec();
    anomalyFunctionSpec.setId(id);
    anomalyFunctionSpec.setIsActive(isActive);
    anomalyFunctionSpec.setCollection(dataset);
    anomalyFunctionSpec.setFunctionName(functionName);
    anomalyFunctionSpec.setMetric(metric);
    if (StringUtils.isEmpty(type)) {
      type = "USER_RULE";
    }
    anomalyFunctionSpec.setType(type);
    anomalyFunctionSpec.setWindowSize(Integer.valueOf(windowSize));
    anomalyFunctionSpec.setWindowUnit(TimeUnit.valueOf(windowUnit));
    anomalyFunctionSpec.setWindowDelay(Integer.valueOf(windowDelay));
    if (StringUtils.isEmpty(windowDelayUnit)) {
      anomalyFunctionSpec.setWindowDelayUnit(TimeUnit.valueOf(windowUnit));
    } else {
      anomalyFunctionSpec.setWindowDelayUnit(TimeUnit.valueOf(windowDelayUnit));
    }
    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());
    anomalyFunctionSpec.setExploreDimensions(exploreDimensions);
    anomalyFunctionSpec.setProperties(properties);

    String cron = "";
    if (StringUtils.isNotEmpty(repeatEvery)) {
      cron = constructCron(scheduleMinute, scheduleHour, repeatEvery);
    }
    anomalyFunctionSpec.setCron(cron);

    Long responseId = anomalyFunctionSpecDAO.createOrUpdate(anomalyFunctionSpec);

    return Response.ok(responseId).build();
  }

  // Delete anomaly function
  @DELETE
  @UnitOfWork
  @Path("/anomaly-function/delete")
  public Response deleteAnomalyFunctions(@QueryParam("id") Long id, @QueryParam("functionName") String functionName) {

    anomalyFunctionSpecDAO.delete(id);
    return Response.noContent().build();
  }

  // Run anomaly function ad hoc
  @POST
  @UnitOfWork
  @Path("/anomaly-function/adhoc")
  public Response runAdhocAnomalyFunctions(@QueryParam("id") Long id, @QueryParam("functionName") String functionName,
      @QueryParam("windowStartIso") String windowStartIso, @QueryParam("windowEndIso") String windowEndIso)
          throws Exception {

    AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if (StringUtils.isEmpty(windowStartIso) || StringUtils.isEmpty(windowEndIso)) {
      int windowSize = anomalyFunctionSpec.getWindowSize();
      TimeUnit windowUnit = anomalyFunctionSpec.getWindowUnit();
      int delaySize = anomalyFunctionSpec.getWindowDelay();
      TimeUnit delayUnit = anomalyFunctionSpec.getWindowDelayUnit();

      DateTime now = new DateTime();
      DateTime windowEnd = now.minus(TimeUnit.MILLISECONDS.convert(delaySize, delayUnit));
      windowEndIso = windowEnd.toString();
      DateTime windowStart = windowEnd.minus(TimeUnit.MILLISECONDS.convert(windowSize, windowUnit));
      windowStartIso = windowStart.toString();
    }
    anomalyDetectionJobManager.runAdHoc(id, windowStartIso, windowEndIso);
    return Response.noContent().build();
  }

  /*************** CRUD for email functions of collection *********************************************/

  // View all email functions
  @GET
  @UnitOfWork
  @Path("/email-config/view")
  public List<EmailConfiguration> viewEmailConfigs(@QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric) {

    if (StringUtils.isEmpty(dataset)) {
      throw new UnsupportedOperationException("dataset is a required query param");
    }

    List<EmailConfiguration> emailConfigSpecs = emailConfigurationDAO.findAll();

    List<EmailConfiguration> emailConfigurations = new ArrayList<>();
    for (EmailConfiguration emailConfigSpec : emailConfigSpecs) {
      if (dataset.equals(emailConfigSpec.getCollection()) &&
          (StringUtils.isEmpty(metric) || (StringUtils.isNotEmpty(metric) && metric.equals(emailConfigSpec.getMetric())))) {

        emailConfigurations.add(emailConfigSpec);
      }
    }
    return emailConfigurations;
  }

  // Add email function
  @POST
  @UnitOfWork
  @Path("/email-config/create")
  public Response createEmailConfigs(@QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric,
      @QueryParam("fromAddress") String fromAddress,
      @QueryParam("toAddresses") String toAddresses,
      @QueryParam("repeatEvery") String repeatEvery,
      @QueryParam("scheduleMinute") String scheduleMinute,
      @QueryParam("scheduleHour") String scheduleHour,
      @QueryParam("smtpHost") String smtpHost,
      @QueryParam("smtpPort") String smtpPort,
      @QueryParam("windowSize") String windowSize,
      @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") String windowDelay,
      @QueryParam("windowDelayUnit") String windowDelayUnit,
      @QueryParam("filters") String filters,
      @QueryParam("isActive") boolean isActive,
      @QueryParam("sendZeroAnomalyEmail") boolean sendZeroAnomalyEmail,
      @QueryParam("functionIds") String functionIds) {

    if (StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionIds) || StringUtils.isEmpty(metric)
        || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit) || StringUtils.isEmpty(fromAddress)
        || StringUtils.isEmpty(toAddresses) || StringUtils.isEmpty(smtpHost)) {
      throw new UnsupportedOperationException("Received null for one of the mandatory params: "
          + "dataset " + dataset + ", functionIds " + functionIds + ", metric " + metric
          + ", windowSize " + windowSize + ", windowUnit " + windowUnit + ", fromAddress" + fromAddress
          + ", toAddresses " + toAddresses + ", smtpHost" + smtpHost);
    }

    EmailConfiguration emailConfiguration = new EmailConfiguration();
    emailConfiguration.setCollection(dataset);
    emailConfiguration.setMetric(metric);
    emailConfiguration.setFromAddress(fromAddress);
    emailConfiguration.setToAddresses(toAddresses);
    String cron = "";
    if (StringUtils.isNotEmpty(repeatEvery)) {
      cron = constructCron(scheduleMinute, scheduleHour, repeatEvery);
    }
    emailConfiguration.setCron(cron);
    if (StringUtils.isEmpty(smtpPort)) {
      smtpPort = DEFAULT_SMTP_PORT;
    }
    emailConfiguration.setSmtpHost(smtpHost);
    emailConfiguration.setSmtpPort(Integer.valueOf(smtpPort));

    emailConfiguration.setWindowSize(Integer.valueOf(windowSize));
    emailConfiguration.setWindowUnit(TimeUnit.valueOf(windowUnit));

    TimeUnit windowDelayTimeUnit = TimeUnit.valueOf(windowUnit);
    if (StringUtils.isNotEmpty(windowDelayUnit)) {
      windowDelayTimeUnit = TimeUnit.valueOf(windowDelayUnit);
    }
    int windowDelayTime = 0;
    if (StringUtils.isNotEmpty(windowDelay)) {
      windowDelayTime = Integer.valueOf(windowDelay);
    }
    emailConfiguration.setWindowDelayUnit(windowDelayTimeUnit);
    emailConfiguration.setWindowDelay(windowDelayTime);

    emailConfiguration.setIsActive(isActive);
    emailConfiguration.setSendZeroAnomalyEmail(sendZeroAnomalyEmail);
    emailConfiguration.setFilters(filters);

    List<AnomalyFunctionSpec> anomalyFunctionSpecs = new ArrayList<>();
    for (String functionIdString : functionIds.split(",")) {
      AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(Long.valueOf(functionIdString));
      anomalyFunctionSpecs.add(anomalyFunctionSpec);
    }
    emailConfiguration.setFunctions(anomalyFunctionSpecs);

    Long id = emailConfigurationDAO.createOrUpdate(emailConfiguration);

    return Response.ok(id).build();
  }

  // Update email function
  @POST
  @UnitOfWork
  @Path("/email-config/update")
  public Response updateEmailConfigs(@QueryParam("id") Long id,
      @QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric,
      @QueryParam("fromAddress") String fromAddress,
      @QueryParam("toAddresses") String toAddresses,
      @QueryParam("repeatEvery") String repeatEvery,
      @QueryParam("scheduleMinute") String scheduleMinute,
      @QueryParam("scheduleHour") String scheduleHour,
      @QueryParam("smtpHost") String smtpHost,
      @QueryParam("smtpPort") String smtpPort,
      @QueryParam("windowSize") String windowSize,
      @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") String windowDelay,
      @QueryParam("windowDelayUnit") String windowDelayUnit,
      @QueryParam("filters") String filters,
      @QueryParam("isActive") boolean isActive,
      @QueryParam("sendZeroAnomalyEmail") boolean sendZeroAnomalyEmail,
      @QueryParam("functionIds") String functionIds) {

    if (id == null || StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionIds) || StringUtils.isEmpty(metric)
        || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit) || StringUtils.isEmpty(fromAddress)
        || StringUtils.isEmpty(toAddresses) || StringUtils.isEmpty(smtpHost)) {
      throw new UnsupportedOperationException("Received null for one of the mandatory params: "
          + "dataset " + dataset + ", functionIds " + functionIds + ", metric " + metric
          + ", windowSize " + windowSize + ", windowUnit " + windowUnit + ", fromAddress" + fromAddress
          + ", toAddresses " + toAddresses + ", smtpHost" + smtpHost);
    }

    EmailConfiguration emailConfiguration = new EmailConfiguration();
    emailConfiguration.setId(id);
    emailConfiguration.setCollection(dataset);
    emailConfiguration.setMetric(metric);
    emailConfiguration.setFromAddress(fromAddress);
    emailConfiguration.setToAddresses(toAddresses);
    String cron = "";
    if (StringUtils.isNotEmpty(repeatEvery)) {
      cron = constructCron(scheduleMinute, scheduleHour, repeatEvery);
    }
    emailConfiguration.setCron(cron);
    if (StringUtils.isEmpty(smtpPort)) {
      smtpPort = DEFAULT_SMTP_PORT;
    }
    emailConfiguration.setSmtpHost(smtpHost);
    emailConfiguration.setSmtpPort(Integer.valueOf(smtpPort));

    emailConfiguration.setWindowSize(Integer.valueOf(windowSize));
    emailConfiguration.setWindowUnit(TimeUnit.valueOf(windowUnit));

    TimeUnit windowDelayTimeUnit = TimeUnit.valueOf(windowUnit);
    if (StringUtils.isNotEmpty(windowDelayUnit)) {
      windowDelayTimeUnit = TimeUnit.valueOf(windowDelayUnit);
    }
    int windowDelayTime = 0;
    if (StringUtils.isNotEmpty(windowDelay)) {
      windowDelayTime = Integer.valueOf(windowDelay);
    }
    emailConfiguration.setWindowDelayUnit(windowDelayTimeUnit);
    emailConfiguration.setWindowDelay(windowDelayTime);

    emailConfiguration.setIsActive(isActive);
    emailConfiguration.setSendZeroAnomalyEmail(sendZeroAnomalyEmail);
    emailConfiguration.setFilters(filters);

    List<AnomalyFunctionSpec> anomalyFunctionSpecs = new ArrayList<>();
    for (String functionIdString : functionIds.split(",")) {
      AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(Long.valueOf(functionIdString));
      anomalyFunctionSpecs.add(anomalyFunctionSpec);
    }
    emailConfiguration.setFunctions(anomalyFunctionSpecs);

    Long responseId = emailConfigurationDAO.createOrUpdate(emailConfiguration);

    return Response.ok(responseId).build();
  }


  // Delete email function
  @DELETE
  @UnitOfWork
  @Path("/email-config/delete")
  public Response deleteEmailConfigs(@QueryParam("id") Long id) {

    emailConfigurationDAO.delete(id);
    return Response.noContent().build();
  }

  // Run email function ad hoc
  @POST
  @UnitOfWork
  @Path("/email-config/adhoc")
  public Response runAdhocEmailConfig(@QueryParam("id") Long id) throws Exception {
    emailReportJobManager.sendAdHoc(id);
    return Response.ok(id).build();
  }

  public static void main(String[] args) {

  }

}
