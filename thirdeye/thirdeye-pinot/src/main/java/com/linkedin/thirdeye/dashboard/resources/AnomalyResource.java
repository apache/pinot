package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.constant.AnomalyFeedback;
import io.dropwizard.hibernate.UnitOfWork;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.transaction.Transactional;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import jersey.repackaged.com.google.common.base.Joiner;
import jersey.repackaged.com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.dashboard.DetectorHttpUtils;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.api.EmailConfiguration;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.db.EmailConfigurationDAO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

@Path(value = "/dashboard")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyResource.class);

  public static final String DEFAULT_CRON = "0 0 0 * * ?";
  private static final String UTF8 = "UTF-8";
  private static final String STAR_DIMENSION = "*";
  private static final String DIMENSION_JOINER = ",";
  private static final String DEFAULT_FUNCTION_TYPE = "USER_RULE";

  private AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  private AnomalyResultDAO anomalyResultDAO;
  private EmailConfigurationDAO emailConfigurationDAO;
  private DetectorHttpUtils detectorHttpUtils;
  private ThirdEyeDashboardConfiguration dashboardConfiguration;

  public AnomalyResource(AnomalyFunctionSpecDAO anomalyFunctionSpecDAO,
      AnomalyResultDAO anomalyResultDAO,
      EmailConfigurationDAO emailConfigurationDAO,
      ThirdEyeDashboardConfiguration dashboardConfiguration) {

    this.dashboardConfiguration = dashboardConfiguration;
    this.detectorHttpUtils = new DetectorHttpUtils(dashboardConfiguration.getDetectorHost(),
        dashboardConfiguration.getDetectorPort());
    this.anomalyFunctionSpecDAO = anomalyFunctionSpecDAO;
    this.anomalyResultDAO = anomalyResultDAO;
    this.emailConfigurationDAO = emailConfigurationDAO;
  }

  /************** CRUD for anomalies of a collection ********************************************************/

  @GET
  @UnitOfWork
  @Path("/anomalies/metrics")
  public List<String> viewMetricsForDataset(@QueryParam("dataset") String dataset) {
    if (StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("dataset is a required query param");
    }
    List<String> metrics = anomalyFunctionSpecDAO.findDistinctMetricsByCollection(dataset);
    return metrics;
  }

  // View anomalies for collection
  @GET
  @UnitOfWork
  @Path("/anomalies/view")
  public List<AnomalyResult> viewAnomaliesInRange(@NotNull @QueryParam("dataset") String dataset,
      @QueryParam("startTimeIso") String startTimeIso,
      @QueryParam("endTimeIso") String endTimeIso,
      @QueryParam("metric") String metric,
      @QueryParam("dimensions") String dimensions) {

    if (StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("dataset is a required query param");
    }

    List<AnomalyResult> anomalyResults = new ArrayList<>();
    try {
      DateTime endTime = DateTime.now();
      if (StringUtils.isNotEmpty(endTimeIso)) {
        endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
      }
      DateTime startTime = endTime.minusDays(7);
      if (StringUtils.isNotEmpty(startTimeIso)) {
        startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
      }

      String[] dimensionPatterns = null;
      if (StringUtils.isNotBlank(dimensions)) {

        // get dimension names and index position
        List<String> dimensionNames = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache()
            .get(dataset).getDimensionNames();
        Map<String, Integer> dimensionNameToIndexMap = new HashMap<>();
        for (int i = 0; i < dimensionNames.size(); i ++) {
          dimensionNameToIndexMap.put(dimensionNames.get(i), i);
        }

        // get dimensions map from request
        dimensions = URLDecoder.decode(dimensions, UTF8 );
        Multimap<String, String> dimensionsMap = ThirdEyeUtils.convertToMultiMap(dimensions);

        // create dimension patterns
        String[] dimensionsArray = new String[dimensionNames.size()];
        Arrays.fill(dimensionsArray, STAR_DIMENSION);
        List<String> dimensionPatternsList = new ArrayList<>();

        for (String dimensionName : dimensionsMap.keySet()) {
          List<String> dimensionValues = Lists.newArrayList(dimensionsMap.get(dimensionName));
          int dimensionIndex = dimensionNameToIndexMap.get(dimensionName);
          for (String dimensionValue : dimensionValues) {
            StringBuffer sb = new StringBuffer();
            dimensionsArray[dimensionIndex] = dimensionValue;
            sb.append(Joiner.on(DIMENSION_JOINER).join(Lists.newArrayList(dimensionsArray)));
            dimensionPatternsList.add(sb.toString());
            dimensionsArray[dimensionIndex] = STAR_DIMENSION;
          }
        }
        dimensionPatterns = new String[dimensionPatternsList.size()];
        dimensionPatterns = dimensionPatternsList.toArray(dimensionPatterns);
      }

      if (StringUtils.isNotBlank(metric)) {
        if (StringUtils.isNotBlank(dimensions)) {
          anomalyResults = anomalyResultDAO.findAllByCollectionTimeMetricAndDimensions(dataset, metric,
              startTime, endTime, dimensionPatterns);
        } else {
          anomalyResults = anomalyResultDAO.findAllByCollectionTimeAndMetric(dataset, metric,
              startTime, endTime);
        }
      } else {
        anomalyResults = anomalyResultDAO.findAllByCollectionAndTime(dataset, startTime, endTime);
      }

    } catch (Exception e) {
      LOG.error("Exception in fetching anomalies", e);
    }
    return anomalyResults;
  }

  /************* CRUD for anomaly functions of collection **********************************************/

  // View all anomaly functions
  @GET
  @UnitOfWork
  @Path("/anomaly-function/view")
  public List<AnomalyFunctionSpec> viewAnomalyFunctions(@NotNull @QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric) {

    if (StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("dataset is a required query param");
    }

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
  public Response createAnomalyFunction(@NotNull @QueryParam("dataset") String dataset,
      @NotNull @QueryParam("functionName") String functionName,
      @NotNull @QueryParam("metric") String metric,
      @QueryParam("type") String type,
      @NotNull @QueryParam("windowSize") String windowSize,
      @NotNull @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") String windowDelay,
      @QueryParam("windowDelayUnit") String windowDelayUnit,
      @QueryParam("scheduleMinute") String scheduleMinute,
      @QueryParam("scheduleHour") String scheduleHour,
      @QueryParam("repeatEvery") String repeatEvery,
      @QueryParam("exploreDimension") String exploreDimensions,
      @QueryParam("filters") String filters,
      @NotNull @QueryParam("properties") String properties,
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
    anomalyFunctionSpec.setIsActive(false);
    anomalyFunctionSpec.setCollection(dataset);
    anomalyFunctionSpec.setFunctionName(functionName);
    anomalyFunctionSpec.setMetric(metric);
    if (StringUtils.isEmpty(type)) {
      type = DEFAULT_FUNCTION_TYPE;
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

    // bucket size and unit are defaulted to the collection granularity
    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());

    anomalyFunctionSpec.setExploreDimensions(exploreDimensions);
    anomalyFunctionSpec.setFilters(filters);
    anomalyFunctionSpec.setProperties(properties);

    String cron = DEFAULT_CRON;
    if (StringUtils.isNotEmpty(repeatEvery)) {
      cron = ThirdEyeUtils.constructCron(scheduleMinute, scheduleHour, TimeUnit.valueOf(repeatEvery));
    }
    anomalyFunctionSpec.setCron(cron);

    Long id = anomalyFunctionSpecDAO.createOrUpdate(anomalyFunctionSpec);

    if (isActive) { // this call will set isActive and schedule it
      detectorHttpUtils.enableAnomalyFunction(String.valueOf(id));
    }

    return Response.ok(id).build();
  }

  // Edit anomaly function
  @POST
  @UnitOfWork
  @Path("/anomaly-function/update")
  public Response updateAnomalyFunction(@NotNull @QueryParam("id") Long id,
      @NotNull @QueryParam("dataset") String dataset,
      @NotNull @QueryParam("functionName") String functionName,
      @NotNull @QueryParam("metric") String metric,
      @QueryParam("type") String type,
      @NotNull @QueryParam("windowSize") String windowSize,
      @NotNull @QueryParam("windowUnit") String windowUnit,
      @NotNull @QueryParam("windowDelay") String windowDelay,
      @QueryParam("windowDelayUnit") String windowDelayUnit,
      @QueryParam("scheduleMinute") String scheduleMinute,
      @QueryParam("scheduleHour") String scheduleHour,
      @QueryParam("repeatEvery") String repeatEvery,
      @QueryParam("exploreDimension") String exploreDimensions,
      @QueryParam("filters") String filters,
      @NotNull @QueryParam("properties") String properties,
      @QueryParam("isActive") boolean isActive) throws Exception {

    if (id == null || StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionName)
        || StringUtils.isEmpty(metric) || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit)
        || StringUtils.isEmpty(windowDelay) || StringUtils.isEmpty(properties)) {
      throw new UnsupportedOperationException("Received null for one of the mandatory params: "
          + "id " + id + ",dataset " + dataset + ", functionName " + functionName + ", metric " + metric
          + ", windowSize " + windowSize + ", windowUnit " + windowUnit + ", windowDelay " + windowDelay
          + ", properties" + properties);
    }

    AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalStateException("AnomalyFunctionSpec with id " + id + " does not exist");
    }
    // call endpoint to stop if active
    if (anomalyFunctionSpec.getIsActive()) {
      detectorHttpUtils.disableAnomalyFunction(String.valueOf(id));
    }

    CollectionSchema schema = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache().get(dataset);
    TimeGranularity dataGranularity = schema.getTime().getDataGranularity();

    anomalyFunctionSpec.setIsActive(false);
    anomalyFunctionSpec.setCollection(dataset);
    anomalyFunctionSpec.setFunctionName(functionName);
    anomalyFunctionSpec.setMetric(metric);
    if (StringUtils.isEmpty(type)) {
      type = DEFAULT_FUNCTION_TYPE;
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

    // bucket size and unit are defaulted to the collection granularity
    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());

    anomalyFunctionSpec.setFilters(filters);
    anomalyFunctionSpec.setExploreDimensions(exploreDimensions);
    anomalyFunctionSpec.setProperties(properties);

    String cron = DEFAULT_CRON;
    if (StringUtils.isNotEmpty(repeatEvery)) {
      cron = ThirdEyeUtils.constructCron(scheduleMinute, scheduleHour, TimeUnit.valueOf(repeatEvery));
    }
    anomalyFunctionSpec.setCron(cron);

    Long responseId = anomalyFunctionSpecDAO.createOrUpdate(anomalyFunctionSpec);

    if (isActive) {
      detectorHttpUtils.enableAnomalyFunction(String.valueOf(responseId));
    }

    return Response.ok(responseId).build();
  }

  // Delete anomaly function
  @DELETE
  @UnitOfWork
  @Path("/anomaly-function/delete")
  public Response deleteAnomalyFunctions(@NotNull @QueryParam("id") Long id,
      @QueryParam("functionName") String functionName)
      throws IOException {

    if (id == null) {
      throw new IllegalArgumentException("id is a required query param");
    }

    // call endpoint to stop if active
    AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalStateException("No anomalyFunctionSpec with id " + id);
    }
    if (anomalyFunctionSpec.getIsActive()) {
      detectorHttpUtils.disableAnomalyFunction(String.valueOf(id));
    }

    // delete from db
    anomalyFunctionSpecDAO.delete(anomalyFunctionSpec);

    return Response.noContent().build();
  }

  // Run anomaly function ad hoc
  @POST
  @UnitOfWork
  @Path("/anomaly-function/adhoc")
  public Response runAdhocAnomalyFunctions(@NotNull @QueryParam("id") Long id,
      @QueryParam("functionName") String functionName,
      @QueryParam("windowStartIso") String windowStartIso,
      @QueryParam("windowEndIso") String windowEndIso)
          throws Exception {

    if (id == null) {
      throw new IllegalArgumentException("id is a required query param");
    }

    AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalStateException("No anomalyFunctionSpec with id " + id);
    }
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
    // call endpoint to run adhoc
    detectorHttpUtils.runAdhocAnomalyFunction(String.valueOf(id), windowStartIso, windowEndIso);
    return Response.noContent().build();
  }

  /*************** CRUD for email functions of collection *********************************************/

  // View all email functions
  @GET
  @UnitOfWork
  @Path("/email-config/view")
  public List<EmailConfiguration> viewEmailConfigs(@NotNull @QueryParam("dataset") String dataset,
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
  public Response createEmailConfigs(@NotNull @QueryParam("dataset") String dataset,
      @NotNull @QueryParam("metric") String metric,
      @NotNull @QueryParam("fromAddress") String fromAddress,
      @NotNull @QueryParam("toAddresses") String toAddresses,
      @QueryParam("repeatEvery") String repeatEvery,
      @QueryParam("scheduleMinute") String scheduleMinute,
      @QueryParam("scheduleHour") String scheduleHour,
      @NotNull @QueryParam("windowSize") String windowSize,
      @NotNull @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") String windowDelay,
      @QueryParam("windowDelayUnit") String windowDelayUnit,
      @QueryParam("filters") String filters,
      @QueryParam("isActive") boolean isActive,
      @QueryParam("sendZeroAnomalyEmail") boolean sendZeroAnomalyEmail,
      @QueryParam("functionIds") String functionIds) throws ClientProtocolException, IOException {

    if (StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionIds) || StringUtils.isEmpty(metric)
        || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit) || StringUtils.isEmpty(fromAddress)
        || StringUtils.isEmpty(toAddresses)) {
      throw new UnsupportedOperationException("Received null for one of the mandatory params: "
          + "dataset " + dataset + ", functionIds " + functionIds + ", metric " + metric
          + ", windowSize " + windowSize + ", windowUnit " + windowUnit + ", fromAddress" + fromAddress
          + ", toAddresses " + toAddresses);
    }

    EmailConfiguration emailConfiguration = new EmailConfiguration();
    emailConfiguration.setIsActive(false);
    emailConfiguration.setCollection(dataset);
    emailConfiguration.setMetric(metric);
    emailConfiguration.setFromAddress(fromAddress);
    emailConfiguration.setToAddresses(toAddresses);
    String cron = DEFAULT_CRON;
    if (StringUtils.isNotEmpty(repeatEvery)) {
      cron = ThirdEyeUtils.constructCron(scheduleMinute, scheduleHour, TimeUnit.valueOf(repeatEvery));
    }
    emailConfiguration.setCron(cron);

    emailConfiguration.setSmtpHost(dashboardConfiguration.getSmtpHost());
    emailConfiguration.setSmtpPort(dashboardConfiguration.getSmtpPort());

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

    emailConfiguration.setSendZeroAnomalyEmail(sendZeroAnomalyEmail);
    emailConfiguration.setFilters(filters);

    List<AnomalyFunctionSpec> anomalyFunctionSpecs = new ArrayList<>();
    for (String functionIdString : functionIds.split(",")) {
      AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(Long.valueOf(functionIdString));
      anomalyFunctionSpecs.add(anomalyFunctionSpec);
    }
    emailConfiguration.setFunctions(anomalyFunctionSpecs);

    Long id = emailConfigurationDAO.createOrUpdate(emailConfiguration);
    // enable id isActive
    if (isActive) {
      detectorHttpUtils.enableEmailConfiguration(String.valueOf(id));
    }

    return Response.ok(id).build();
  }

  // Update email function
  @POST
  @UnitOfWork
  @Path("/email-config/update")
  public Response updateEmailConfigs(@NotNull @QueryParam("id") Long id,
      @NotNull @QueryParam("dataset") String dataset,
      @NotNull @QueryParam("metric") String metric,
      @NotNull @QueryParam("fromAddress") String fromAddress,
      @NotNull @QueryParam("toAddresses") String toAddresses,
      @QueryParam("repeatEvery") String repeatEvery,
      @QueryParam("scheduleMinute") String scheduleMinute,
      @QueryParam("scheduleHour") String scheduleHour,
      @NotNull @QueryParam("windowSize") String windowSize,
      @NotNull @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") String windowDelay,
      @QueryParam("windowDelayUnit") String windowDelayUnit,
      @QueryParam("filters") String filters,
      @QueryParam("isActive") boolean isActive,
      @QueryParam("sendZeroAnomalyEmail") boolean sendZeroAnomalyEmail,
      @QueryParam("functionIds") String functionIds) throws ClientProtocolException, IOException {

    if (id == null || StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionIds) || StringUtils.isEmpty(metric)
        || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit) || StringUtils.isEmpty(fromAddress)
        || StringUtils.isEmpty(toAddresses)) {
      throw new UnsupportedOperationException("Received null for one of the mandatory params: "
          + "dataset " + dataset + ", functionIds " + functionIds + ", metric " + metric
          + ", windowSize " + windowSize + ", windowUnit " + windowUnit + ", fromAddress" + fromAddress
          + ", toAddresses " + toAddresses);
    }

    // stop email report if active
    EmailConfiguration emailConfiguration = emailConfigurationDAO.findById(id);
    if (emailConfiguration == null) {
      throw new IllegalStateException("No email configuration for id " + id);
    }
    if (emailConfiguration.getIsActive()) {
      detectorHttpUtils.disableEmailConfiguration(String.valueOf(id));
    }
    emailConfiguration.setIsActive(false);
    emailConfiguration.setId(id);
    emailConfiguration.setCollection(dataset);
    emailConfiguration.setMetric(metric);
    emailConfiguration.setFromAddress(fromAddress);
    emailConfiguration.setToAddresses(toAddresses);
    String cron = DEFAULT_CRON;
    if (StringUtils.isNotEmpty(repeatEvery)) {
      cron = ThirdEyeUtils.constructCron(scheduleMinute, scheduleHour, TimeUnit.valueOf(repeatEvery));
    }
    emailConfiguration.setCron(cron);

    emailConfiguration.setSmtpHost(dashboardConfiguration.getSmtpHost());
    emailConfiguration.setSmtpPort(dashboardConfiguration.getSmtpPort());

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

    emailConfiguration.setSendZeroAnomalyEmail(sendZeroAnomalyEmail);
    emailConfiguration.setFilters(filters);

    List<AnomalyFunctionSpec> anomalyFunctionSpecs = new ArrayList<>();
    for (String functionIdString : functionIds.split(",")) {
      AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(Long.valueOf(functionIdString));
      anomalyFunctionSpecs.add(anomalyFunctionSpec);
    }
    emailConfiguration.setFunctions(anomalyFunctionSpecs);

    Long responseId = emailConfigurationDAO.createOrUpdate(emailConfiguration);

    // call endpoint to start, if active
    if (isActive) {
      detectorHttpUtils.enableEmailConfiguration(String.valueOf(id));
    }
    return Response.ok(responseId).build();
  }


  // Delete email function
  @DELETE
  @UnitOfWork
  @Path("/email-config/delete")
  public Response deleteEmailConfigs(@NotNull @QueryParam("id") Long id) throws ClientProtocolException, IOException {

    if (id == null) {
      throw new IllegalArgumentException("id is a required query param");
    }
    // stop schedule if active
    EmailConfiguration emailConfiguration = emailConfigurationDAO.findById(id);
    if (emailConfiguration == null) {
      throw new IllegalStateException("No emailConfiguraiton for id " + id);
    }
    if (emailConfiguration.getIsActive()) {
      detectorHttpUtils.disableEmailConfiguration(String.valueOf(id));
    }
    // delete from db
    emailConfigurationDAO.delete(emailConfiguration);
    return Response.noContent().build();
  }

  // Run email function ad hoc
  @POST
  @UnitOfWork
  @Path("/email-config/adhoc")
  public Response runAdhocEmailConfig(@NotNull @QueryParam("id") Long id) throws Exception {

    if (id == null) {
      throw new IllegalArgumentException("id is a required query param");
    }
    EmailConfiguration emailConfiguration = emailConfigurationDAO.findById(id);
    if (emailConfiguration == null) {
      throw new IllegalStateException("No emailConfiguraiton for id " + id);
    }
    detectorHttpUtils.runAdhocEmailConfiguration(String.valueOf(id));
    return Response.ok(id).build();
  }

  @GET
  @Path(value = "anomaly-result/feedback")
  @Produces(MediaType.APPLICATION_JSON)
  public AnomalyFeedback[] getAnomalyFeedbackTypes() {
    return AnomalyFeedback.values();
  }

  @POST
  @Path(value = "anomaly-result/feedback/{id}/{feedback}")
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public void updateAnomalyResultFeedback(@PathParam("id") long anomalyResultId,
      @PathParam("feedback") AnomalyFeedback feedBack) {
    AnomalyResult result = anomalyResultDAO.findById(anomalyResultId);
    if(result == null) {
      throw new IllegalArgumentException("AnomalyResult not found with id " + anomalyResultId);
    }
    result.setFeedback(feedBack);
    anomalyResultDAO.createOrUpdate(result);
  }
}
