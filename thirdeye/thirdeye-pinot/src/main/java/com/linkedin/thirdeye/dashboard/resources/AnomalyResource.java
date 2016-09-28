package com.linkedin.thirdeye.dashboard.resources;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import jersey.repackaged.com.google.common.base.Joiner;
import jersey.repackaged.com.google.common.collect.Lists;

@Path(value = "/dashboard")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyResource.class);

  public static final String DEFAULT_CRON = "0 0 0 * * ?";
  private static final String UTF8 = "UTF-8";
  private static final String STAR_DIMENSION = "*";
  private static final String DIMENSION_JOINER = ",";
  private static final String DEFAULT_FUNCTION_TYPE = "WEEK_OVER_WEEK_RULE";

  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager anomalyMergedResultDAO;
  private RawAnomalyResultManager anomalyResultDAO;
  private EmailConfigurationManager emailConfigurationDAO;

  private ThirdEyeDashboardConfiguration dashboardConfiguration;

  public AnomalyResource(ThirdEyeDashboardConfiguration dashboardConfiguration,
      AnomalyFunctionManager anomalyFunctionDAO, RawAnomalyResultManager anomalyResultDAO,
      EmailConfigurationManager emailConfigurationDAO, MergedAnomalyResultManager anomalyMergedResultDAO) {
    this.dashboardConfiguration = dashboardConfiguration;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.anomalyResultDAO = anomalyResultDAO;
    this.anomalyMergedResultDAO = anomalyMergedResultDAO;
    this.emailConfigurationDAO = emailConfigurationDAO;
  }

  /************** CRUD for anomalies of a collection ********************************************************/
  @GET
  @Path("/anomalies/metrics")
  public List<String> viewMetricsForDataset(@QueryParam("dataset") String dataset) {
    if (StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("dataset is a required query param");
    }
    List<String> metrics = anomalyFunctionDAO.findDistinctMetricsByCollection(dataset);
    return metrics;
  }

  // View merged anomalies for collection
  @GET
  @Path("/anomalies/view")
  public List<MergedAnomalyResultDTO> viewAnomaliesInRange(@NotNull @QueryParam("dataset") String dataset,
      @QueryParam("startTimeIso") String startTimeIso,
      @QueryParam("endTimeIso") String endTimeIso,
      @QueryParam("metric") String metric,
      @QueryParam("dimensions") String dimensions) {

    if (StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("dataset is a required query param");
    }

    DateTime endTime = DateTime.now();
    if (StringUtils.isNotEmpty(endTimeIso)) {
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
    }
    DateTime startTime = endTime.minusDays(7);
    if (StringUtils.isNotEmpty(startTimeIso)) {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    }
    List<MergedAnomalyResultDTO> anomalyResults = new ArrayList<>();
    try {
      String[] dimensionPatterns = null;
      if (StringUtils.isNotBlank(dimensions)) {
        // get dimension names and index position
        List<String> dimensionNames = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache().get(dataset).getDimensionNames();
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
          anomalyResults = anomalyMergedResultDAO.findByCollectionMetricDimensionsTime(dataset, metric, dimensionPatterns, startTime.getMillis(), endTime.getMillis());
        } else {
          anomalyResults = anomalyMergedResultDAO.findByCollectionMetricTime(dataset, metric, startTime.getMillis(), endTime.getMillis());
        }
      } else {
        anomalyResults = anomalyMergedResultDAO.findByCollectionTime(dataset, startTime.getMillis(), endTime.getMillis());
      }

    } catch (Exception e) {
      LOG.error("Exception in fetching anomalies", e);
    }
    return anomalyResults;
  }

  /************* CRUD for anomaly functions of collection **********************************************/

  // View all anomaly functions
  @GET
  @Path("/anomaly-function/view")
  public List<AnomalyFunctionDTO> viewAnomalyFunctions(@NotNull @QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric) {

    if (StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("dataset is a required query param");
    }

    List<AnomalyFunctionDTO> anomalyFunctionSpecs = anomalyFunctionDAO.findAllByCollection(dataset);
    List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionSpecs;

    if (StringUtils.isNotEmpty(metric)) {
      anomalyFunctions = new ArrayList<>();
      for (AnomalyFunctionDTO anomalyFunctionSpec : anomalyFunctionSpecs) {
        if (metric.equals(anomalyFunctionSpec.getMetric())) {
          anomalyFunctions.add(anomalyFunctionSpec);
        }
      }
    }
    return anomalyFunctions;
  }

  // Add anomaly function
  @POST
  @Path("/anomaly-function/create")
  public Response createAnomalyFunction(@NotNull @QueryParam("dataset") String dataset,
      @NotNull @QueryParam("functionName") String functionName,
      @NotNull @QueryParam("metric") String metric,
      @NotNull @QueryParam("metricFunction") String metric_function,
      @QueryParam("type") String type,
      @NotNull @QueryParam("windowSize") String windowSize,
      @NotNull @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") String windowDelay,
      @QueryParam("cron") String cron,
      @QueryParam("windowDelayUnit") String windowDelayUnit,
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

    AnomalyFunctionDTO anomalyFunctionSpec = new AnomalyFunctionDTO();
    anomalyFunctionSpec.setActive(isActive);
    anomalyFunctionSpec.setMetricFunction(MetricAggFunction.valueOf(metric_function));
    anomalyFunctionSpec.setCollection(dataset);
    anomalyFunctionSpec.setFunctionName(functionName);
    anomalyFunctionSpec.setMetric(metric);
    if (StringUtils.isEmpty(type)) {
      type = DEFAULT_FUNCTION_TYPE;
    }
    anomalyFunctionSpec.setType(type);
    anomalyFunctionSpec.setWindowSize(Integer.valueOf(windowSize));
    anomalyFunctionSpec.setWindowUnit(TimeUnit.valueOf(windowUnit));

    TimeUnit dataGranularityUnit = dataGranularity.getUnit();
    TimeUnit windowDelayTimeUnit =
        dataGranularityUnit.equals(TimeUnit.MINUTES) || dataGranularityUnit.equals(TimeUnit.HOURS)
            ? TimeUnit.HOURS : TimeUnit.DAYS;
    if (StringUtils.isNotEmpty(windowDelayUnit)) {
      windowDelayTimeUnit = TimeUnit.valueOf(windowDelayUnit);
    }
    int windowDelayTime = 2;
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

    if (!StringUtils.isBlank(filters)) {
      filters = URLDecoder.decode(filters, UTF8);
      String filterString = ThirdEyeUtils.getSortedFiltersFromJson(filters);
      anomalyFunctionSpec.setFilters(filterString);
    }
    anomalyFunctionSpec.setProperties(properties);

    if (StringUtils.isEmpty(cron)) {
      cron = DEFAULT_CRON;
    } else {
      // validate cron
      if (!CronExpression.isValidExpression(cron)) {
        throw new IllegalArgumentException("Invalid cron expression for cron : " + cron);
      }
    }
    anomalyFunctionSpec.setCron(cron);

    Long id = anomalyFunctionDAO.save(anomalyFunctionSpec);
    return Response.ok(id).build();
  }

  // Edit anomaly function
  @POST
  @Path("/anomaly-function/update")
  public Response updateAnomalyFunction(@NotNull @QueryParam("id") Long id,
      @NotNull @QueryParam("dataset") String dataset,
      @NotNull @QueryParam("functionName") String functionName,
      @NotNull @QueryParam("metric") String metric,
      @QueryParam("type") String type,
      @NotNull @QueryParam("windowSize") String windowSize,
      @NotNull @QueryParam("windowUnit") String windowUnit,
      @NotNull @QueryParam("windowDelay") String windowDelay,
      @QueryParam("cron") String cron,
      @QueryParam("windowDelayUnit") String windowDelayUnit,
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

    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalStateException("AnomalyFunctionSpec with id " + id + " does not exist");
    }

    CollectionSchema schema = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache().get(dataset);
    TimeGranularity dataGranularity = schema.getTime().getDataGranularity();

    anomalyFunctionSpec.setActive(isActive);
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

    TimeUnit dataGranularityUnit = dataGranularity.getUnit();
    TimeUnit windowDelayTimeUnit =
        dataGranularityUnit.equals(TimeUnit.MINUTES) || dataGranularityUnit.equals(TimeUnit.HOURS)
            ? TimeUnit.HOURS : TimeUnit.DAYS;
    if (StringUtils.isNotEmpty(windowDelayUnit)) {
      windowDelayTimeUnit = TimeUnit.valueOf(windowDelayUnit);
    }
    anomalyFunctionSpec.setWindowDelayUnit(windowDelayTimeUnit);

    // bucket size and unit are defaulted to the collection granularity
    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());

    if (!StringUtils.isBlank(filters)) {
      filters = URLDecoder.decode(filters, UTF8);
      String filterString = ThirdEyeUtils.getSortedFiltersFromJson(filters);
      anomalyFunctionSpec.setFilters(filterString);
    }
    anomalyFunctionSpec.setExploreDimensions(exploreDimensions);
    anomalyFunctionSpec.setProperties(properties);

    if (StringUtils.isEmpty(cron)) {
      cron = DEFAULT_CRON;
    } else {
      // validate cron
      if (!CronExpression.isValidExpression(cron)) {
        throw new IllegalArgumentException("Invalid cron expression for cron : " + cron);
      }
    }
    anomalyFunctionSpec.setCron(cron);

    anomalyFunctionDAO.update(anomalyFunctionSpec);
    return Response.ok(id).build();
  }

  // Delete anomaly function
  @DELETE
  @Path("/anomaly-function/delete")
  public Response deleteAnomalyFunctions(@NotNull @QueryParam("id") Long id,
      @QueryParam("functionName") String functionName)
      throws IOException {

    if (id == null) {
      throw new IllegalArgumentException("id is a required query param");
    }

    // call endpoint to stop if active
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalStateException("No anomalyFunctionSpec with id " + id);
    }

    // delete dependent entities
    // email config mapping
    List<EmailConfigurationDTO> emailConfigurations = emailConfigurationDAO.findByFunctionId(id);
    for (EmailConfigurationDTO emailConfiguration : emailConfigurations) {
      emailConfiguration.getFunctions().remove(anomalyFunctionSpec);
      emailConfigurationDAO.update(emailConfiguration);
    }

    // raw result mapping
    List<RawAnomalyResultDTO> rawResults = anomalyResultDAO.findAllByTimeAndFunctionId(0, System.currentTimeMillis(), id);
    for (RawAnomalyResultDTO result : rawResults) {
      anomalyResultDAO.delete(result);
    }

    // merged anomaly mapping
    List<MergedAnomalyResultDTO> mergedResults = anomalyMergedResultDAO.findByFunctionId(id);
    for (MergedAnomalyResultDTO result : mergedResults) {
      anomalyMergedResultDAO.delete(result);
    }

    // delete from db
    anomalyFunctionDAO.deleteById(id);

    return Response.noContent().build();
  }

  /*************** CRUD for email functions of collection *********************************************/

  // View all email functions
  @GET
  @Path("/email-config/view")
  public List<EmailConfigurationDTO> viewEmailConfigs(@NotNull @QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric) {

    if (StringUtils.isEmpty(dataset)) {
      throw new UnsupportedOperationException("dataset is a required query param");
    }

    List<EmailConfigurationDTO> emailConfigSpecs = emailConfigurationDAO.findAll();

    List<EmailConfigurationDTO> emailConfigurations = new ArrayList<>();
    for (EmailConfigurationDTO emailConfigSpec : emailConfigSpecs) {
      if (dataset.equals(emailConfigSpec.getCollection()) &&
          (StringUtils.isEmpty(metric) || (StringUtils.isNotEmpty(metric) && metric.equals(emailConfigSpec.getMetric())))) {

        emailConfigurations.add(emailConfigSpec);
      }
    }
    return emailConfigurations;
  }

  // Add email function
  @POST
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
      @QueryParam("functionIds") String functionIds) throws IOException {

    if (StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionIds) || StringUtils.isEmpty(metric)
        || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit) || StringUtils.isEmpty(fromAddress)
        || StringUtils.isEmpty(toAddresses)) {
      throw new UnsupportedOperationException("Received null for one of the mandatory params: "
          + "dataset " + dataset + ", functionIds " + functionIds + ", metric " + metric
          + ", windowSize " + windowSize + ", windowUnit " + windowUnit + ", fromAddress" + fromAddress
          + ", toAddresses " + toAddresses);
    }

    EmailConfigurationDTO emailConfiguration = new EmailConfigurationDTO();
    emailConfiguration.setActive(isActive);
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

    List<AnomalyFunctionDTO> anomalyFunctionSpecs = new ArrayList<>();
    for (String functionIdString : functionIds.split(",")) {
      AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(Long.valueOf(functionIdString));
      anomalyFunctionSpecs.add(anomalyFunctionSpec);
    }
    emailConfiguration.setFunctions(anomalyFunctionSpecs);

    Long id = emailConfigurationDAO.save(emailConfiguration);

    return Response.ok(id).build();
  }

  // Update email function
  @POST
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
      @QueryParam("functionIds") String functionIds) throws IOException {

    if (id == null || StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionIds) || StringUtils.isEmpty(metric)
        || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit) || StringUtils.isEmpty(fromAddress)
        || StringUtils.isEmpty(toAddresses)) {
      throw new UnsupportedOperationException("Received null for one of the mandatory params: "
          + "dataset " + dataset + ", functionIds " + functionIds + ", metric " + metric
          + ", windowSize " + windowSize + ", windowUnit " + windowUnit + ", fromAddress" + fromAddress
          + ", toAddresses " + toAddresses);
    }

    // stop email report if active
    EmailConfigurationDTO emailConfiguration = emailConfigurationDAO.findById(id);
    if (emailConfiguration == null) {
      throw new IllegalStateException("No email configuration for id " + id);
    }

    emailConfiguration.setActive(isActive);
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

    List<AnomalyFunctionDTO> anomalyFunctionSpecs = new ArrayList<>();
    for (String functionIdString : functionIds.split(",")) {
      AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(Long.valueOf(functionIdString));
      anomalyFunctionSpecs.add(anomalyFunctionSpec);
    }
    emailConfiguration.setFunctions(anomalyFunctionSpecs);

    emailConfigurationDAO.update(emailConfiguration);
    return Response.ok(id).build();
  }


  // Delete email function
  @DELETE
  @Path("/email-config/delete")
  public Response deleteEmailConfigs(@NotNull @QueryParam("id") Long id) throws ClientProtocolException, IOException {

    if (id == null) {
      throw new IllegalArgumentException("id is a required query param");
    }
    // stop schedule if active
    EmailConfigurationDTO emailConfiguration = emailConfigurationDAO.findById(id);
    if (emailConfiguration == null) {
      throw new IllegalStateException("No emailConfiguraiton for id " + id);
    }

    // delete from db
    emailConfigurationDAO.deleteById(id);
    return Response.noContent().build();
  }


  @GET
  @Path(value = "anomaly-result/feedback")
  @Produces(MediaType.APPLICATION_JSON)
  public AnomalyFeedbackType[] getAnomalyFeedbackTypes() {
    return AnomalyFeedbackType.values();
  }

  /**
   * @param anomalyResultId : anomaly merged result id
   * @param payload         : Json payload containing feedback @see com.linkedin.thirdeye.constant.AnomalyFeedbackType
   *                        eg. payload
   *                        <p/>
   *                        { "feedbackType": "NOT_ANOMALY", "comment": "this is not an anomaly" }
   */
  @POST
  @Path(value = "anomaly-merged-result/feedback/{anomaly_merged_result_id}")
  public void updateAnomalyMergedResultFeedback(@PathParam("anomaly_merged_result_id") long anomalyResultId, String payload) {
    try {
      MergedAnomalyResultDTO result = anomalyMergedResultDAO.findById(anomalyResultId);
      if (result == null) {
        throw new IllegalArgumentException("AnomalyResult not found with id " + anomalyResultId);
      }
      ObjectMapper mapper = new ObjectMapper();
      AnomalyFeedbackDTO feedbackRequest = mapper.readValue(payload, AnomalyFeedbackDTO.class);
      AnomalyFeedbackDTO feedback = result.getFeedback();
      if (feedback == null) {
        feedback = new AnomalyFeedbackDTO();
        result.setFeedback(feedback);
      }
      if (feedbackRequest.getStatus() == null) {
        feedback.setStatus(FeedbackStatus.NEW);
      } else {
        feedback.setStatus(feedbackRequest.getStatus());
      }
      feedback.setComment(feedbackRequest.getComment());
      feedback.setFeedbackType(feedbackRequest.getFeedbackType());

      anomalyMergedResultDAO.update(result);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid payload " + payload, e);
    }
  }

  @POST
  @Path(value = "anomaly-result/feedback/{anomaly_result_id}")
  public void updateAnomalyResultFeedback(@PathParam("anomaly_result_id") long anomalyResultId, String payload) {
    try {
      RawAnomalyResultDTO result = anomalyResultDAO.findById(anomalyResultId);
      if (result == null) {
        throw new IllegalArgumentException("AnomalyResult not found with id " + anomalyResultId);
      }
      ObjectMapper mapper = new ObjectMapper();
      AnomalyFeedbackDTO feedbackRequest = mapper.readValue(payload, AnomalyFeedbackDTO.class);
      AnomalyFeedbackDTO feedback = result.getFeedback();
      if (feedback == null) {
        feedback = new AnomalyFeedbackDTO();
        result.setFeedback(feedback);
      }
      if (feedbackRequest.getStatus() == null) {
        feedback.setStatus(FeedbackStatus.NEW);
      } else {
        feedback.setStatus(feedbackRequest.getStatus());
      }
      feedback.setComment(feedbackRequest.getComment());
      feedback.setFeedbackType(feedbackRequest.getFeedbackType());

      anomalyResultDAO.update(result);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid payload " + payload, e);
    }
  }
}
