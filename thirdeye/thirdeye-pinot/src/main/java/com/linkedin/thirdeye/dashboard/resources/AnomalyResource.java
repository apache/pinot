package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.anomaly.detection.TimeSeriesUtil;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
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

  private static final String DEFAULT_CRON = "0 0 0 * * ?";
  private static final String UTF8 = "UTF-8";
  private static final String STAR_DIMENSION = "*";
  private static final String DIMENSION_JOINER = ",";
  private static final String DEFAULT_FUNCTION_TYPE = "WEEK_OVER_WEEK_RULE";

  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager anomalyMergedResultDAO;
  private RawAnomalyResultManager anomalyResultDAO;
  private EmailConfigurationManager emailConfigurationDAO;
  private DatasetConfigManager datasetConfigDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public AnomalyResource(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.anomalyResultDAO = DAO_REGISTRY.getRawAnomalyResultDAO();
    this.anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.emailConfigurationDAO = DAO_REGISTRY.getEmailConfigurationDAO();
    this.datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
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
        List<String> dimensionNames = datasetConfigDAO.findByDataset(dataset).getDimensions();
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

    DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
    TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeGranularity dataGranularity = timespec.getDataGranularity();

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

    // Setting window delay time / unit
    TimeUnit dataGranularityUnit = dataGranularity.getUnit();

    // default window delay time = 10 hours
    int windowDelayTime = 10;
    TimeUnit windowDelayTimeUnit = TimeUnit.HOURS;

    if(dataGranularityUnit.equals(TimeUnit.MINUTES) || dataGranularityUnit.equals(TimeUnit.HOURS)) {
      windowDelayTime = 4;
    }
    anomalyFunctionSpec.setWindowDelayUnit(windowDelayTimeUnit);
    anomalyFunctionSpec.setWindowDelay(windowDelayTime);

    // bucket size and unit are defaulted to the collection granularity
    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());

    if(StringUtils.isNotEmpty(exploreDimensions)) {
      anomalyFunctionSpec.setExploreDimensions(getDimensions(dataset, exploreDimensions));
    }
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

    DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
    TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeGranularity dataGranularity = timespec.getDataGranularity();

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

    // bucket size and unit are defaulted to the collection granularity
    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());

    if (!StringUtils.isBlank(filters)) {
      filters = URLDecoder.decode(filters, UTF8);
      String filterString = ThirdEyeUtils.getSortedFiltersFromJson(filters);
      anomalyFunctionSpec.setFilters(filterString);
    }
    anomalyFunctionSpec.setProperties(properties);

    if(StringUtils.isNotEmpty(exploreDimensions)) {
      // Ensure that the explore dimension names are ordered as schema dimension names
      anomalyFunctionSpec.setExploreDimensions(getDimensions(dataset, exploreDimensions));
    }
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

  private String getDimensions(String dataset, String exploreDimensions) throws Exception {
    // Ensure that the explore dimension names are ordered as schema dimension names
    List<String> schemaDimensionNames = CACHE_REGISTRY_INSTANCE.getDatasetConfigCache().get(dataset).getDimensions();
    Set<String> splitExploreDimensions = new HashSet<>(Arrays.asList(exploreDimensions.trim().split(",")));
    StringBuilder reorderedExploreDimensions = new StringBuilder();
    String separator = "";
    for (String dimensionName : schemaDimensionNames) {
      if (splitExploreDimensions.contains(dimensionName)) {
        reorderedExploreDimensions.append(separator).append(dimensionName);
        separator = ",";
      }
    }
    return reorderedExploreDimensions.toString();
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
    List<RawAnomalyResultDTO> rawResults =
        anomalyResultDAO.findAllByTimeAndFunctionId(0, System.currentTimeMillis(), id);
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

  /************ Anomaly Feedback **********/
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

  /**
   * Returns the time series for the given anomaly.
   *
   * If viewWindowStartTime and/or viewWindowEndTime is not given, then a window is padded automatically. The padded
   * windows is half of the anomaly window size. For instance, if the anomaly lasts for 4 hours, then the pad window
   * size is 2 hours. The max padding size is 1 day.
   *
   * @param anomalyResultId the id of the given anomaly
   * @param viewWindowStartTime start time of the time series, inclusive
   * @param viewWindowEndTime end time of the time series, inclusive
   * @return the time series of the given anomaly
   * @throws Exception when it fails to retrieve collection, i.e., dataset, information
   */
  @GET
  @Path("/anomaly-merged-result/timeseries/{anomaly_merged_result_id}")
  public AnomalyTimelinesView getAnomalyMergedResultTimeSeries(@NotNull @PathParam("anomaly_merged_result_id") long anomalyResultId,
      @NotNull @QueryParam("aggTimeGranularity") String aggTimeGranularity, @QueryParam("start") long viewWindowStartTime,
      @QueryParam("end") long viewWindowEndTime)
      throws Exception {

    MergedAnomalyResultDTO anomalyResult = anomalyMergedResultDAO.findById(anomalyResultId);
    String dimensions = anomalyResult.getDimensions();
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyResult.getFunction();
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    // Calculate view window start and end if they are not given by the user, which should be 0 if it is not given.
    // By default, the padding window size is half of the anomaly window.
    if (viewWindowStartTime == 0 || viewWindowEndTime == 0) {
      long anomalyWindowStartTime = anomalyResult.getStartTime();
      long anomalyWindowEndTime = anomalyResult.getEndTime();

      long bucketMillis = TimeUnit.MILLISECONDS.convert(anomalyFunctionSpec.getBucketSize(), anomalyFunctionSpec.getBucketUnit());
      long bucketCount = (anomalyWindowEndTime - anomalyWindowStartTime) / bucketMillis;
      long paddingMillis = Math.max(1, (bucketCount / 2)) * bucketMillis;
      if (paddingMillis > TimeUnit.DAYS.toMillis(1)) {
        paddingMillis = TimeUnit.DAYS.toMillis(1);
      }

      if (viewWindowStartTime == 0) {
        viewWindowStartTime = anomalyWindowStartTime - paddingMillis;
      }
      if (viewWindowEndTime == 0) {
        viewWindowEndTime = anomalyWindowEndTime + paddingMillis;
      }
    }

    TimeGranularity timeGranularity =
        Utils.getAggregationTimeGranularity(aggTimeGranularity, anomalyFunctionSpec.getCollection());
    long bucketMillis = timeGranularity.toMillis();
    // ThirdEye backend is end time exclusive, so one more bucket is appended to make end time inclusive for frontend.
    viewWindowEndTime += bucketMillis;

    TimeSeriesResponse timeSeriesResponse =
        TimeSeriesUtil.getTimeSeriesResponseForPresentation(anomalyFunction, dimensions, timeGranularity,
            viewWindowStartTime, viewWindowEndTime);

    TimeSeriesResponseConverter timeSeriesResponseConverter = TimeSeriesResponseConverter.getInstance();
    Map<DimensionKey, MetricTimeSeries> res = timeSeriesResponseConverter.toMap(timeSeriesResponse,
        Utils.getSchemaDimensionNames(anomalyResult.getFunction().getCollection()));

    // Currently, we assume that we get time series for one metric at a time
    String metricName = anomalyFunctionSpec.getMetric();
    Iterator<MetricTimeSeries> ite = res.values().iterator();
    if (ite.hasNext()) {
      MetricTimeSeries metricTimeSeries = ite.next();
      AnomalyTimelinesView anomalyTimelinesView =
          anomalyFunction.getPresentationTimeseries(metricTimeSeries, bucketMillis, metricName, viewWindowStartTime, viewWindowEndTime, null);

      // Generate summary for frontend
      List<TimeBucket> timeBuckets = anomalyTimelinesView.getTimeBuckets();
      if (timeBuckets.size() > 0) {
        TimeBucket firstBucket = timeBuckets.get(0);
        anomalyTimelinesView.addSummary("currentStart", Long.toString(firstBucket.getCurrentStart()));
        anomalyTimelinesView.addSummary("baselineStart", Long.toString(firstBucket.getBaselineStart()));

        TimeBucket lastBucket = timeBuckets.get(timeBuckets.size()-1);
        anomalyTimelinesView.addSummary("currentEnd", Long.toString(lastBucket.getCurrentStart()));
        anomalyTimelinesView.addSummary("baselineEnd", Long.toString(lastBucket.getBaselineEnd()));
      }

      return anomalyTimelinesView;
    }

    return null;
  }

}
