package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.MetricTransfer;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONObject;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType.*;


@Path(value = "/dashboard")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyResource.class);
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String DEFAULT_CRON = "0 0 0 * * ?";
  private static final String UTF8 = "UTF-8";
  private static final String DEFAULT_FUNCTION_TYPE = "WEEK_OVER_WEEK_RULE";

  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager anomalyMergedResultDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;
  private EmailConfigurationManager emailConfigurationDAO;
  private MetricConfigManager metricConfigDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private OverrideConfigManager overrideConfigDAO;
  private AutotuneConfigManager autotuneConfigDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AlertFilterFactory alertFilterFactory;
  private LoadingCache<String, Long> collectionMaxDataTimeCache;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public AnomalyResource(AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory) {
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.rawAnomalyResultDAO = DAO_REGISTRY.getRawAnomalyResultDAO();
    this.anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.emailConfigurationDAO = DAO_REGISTRY.getEmailConfigurationDAO();
    this.metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.overrideConfigDAO = DAO_REGISTRY.getOverrideConfigDAO();
    this.autotuneConfigDAO = DAO_REGISTRY.getAutotuneConfigDAO();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    this.alertFilterFactory = alertFilterFactory;
    this.collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getCollectionMaxDataTimeCache();
  }

  /************** CRUD for anomalies of a collection ********************************************************/
  @GET
  @Path("/anomalies/metrics")
  public List<String> viewMetricsForDataset(@QueryParam("dataset") String dataset) {
    if (StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("dataset is a required query param");
    }
    List<String> metrics = anomalyFunctionDAO.findDistinctTopicMetricsByCollection(dataset);
    return metrics;
  }

  @GET
  @Path("/anomalies/view/{anomaly_merged_result_id}")
  public MergedAnomalyResultDTO getMergedAnomalyDetail(
      @NotNull @PathParam("anomaly_merged_result_id") long mergedAnomalyId) {
    return anomalyMergedResultDAO.findById(mergedAnomalyId);
  }

  // View merged anomalies for collection
  @GET
  @Path("/anomalies/view")
  public List<MergedAnomalyResultDTO> viewMergedAnomaliesInRange(@NotNull @QueryParam("dataset") String dataset,
      @QueryParam("startTimeIso") String startTimeIso,
      @QueryParam("endTimeIso") String endTimeIso,
      @QueryParam("metric") String metric,
      @QueryParam("dimensions") String exploredDimensions,
      @DefaultValue("true") @QueryParam("applyAlertFilter") boolean applyAlertFiler) {

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
      if (StringUtils.isNotBlank(exploredDimensions)) {
        // Decode dimensions map from request, which may contain encode symbols such as "%20D", etc.
        exploredDimensions = URLDecoder.decode(exploredDimensions, UTF8);
        try {
          // Ensure the dimension names are sorted in order to match the string in backend database
          DimensionMap sortedDimensions = OBJECT_MAPPER.readValue(exploredDimensions, DimensionMap.class);
          exploredDimensions = OBJECT_MAPPER.writeValueAsString(sortedDimensions);
        } catch (IOException e) {
          LOG.warn("exploreDimensions may not be sorted because failed to read it as a json string: {}", e.toString());
        }
      }

      boolean loadRawAnomalies = false;

      if (StringUtils.isNotBlank(metric)) {
        if (StringUtils.isNotBlank(exploredDimensions)) {
          anomalyResults = anomalyMergedResultDAO.findByCollectionMetricDimensionsTime(dataset, metric, exploredDimensions, startTime.getMillis(), endTime.getMillis(), loadRawAnomalies);
        } else {
          anomalyResults = anomalyMergedResultDAO.findByCollectionMetricTime(dataset, metric, startTime.getMillis(), endTime.getMillis(), loadRawAnomalies);
        }
      } else {
        anomalyResults = anomalyMergedResultDAO.findByCollectionTime(dataset, startTime.getMillis(), endTime.getMillis(), loadRawAnomalies);
      }
    } catch (Exception e) {
      LOG.error("Exception in fetching anomalies", e);
    }

    if (applyAlertFiler) {
      // TODO: why need try catch?
      try {
        anomalyResults = AlertFilterHelper.applyFiltrationRule(anomalyResults, alertFilterFactory);
      } catch (Exception e) {
        LOG.warn(
            "Failed to apply alert filters on anomalies for dataset:{}, metric:{}, start:{}, end:{}, exception:{}",
            dataset, metric, startTimeIso, endTimeIso, e);
      }
    }

    return anomalyResults;
  }


//View raw anomalies for collection
 @GET
 @Path("/raw-anomalies/view")
 @Produces(MediaType.APPLICATION_JSON)
 public String viewRawAnomaliesInRange(
     @QueryParam("functionId") String functionId,
     @QueryParam("dataset") String dataset,
     @QueryParam("startTimeIso") String startTimeIso,
     @QueryParam("endTimeIso") String endTimeIso,
     @QueryParam("metric") String metric) throws JsonProcessingException {

   if (StringUtils.isBlank(functionId) && StringUtils.isBlank(dataset)) {
     throw new IllegalArgumentException("must provide dataset or functionId");
   }
   DateTime endTime = DateTime.now();
   if (StringUtils.isNotEmpty(endTimeIso)) {
     endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
   }
   DateTime startTime = endTime.minusDays(7);
   if (StringUtils.isNotEmpty(startTimeIso)) {
     startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
   }

   List<RawAnomalyResultDTO> rawAnomalyResults = new ArrayList<>();
   if (StringUtils.isNotBlank(functionId)) {
     rawAnomalyResults = rawAnomalyResultDAO.
         findAllByTimeAndFunctionId(startTime.getMillis(), endTime.getMillis(), Long.valueOf(functionId));
   } else if (StringUtils.isNotBlank(dataset)) {
     List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByCollection(dataset);
     List<Long> functionIds = new ArrayList<>();
     for (AnomalyFunctionDTO anomalyFunction : anomalyFunctions) {
       if (StringUtils.isNotBlank(metric) && !anomalyFunction.getTopicMetric().equals(metric)) {
         continue;
       }
       functionIds.add(anomalyFunction.getId());
     }
     for (Long id : functionIds) {
       rawAnomalyResults.addAll(rawAnomalyResultDAO.
           findAllByTimeAndFunctionId(startTime.getMillis(), endTime.getMillis(), id));
     }
   }
   String response = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(rawAnomalyResults);
   return response;
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
        if (metric.equals(anomalyFunctionSpec.getTopicMetric())) {
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
    anomalyFunctionSpec.setTopicMetric(metric);
    anomalyFunctionSpec.setMetrics(Arrays.asList(metric));
    if (StringUtils.isEmpty(type)) {
      type = DEFAULT_FUNCTION_TYPE;
    }
    anomalyFunctionSpec.setType(type);
    anomalyFunctionSpec.setWindowSize(Integer.valueOf(windowSize));
    anomalyFunctionSpec.setWindowUnit(TimeUnit.valueOf(windowUnit));

    // Setting window delay time / unit
    TimeUnit dataGranularityUnit = dataGranularity.getUnit();

    // default window delay time = 10 hours
    int windowDelayTime;
    TimeUnit windowDelayTimeUnit;
    switch (dataGranularityUnit) {
    case MINUTES:
      windowDelayTime = 30;
      windowDelayTimeUnit = TimeUnit.MINUTES;
      break;
    case DAYS:
      windowDelayTime = 0;
      windowDelayTimeUnit = TimeUnit.DAYS;
      break;
    case HOURS:
    default:
      windowDelayTime = 10;
      windowDelayTimeUnit = TimeUnit.HOURS;
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
    anomalyFunctionSpec.setTopicMetric(metric);
    anomalyFunctionSpec.setMetrics(Arrays.asList(metric));
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

  /**
   * Apply an autotune configuration to an existing function
   * @param id
   * The id of an autotune configuration
   * @param isCloneFunction
   * Should we clone the function or simply apply the autotune configuration to the existing function
   * @return
   * an activated anomaly detection function
   */
  @POST
  @Path("/anomaly-function/apply/{autotuneConfigId}")
  public Response applyAutotuneConfig(@PathParam("autotuneConfigId") @NotNull long id,
      @QueryParam("cloneFunction") @DefaultValue("false") boolean isCloneFunction,
      @QueryParam("cloneAnomalies") Boolean isCloneAnomalies) {
    Map<String, String> responseMessage = new HashMap<>();
    AutotuneConfigDTO autotuneConfigDTO = autotuneConfigDAO.findById(id);
    if (autotuneConfigDTO == null) {
      responseMessage.put("message", "Cannot find the autotune configuration entry " + id + ".");
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    }
    if (autotuneConfigDTO.getConfiguration() == null || autotuneConfigDTO.getConfiguration().isEmpty()) {
      responseMessage.put("message", "Autotune configuration is null or empty. The original function is optimal. Nothing to change");
      return Response.ok(responseMessage).build();
    }

    if (isCloneAnomalies == null) { // if isCloneAnomalies is not given, assign a default value
      isCloneAnomalies = containsLabeledAnomalies(autotuneConfigDTO.getFunctionId());
    }

    AnomalyFunctionDTO originalFunction = anomalyFunctionDAO.findById(autotuneConfigDTO.getFunctionId());
    AnomalyFunctionDTO targetFunction = originalFunction;

    // clone anomaly function and its anomaly results if requested
    if (isCloneFunction) {
      OnboardResource
          onboardResource = new OnboardResource(anomalyFunctionDAO, mergedAnomalyResultDAO, rawAnomalyResultDAO);
      long cloneId;
      String tag = "clone";
      try {
        cloneId = onboardResource.cloneAnomalyFunctionById(originalFunction.getId(), "clone", isCloneAnomalies);
      } catch (Exception e) {
        responseMessage.put("message", "Unable to clone function " + originalFunction.getId() + " with clone tag \"clone\"");
        LOG.warn("Unable to clone function {} with clone tag \"{}\"", originalFunction.getId(), "clone");
        return Response.status(Response.Status.CONFLICT).entity(responseMessage).build();
      }
      targetFunction = anomalyFunctionDAO.findById(cloneId);
    }

    // Verify if to update alert filter or function configuraions
    // if auto tune method is EXHAUSTIVE, which belongs to function auto tune, need to update function configurations
    // if auto tune method is ALERT_FILTER_LOGISITC_AUTO_TUNE or INITIATE_ALERT_FILTER_LOGISTIC_AUTO_TUNE, alert filter is to be updated
    if (autotuneConfigDTO.getAutotuneMethod() != EXHAUSTIVE) {
      targetFunction.setAlertFilter(autotuneConfigDTO.getConfiguration());
    } else {
      // Update function configuration
      targetFunction.updateProperties(autotuneConfigDTO.getConfiguration());
    }
    targetFunction.setActive(true);
    anomalyFunctionDAO.update(targetFunction);

    // Deactivate original function
    if (isCloneFunction) {
      originalFunction.setActive(false);
      anomalyFunctionDAO.update(originalFunction);
    }

    return Response.ok(targetFunction).build();
  }

  /**
   * Check if the given function contains labeled anomalies
   * @param functionId
   * an id of an anomaly detection function
   * @return
   * true if there are labeled anomalies detected by the function
   */
  private boolean containsLabeledAnomalies(long functionId) {
    List<MergedAnomalyResultDTO> mergedAnomalies = mergedAnomalyResultDAO.findByFunctionId(functionId, true);

    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      AnomalyFeedback feedback = mergedAnomaly.getFeedback();
      if (feedback == null) {
        continue;
      }
      if (feedback.getFeedbackType().equals(AnomalyFeedbackType.ANOMALY) ||
          feedback.getFeedbackType().equals(AnomalyFeedbackType.ANOMALY_NEW_TREND)) {
        return true;
      }
    }
    return false;
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

  /**
   * Show the content of merged anomalies whose start time is located in the given time ranges
   *
   * @param functionId id of the anomaly function
   * @param startTimeIso The start time of the monitoring window
   * @param endTimeIso The start time of the monitoring window
   * @param anomalyType type of anomaly: "raw" or "merged"
   * @param applyAlertFiler can choose apply alert filter when query merged anomaly results
   * @return list of anomalyIds (Long)
   */
  @GET
  @Path("anomaly-function/{id}/anomalies")
  public List<Long> getAnomaliesByFunctionId(@PathParam("id") Long functionId,
      @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso,
      @QueryParam("type") @DefaultValue("merged") String anomalyType,
      @QueryParam("apply-alert-filter") @DefaultValue("false") boolean applyAlertFiler) {

    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if (anomalyFunction == null) {
      LOG.info("Anomaly functionId {} is not found", functionId);
      return null;
    }

    long startTime;
    long endTime;
    try {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
    } catch (Exception e) {
      throw new WebApplicationException("Unable to parse strings, " + startTimeIso + " and " + endTimeIso
          + ", in ISO DateTime format", e);
    }

    LOG.info("Retrieving {} anomaly results for function {} in the time range: {} -- {}",
        anomalyType, functionId, startTimeIso, endTimeIso);

    ArrayList<Long> idList = new ArrayList<>();
    if (anomalyType.equals("raw")) {
      List<RawAnomalyResultDTO> rawDTO = rawAnomalyResultDAO.findAllByTimeAndFunctionId(startTime, endTime, functionId);
      for (RawAnomalyResultDTO dto : rawDTO) {
        idList.add(dto.getId());
      }
    } else if (anomalyType.equals("merged")) {
      List<MergedAnomalyResultDTO> mergedResults = mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(startTime,
          endTime, functionId, true);

      // apply alert filter
      if (applyAlertFiler) {
        try {
          mergedResults = AlertFilterHelper.applyFiltrationRule(mergedResults, alertFilterFactory);
        } catch (Exception e) {
          LOG.warn(
              "Failed to apply alert filters on {} anomalies for function id:{}, start:{}, end:{}, exception:{}",
              anomalyType, functionId, startTimeIso, endTimeIso, e);
        }
      }

      for (MergedAnomalyResultDTO dto : mergedResults) {
        idList.add(dto.getId());
      }
    }

      return idList;
  }

  // Activate anomaly function
  @POST
  @Path("/anomaly-function/activate")
  public Response activateAnomalyFunction(@NotNull @QueryParam("functionId") Long id) {
    if (id == null) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    toggleFunctionById(id, true);
    return Response.ok(id).build();
  }

  // batch activate and deactivate anomaly functions
  @POST
  @Path("/anomaly-functions/activate")
  public String activateFunction(@QueryParam("functionIds") String functionIds) {
    toggleFunctions(functionIds, true);
    return functionIds;
  }

  @POST
  @Path("/anomaly-functions/deactivate")
  public String deactivateFunction(@QueryParam("functionIds") String functionIds) {
    toggleFunctions(functionIds, false);
    return functionIds;
  }

  /**
   * toggle anomaly functions to active and inactive
   *
   * @param functionIds string comma separated function ids, ALL meaning all functions
   * @param isActive boolean true or false, set function as true or false
   */
  private void toggleFunctions(String functionIds, boolean isActive) {
    List<Long> functionIdsList = new ArrayList<>();

    // can add tokens here to activate and deactivate all functions for example
    // functionIds == {SPECIAL TOKENS} --> functionIdsList = anomalyFunctionDAO.findAll()

    if (StringUtils.isNotBlank(functionIds)) {
      String[] tokens = functionIds.split(",");
      for (String token : tokens) {
        functionIdsList.add(Long.valueOf(token));  // unhandled exception is expected
      }
    }

    for (long id : functionIdsList) {
      toggleFunctionById(id, isActive);
    }
  }

  private void toggleFunctionById(long id, boolean isActive) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    anomalyFunctionSpec.setActive(isActive);
    anomalyFunctionDAO.update(anomalyFunctionSpec);
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

    // call endpoint to shutdown if active
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
        rawAnomalyResultDAO.findAllByTimeAndFunctionId(0, System.currentTimeMillis(), id);
    for (RawAnomalyResultDTO result : rawResults) {
      rawAnomalyResultDAO.delete(result);
    }

    // merged anomaly mapping
    List<MergedAnomalyResultDTO> mergedResults = anomalyMergedResultDAO.findByFunctionId(id, true);
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
      AnomalyFeedbackDTO feedbackRequest = OBJECT_MAPPER.readValue(payload, AnomalyFeedbackDTO.class);
      AnomalyFeedback feedback = result.getFeedback();
      if (feedback == null) {
        feedback = new AnomalyFeedbackDTO();
        result.setFeedback(feedback);
      }
      feedback.setComment(feedbackRequest.getComment());
      feedback.setFeedbackType(feedbackRequest.getFeedbackType());
      anomalyMergedResultDAO.updateAnomalyFeedback(result);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid payload " + payload, e);
    }
  }

  @POST
  @Path(value = "anomaly-result/feedback/{anomaly_result_id}")
  public void updateAnomalyResultFeedback(@PathParam("anomaly_result_id") long anomalyResultId, String payload) {
    try {
      RawAnomalyResultDTO result = rawAnomalyResultDAO.findById(anomalyResultId);
      if (result == null) {
        throw new IllegalArgumentException("AnomalyResult not found with id " + anomalyResultId);
      }
      AnomalyFeedbackDTO feedbackRequest = OBJECT_MAPPER.readValue(payload, AnomalyFeedbackDTO.class);
      AnomalyFeedbackDTO feedback = result.getFeedback();
      if (feedback == null) {
        feedback = new AnomalyFeedbackDTO();
        result.setFeedback(feedback);
      }
      feedback.setComment(feedbackRequest.getComment());
      feedback.setFeedbackType(feedbackRequest.getFeedbackType());

      rawAnomalyResultDAO.update(result);
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

    boolean loadRawAnomalies = false;
    MergedAnomalyResultDTO anomalyResult = anomalyMergedResultDAO.findById(anomalyResultId, loadRawAnomalies);
    Map<String, String> anomalyProps = anomalyResult.getProperties();

    AnomalyTimelinesView anomalyTimelinesView = null;

    // check if there is AnomalyTimelinesView in the Properties. If yes, use the AnomalyTimelinesView
    if(anomalyProps.containsKey("anomalyTimelinesView")) {
      anomalyTimelinesView = AnomalyTimelinesView.fromJsonString(
          anomalyProps.get("anomalyTimelinesView"));
    } else {

      DimensionMap dimensions = anomalyResult.getDimensions();
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

      TimeGranularity timeGranularity = Utils.getAggregationTimeGranularity(aggTimeGranularity, anomalyFunctionSpec.getCollection());
      long bucketMillis = timeGranularity.toMillis();
      // ThirdEye backend is end time exclusive, so one more bucket is appended to make end time inclusive for frontend.
      viewWindowEndTime += bucketMillis;

      long maxDataTime = collectionMaxDataTimeCache.get(anomalyResult.getCollection());
      if (viewWindowEndTime > maxDataTime) {
        viewWindowEndTime = (anomalyResult.getEndTime() > maxDataTime) ? anomalyResult.getEndTime() : maxDataTime;
      }

      DateTime viewWindowStart = new DateTime(viewWindowStartTime);
      DateTime viewWindowEnd = new DateTime(viewWindowEndTime);
      AnomalyDetectionInputContextBuilder anomalyDetectionInputContextBuilder =
          new AnomalyDetectionInputContextBuilder(anomalyFunctionFactory);
      anomalyDetectionInputContextBuilder.setFunction(anomalyFunctionSpec)
          .fetchTimeSeriesDataByDimension(viewWindowStart, viewWindowEnd, dimensions, false)
          .fetchScalingFactors(viewWindowStart, viewWindowEnd)
          .fetchExistingMergedAnomalies(viewWindowStart, viewWindowEnd, false);

      AnomalyDetectionInputContext adInputContext = anomalyDetectionInputContextBuilder.build();

      MetricTimeSeries metricTimeSeries = adInputContext.getDimensionMapMetricTimeSeriesMap().get(dimensions);

      if (metricTimeSeries == null) {
        // If this case happened, there was something wrong with anomaly detection because we are not able to retrieve
        // the timeseries for the given anomaly
        return new AnomalyTimelinesView();
      }

      // Transform time series with scaling factor
      List<ScalingFactor> scalingFactors = adInputContext.getScalingFactors();
      if (CollectionUtils.isNotEmpty(scalingFactors)) {
        Properties properties = anomalyFunction.getProperties();
        MetricTransfer.rescaleMetric(metricTimeSeries, viewWindowStartTime, scalingFactors, anomalyFunctionSpec.getTopicMetric(), properties);
      }

      List<MergedAnomalyResultDTO> knownAnomalies = adInputContext.getKnownMergedAnomalies().get(dimensions);
      // Known anomalies are ignored (the null parameter) because 1. we can reduce users' waiting time and 2. presentation
      // data does not need to be as accurate as the one used for detecting anomalies
      anomalyTimelinesView =
          anomalyFunction.getTimeSeriesView(metricTimeSeries, bucketMillis, anomalyFunctionSpec.getTopicMetric(),
              viewWindowStartTime, viewWindowEndTime, knownAnomalies);
    }

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

  @GET
  @Path("/external-dashboard-url/{mergedAnomalyId}")
  public String getExternalDashboardUrlForMergedAnomaly(@NotNull @PathParam("mergedAnomalyId") Long mergedAnomalyId)
      throws Exception {

    MergedAnomalyResultDTO mergedAnomalyResultDTO = mergedAnomalyResultDAO.findById(mergedAnomalyId);
    String metric = mergedAnomalyResultDTO.getMetric();
    String dataset = mergedAnomalyResultDTO.getCollection();
    Long startTime = mergedAnomalyResultDTO.getStartTime();
    Long endTime = mergedAnomalyResultDTO.getEndTime();
    MetricConfigDTO metricConfigDTO = metricConfigDAO.findByMetricAndDataset(metric, dataset);

    Map<String, String> context = new HashMap<>();
    context.put(MetricConfigBean.URL_TEMPLATE_START_TIME, String.valueOf(startTime));
    context.put(MetricConfigBean.URL_TEMPLATE_END_TIME, String.valueOf(endTime));
    StrSubstitutor strSubstitutor = new StrSubstitutor(context);
    Map<String, String> urlTemplates = metricConfigDTO.getExtSourceLinkInfo();
    for (Entry<String, String> entry : urlTemplates.entrySet()) {
      String sourceName = entry.getKey();
      String urlTemplate = entry.getValue();
      String extSourceUrl = strSubstitutor.replace(urlTemplate);
      urlTemplates.put(sourceName, extSourceUrl);
    }
    return new JSONObject(urlTemplates).toString();
  }

}
