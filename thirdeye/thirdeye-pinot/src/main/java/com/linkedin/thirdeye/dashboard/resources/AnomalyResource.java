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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.onboard.utils.FunctionCreationUtils;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.api.Constants;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.resources.v2.AnomaliesResource;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.email.filter.BaseAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import com.linkedin.thirdeye.util.TimeSeriesUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
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
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.Weeks;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import static com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType.*;


@Path(value = "/dashboard")
@Api(tags = { Constants.ANOMALY_TAG })
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
  private AlertConfigManager emailConfigurationDAO;
  private MetricConfigManager metricConfigDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private AutotuneConfigManager autotuneConfigDAO;
  private DatasetConfigManager datasetConfigDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AlertFilterFactory alertFilterFactory;
  private AlertFilterAutotuneFactory alertFilterAutotuneFactory;
  private LoadingCache<String, Long> collectionMaxDataTimeCache;
  private LoadingCache<String, String> dimensionFiltersCache;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public AnomalyResource(AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory,
      AlertFilterAutotuneFactory alertFilterAutotuneFactory) {
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.emailConfigurationDAO = DAO_REGISTRY.getAlertConfigDAO();
    this.metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.autotuneConfigDAO = DAO_REGISTRY.getAutotuneConfigDAO();
    this.datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    this.alertFilterFactory = alertFilterFactory;
    this.alertFilterAutotuneFactory = alertFilterAutotuneFactory;
    this.collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getDatasetMaxDataTimeCache();
    this.dimensionFiltersCache = CACHE_REGISTRY_INSTANCE.getDimensionFiltersCache();
  }

  /************** CRUD for anomalies of a collection ********************************************************/
  @GET
  @Path("/anomalies/view/{anomaly_merged_result_id}")
  @ApiOperation(value = "Get anomalies")
  public MergedAnomalyResultDTO getMergedAnomalyDetail(
      @NotNull @PathParam("anomaly_merged_result_id") long mergedAnomalyId) {
    return anomalyMergedResultDAO.findById(mergedAnomalyId);
  }

  // View merged anomalies for collection
  @GET
  @Path("/anomalies/view")
  @ApiOperation(value = "View merged anomalies for collection")
  public List<MergedAnomalyResultDTO> viewMergedAnomaliesInRange(
      @NotNull @QueryParam("dataset") String dataset,
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
          anomalyResults =
              anomalyMergedResultDAO.findByCollectionMetricDimensionsTime(dataset, metric, exploredDimensions,
                  startTime.getMillis(), endTime.getMillis());
        } else {
          anomalyResults = anomalyMergedResultDAO.findByCollectionMetricTime(dataset, metric, startTime.getMillis(),
              endTime.getMillis());
        }
      } else {
        anomalyResults =
            anomalyMergedResultDAO.findByCollectionTime(dataset, startTime.getMillis(), endTime.getMillis());
      }
    } catch (Exception e) {
      LOG.error("Exception in fetching anomalies", e);
    }

    if (applyAlertFiler) {
      // TODO: why need try catch?
      try {
        anomalyResults = AlertFilterHelper.applyFiltrationRule(anomalyResults, alertFilterFactory);
      } catch (Exception e) {
        LOG.warn("Failed to apply alert filters on anomalies for dataset:{}, metric:{}, start:{}, end:{}, exception:{}",
            dataset, metric, startTimeIso, endTimeIso, e);
      }
    }

    return anomalyResults;
  }

  // Get anomaly score
  @GET
  @ApiOperation(value = "Get anomaly score")
  @Path("/anomalies/score/{anomaly_merged_result_id}")
  public double getAnomalyScore(
      @NotNull @PathParam("anomaly_merged_result_id") long mergedAnomalyId) {
    MergedAnomalyResultDTO mergedAnomaly = anomalyMergedResultDAO.findById(mergedAnomalyId);
    BaseAlertFilter alertFilter = new DummyAlertFilter();
    if (mergedAnomaly != null) {
      AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(mergedAnomaly.getFunctionId());
      alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    }
    return alertFilter.getProbability(mergedAnomaly);
  }

  /************* CRUD for anomaly functions of collection **********************************************/
  // View all anomaly functions
  @GET
  @Path("/anomaly-function")
  @ApiOperation(value = "View all anomaly functions")
  public List<AnomalyFunctionDTO> viewAnomalyFunctions(
      @NotNull @QueryParam("dataset") String dataset,
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

  /**
   * Endpoint to be used for creating new anomaly function
   * @param dataset name of dataset for the new anomaly function
   * @param functionName name of the new anomaly function. Should follow convention "productName_metricName_dimensionName_other"
   * @param metric name of metric on Thirdeye of the new anomaly function
   * @param metric_function was using as a consolidation function, now by default use "SUM"
   * @param type type of anomaly function. Minutely metrics use SIGN_TEST_VANILLA,
   *             Hourly metrics use REGRESSION_GAUSSIAN_SCAN, Daily metrics use SPLINE_REGRESSION
   * @param windowSize Detection window size
   * @param windowUnit Detection window unit
   * @param windowDelay Detection window delay (wait for data completeness)
   * @param windowDelayUnit Detection window delay unit
   * @param cron cron of detection
   * @param exploreDimensions explore dimensions in JSON
   * @param filters filters on dimensions
   * @param userInputDataGranularity user input metric granularity, if null uses dataset granularity
   * @param properties properties for anomaly function
   * @param isActive whether set the new anomaly function to be active or not
   * @return new anomaly function Id if successfully create new anomaly function
   * @throws Exception
   */
  @POST
  @Path("/anomaly-function")
  @ApiOperation("Endpoint to be used for creating new anomaly function")
  public Response createAnomalyFunction(@NotNull @QueryParam("dataset") String dataset,
      @NotNull @QueryParam("functionName") String functionName, @NotNull @QueryParam("metric") String metric,
      @NotNull @QueryParam("metricFunction") String metric_function, @QueryParam("type") String type,
      @NotNull @QueryParam("windowSize") String windowSize, @NotNull @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") @DefaultValue("0") String windowDelay, @QueryParam("cron") String cron,
      @QueryParam("windowDelayUnit") String windowDelayUnit, @QueryParam("exploreDimension") String exploreDimensions,
      @QueryParam("filters") String filters, @QueryParam("dataGranularity") String userInputDataGranularity,
      @NotNull @QueryParam("properties") String properties, @QueryParam("isActive") boolean isActive) throws Exception {
    if (StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionName) || StringUtils.isEmpty(metric)
        || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit) || properties == null) {
      throw new IllegalArgumentException(String.format("Received nulll or emtpy String for one of the mandatory params: "
          + "dataset: %s, metric: %s, functionName %s, windowSize: %s, windowUnit: %s, and properties: %s", dataset,
          metric, functionName, windowSize, windowUnit, properties));
    }

    TimeGranularity dataGranularity;
    DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
    if (datasetConfig == null) {
      throw new IllegalArgumentException(String.format("No entry with dataset name %s exists", dataset));
    }
    if (userInputDataGranularity == null) {
      TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
      dataGranularity = timespec.getDataGranularity();
    } else {
      dataGranularity = TimeGranularity.fromString(userInputDataGranularity);
    }

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

    // default windowDelay = 0, can be adjusted to cope with expected delay for given dataset/metric
    int windowDelayTime = Integer.valueOf(windowDelay);
    TimeUnit windowDelayTimeUnit;
    switch (dataGranularityUnit) {
      case MINUTES:
        windowDelayTimeUnit = TimeUnit.MINUTES;
        break;
      case DAYS:
        windowDelayTimeUnit = TimeUnit.DAYS;
        break;
      case HOURS:
      default:
        windowDelayTimeUnit = TimeUnit.HOURS;
    }
    if (StringUtils.isNotBlank(windowDelayUnit)) {
      windowDelayTimeUnit = TimeUnit.valueOf(windowDelayUnit.toUpperCase());
    }
    anomalyFunctionSpec.setWindowDelayUnit(windowDelayTimeUnit);
    anomalyFunctionSpec.setWindowDelay(windowDelayTime);

    // setup detection frequency if it's minutely function
    // the default frequency in AnomalyDetectionFunctionBean is 1 hour
    // when detection job is scheduled, the frequency may also be modified by data granularity
    // this frequency is a override to allow longer time period (lower frequency) and reduce system load
    if (dataGranularity.getUnit().equals(TimeUnit.MINUTES)) {
      TimeGranularity frequency = new TimeGranularity(15, TimeUnit.MINUTES);
      anomalyFunctionSpec.setFrequency(frequency);
    }

    // bucket size and unit are defaulted to the collection granularity
    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());

    if (StringUtils.isNotEmpty(exploreDimensions)) {
      anomalyFunctionSpec.setExploreDimensions(FunctionCreationUtils.getDimensions(datasetConfig, exploreDimensions));
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
  @ApiOperation("Apply an autotune configuration to an existing function")
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
      responseMessage.put("message",
          "Autotune configuration is null or empty. The original function is optimal. Nothing to change");
      return Response.ok(responseMessage).build();
    }

    if (isCloneAnomalies == null) { // if isCloneAnomalies is not given, assign a default value
      isCloneAnomalies = containsLabeledAnomalies(autotuneConfigDTO.getFunctionId());
    }

    AnomalyFunctionDTO originalFunction = anomalyFunctionDAO.findById(autotuneConfigDTO.getFunctionId());
    AnomalyFunctionDTO targetFunction = originalFunction;

    // clone anomaly function and its anomaly results if requested
    if (isCloneFunction) {
      OnboardResource onboardResource = new OnboardResource(anomalyFunctionDAO, mergedAnomalyResultDAO);
      long cloneId;
      String tag = "clone";
      try {
        cloneId = onboardResource.cloneAnomalyFunctionById(originalFunction.getId(), "clone", isCloneAnomalies);
      } catch (Exception e) {
        responseMessage.put("message",
            "Unable to clone function " + originalFunction.getId() + " with clone tag \"clone\"");
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
   * Get the timeseries with function baseline for an anomaly function.
   * The function baseline can be 1) the baseline for online analysis, including baseline in training and testing range,
   * and 2) the baseline for offline analysis, where only training time rage is included.
   *
   * Online Analysis
   *  This mode take the monitoring window as the testing window; the training time range is requested based on the
   *  given testing window. The function returns the baseline and observed values in both testing and training window.
   *  Note that, different from offline analysis, the size of the training window is fixed. Users are able to trim the
   *  window, but not extend.
   *
   * Offline Analysis
   *  This mode take the monitoring window as the training window, while the testing window is empty. The function only
   *  fits the time series in training window. The fitted time series is then returned.
   *
   * For the data points removed because of anomalies, the second part of this function will make up the time series by
   * the information provided by the anomalies.
   *
   * The baseline is generated from getTimeSeriesView() in each detection function.
   * @param functionId
   *      the target anomaly function
   * @param startTimeIso
   *      the start time of the monitoring window (inclusive)
   * @param endTimeIso
   *      the end time of the monitoring window (exclusive)
   * @param mode
   *      the mode of the baseline calculation
   * @param dimension
   *      the dimension string in JSON format
   * @return
   *      the Map that maps dimension string to the AnomalyTimelinesView
   * @throws Exception
   */
  private static final Period DEFAULT_MINIMUM_REQUIRE_TRAINING_PERIOD = Weeks.weeks(8).toPeriod();
  @GET
  @Path(value = "/anomaly-function/{id}/baseline")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get the timeseries with function baseline for an anomaly function")
  public Response getTimeSeriesAndBaselineData(@PathParam("id") long functionId,
      @QueryParam("start") String startTimeIso, @QueryParam("end") String endTimeIso,
      @QueryParam("mode") @DefaultValue("ONLINE") String mode,
      @QueryParam("dimension") @NotNull String dimension) throws Exception {
    AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(functionId);
    if (anomalyFunctionSpec == null) {
      LOG.warn("Cannot find anomaly function {}", functionId);
      return Response.status(Response.Status.BAD_REQUEST).entity("Cannot find anomaly function " + functionId).build();
    }

    TimeGranularity bucketTimeGranularity =
        new TimeGranularity(anomalyFunctionSpec.getBucketSize(), anomalyFunctionSpec.getBucketUnit());
    // If startTime is not assigned, use the max date time as the start time
    DateTime startTime = new DateTime(collectionMaxDataTimeCache.get(anomalyFunctionSpec.getCollection()));
    DateTime endTime = startTime.plus(bucketTimeGranularity.toPeriod());
    if (StringUtils.isNotBlank(startTimeIso)) {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    }
    if (StringUtils.isNotBlank(endTimeIso)) {
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
    }

    if (startTime.isAfter(endTime)) {
      LOG.warn("Start time {} is after end time {}", startTime, endTime);
      return Response.status(Response.Status.BAD_REQUEST).entity("Start time is after end time").build();
    }

    // enlarge view window when is view window is small
    if (!mode.equalsIgnoreCase("ONLINE")) {
      switch (anomalyFunctionSpec.getBucketUnit()) {
        case MINUTES:
          mode = "ONLINE"; // There is no offline training in minute-level detection, use online in all cases
          break;
        case HOURS:
          if (Hours.hoursBetween(startTime, endTime).getHours() < DEFAULT_MINIMUM_REQUIRE_TRAINING_PERIOD.toStandardDuration().getStandardHours()) {
            startTime = startTime.minus(DEFAULT_MINIMUM_REQUIRE_TRAINING_PERIOD);
          }
          break;
        case DAYS:
          if (Days.daysBetween(startTime, endTime).getDays() < DEFAULT_MINIMUM_REQUIRE_TRAINING_PERIOD.toStandardDuration().getStandardDays()) {
            startTime = startTime.minus(DEFAULT_MINIMUM_REQUIRE_TRAINING_PERIOD);
          }
          break;
      }
    }

    // parse dimensions to explore
    List<String> exploreDimensions = Collections.emptyList();
    if (!StringUtils.isBlank(anomalyFunctionSpec.getExploreDimensions())) {
      exploreDimensions = Arrays.asList(anomalyFunctionSpec.getExploreDimensions().split(","));
    }

    // Get DimensionMap from input dimension String
    Map<String, String> inputDimension = OBJECT_MAPPER.readValue(dimension, Map.class);

    // populate pre-aggregated dimension values, if necessary
    DatasetConfigDTO datasetConfigDTO = this.datasetConfigDAO.findByDataset(anomalyFunctionSpec.getCollection());
    if (datasetConfigDTO != null && !datasetConfigDTO.isAdditive()) {
      for (String dimName : exploreDimensions) {
        if (!inputDimension.containsKey(dimName) &&
            !datasetConfigDTO.getDimensionsHaveNoPreAggregation().contains(dimName)) {
          inputDimension.put(dimName, datasetConfigDTO.getPreAggregatedKeyword());
        }
      }
    }
    DateTimeZone timeZone = DateTimeZone.forID(datasetConfigDTO.getTimezone());

    DimensionMap dimensionsToBeEvaluated = new DimensionMap();
    if (anomalyFunctionSpec.getExploreDimensions() != null) {
      for (String exploreDimension : exploreDimensions) {
        if (!inputDimension.containsKey(exploreDimension)) {
          String msg =
              String.format("Query %s doesn't specify the value of explore dimension %s", dimension, exploreDimension);
          LOG.error(msg);
          throw new WebApplicationException(msg);
        } else {
          dimensionsToBeEvaluated.put(exploreDimension, inputDimension.get(exploreDimension));
        }
      }
    }

    // Get the anomaly time lines view of the anomaly function on each dimension
    AnomaliesResource anomaliesResource = new AnomaliesResource(anomalyFunctionFactory, alertFilterFactory);
    AnomalyTimelinesView anomalyTimelinesView = null;
    try {
      if (mode.equalsIgnoreCase("ONLINE")) {
        // If online, use the monitoring time window to query time range and generate the baseline
        anomalyTimelinesView =
            anomaliesResource.getTimelinesViewInMonitoringWindow(anomalyFunctionSpec, datasetConfigDTO, startTime,
                endTime, dimensionsToBeEvaluated);
      } else {
        // If offline, request baseline in user-defined data range
        List<Pair<Long, Long>> dataRangeIntervals = new ArrayList<>();
        // Assign the view window as training window and mock window with end time as test window
        dataRangeIntervals.add(new Pair<Long, Long>(endTime.getMillis(),
            endTime.plus(bucketTimeGranularity.toPeriod()).getMillis()));
        dataRangeIntervals.add(new Pair<Long, Long>(startTime.getMillis(), endTime.getMillis()));
        anomalyTimelinesView =
            anomaliesResource.getTimelinesViewInMonitoringWindow(anomalyFunctionSpec, datasetConfigDTO,
                dataRangeIntervals, dimensionsToBeEvaluated);
      }
    } catch (Exception e) {
      LOG.warn("Unable to generate baseline for given anomaly function: {}", functionId, e);
      // Return no content with empty time line view upon exception
      return Response.status(Response.Status.NO_CONTENT).entity(new AnomalyTimelinesView()).build();
    }

    if (TimeUnit.DAYS.equals(anomalyFunctionSpec.getBucketUnit())) {
      // align timestamp to the start of the day
      List<TimeBucket> timeBuckets = anomalyTimelinesView.getTimeBuckets();
      for (int i = 0; i < timeBuckets.size(); i++) {
        TimeBucket timeBucket = timeBuckets.get(i);
        timeBucket.setCurrentStart(alignToStartOfTheDay(timeBucket.getCurrentStart(), timeZone).getMillis());
        timeBucket.setCurrentEnd(alignToStartOfTheDay(timeBucket.getCurrentEnd(), timeZone).getMillis());
        timeBucket.setBaselineStart(alignToStartOfTheDay(timeBucket.getBaselineStart(), timeZone).getMillis());
        timeBucket.setBaselineEnd(alignToStartOfTheDay(timeBucket.getBaselineEnd(), timeZone).getMillis());
      }
    }

    anomalyTimelinesView = amendAnomalyTimelinesViewWithAnomalyResults(anomalyFunctionSpec, anomalyTimelinesView,
        dimensionsToBeEvaluated, timeZone);

    return Response.ok(anomalyTimelinesView).build();
  }

  public static DateTime alignToStartOfTheDay(long timestamp, DateTimeZone timeZone) {
    DateTime dateTime = new DateTime(timestamp, timeZone);
    DateTime startOfTheDay = dateTime.withTimeAtStartOfDay();
    DateTime startOfNextDay = dateTime.plusDays(1).withTimeAtStartOfDay();
    long diffBetweenStartOfTheDate = Math.abs(dateTime.getMillis() - startOfTheDay.getMillis());
    long diffBetweenStartOfNextDate = Math.abs(dateTime.getMillis() - startOfNextDay.getMillis());
    if (diffBetweenStartOfTheDate < diffBetweenStartOfNextDate) {
      dateTime = startOfTheDay;
    } else {
      dateTime = startOfNextDay;
    }

    return dateTime;
  }

  /**
   * Amend the anomaly time lines view with the information in anomaly results
   *  - if the anomaly result contains view, override the timeseries by the view
   *  - if the anomaly result contains raw anomalies, override the timeseries by the current and baseline values inside
   *  - otherwise, use the merged anomaly information for all missing points
   * @param anomalyFunctionSpec
   * @param originalTimelinesView
   * @param dimensionMap
   * @return
   */
  public AnomalyTimelinesView amendAnomalyTimelinesViewWithAnomalyResults(AnomalyFunctionDTO anomalyFunctionSpec,
      AnomalyTimelinesView originalTimelinesView, DimensionMap dimensionMap, DateTimeZone timeZone) {
    if (originalTimelinesView == null || anomalyFunctionSpec == null) {
      LOG.error("AnomalyTimelinesView or AnomalyFunctionDTO is null");
      return null;
    }
    long bucketMillis =
        (new TimeGranularity(anomalyFunctionSpec.getBucketSize(), anomalyFunctionSpec.getBucketUnit())).toMillis();
    Tuple2<TimeSeries, TimeSeries> timeSeriesTuple = TimeSeriesUtils.toTimeSeries(originalTimelinesView);
    TimeSeries observed = timeSeriesTuple._1();
    TimeSeries expected = timeSeriesTuple._2();
    Interval timeSeriesInterval = observed.getTimeSeriesInterval();
    List<MergedAnomalyResultDTO> mergedAnomalyResults =
        mergedAnomalyResultDAO.findOverlappingByFunctionIdDimensions(anomalyFunctionSpec.getId(),
            timeSeriesInterval.getStartMillis(), timeSeriesInterval.getEndMillis(), dimensionMap.toString());

    for (MergedAnomalyResultDTO mergedAnomalyResult : mergedAnomalyResults) {
      if (mergedAnomalyResult.getDimensions().equals(dimensionMap)) {
        String viewString = mergedAnomalyResult.getProperties().get("anomalyTimelinesView");
        // Strategy 1: override the timeseries by the view
        if (StringUtils.isNotBlank(viewString)) { // Update TimeSeries using the view in the anomaly result
          AnomalyTimelinesView anomalyView;
          try {
            anomalyView = AnomalyTimelinesView.fromJsonString(viewString);
          } catch (Exception e) {
            LOG.warn("Unable to fetch view from anomaly result; use anomaly result directly");
            break;
          }
          timeSeriesTuple = TimeSeriesUtils.toTimeSeries(anomalyView);
          TimeSeries anomalyObserved = timeSeriesTuple._1();
          TimeSeries anomalyExpected = timeSeriesTuple._2();

          for (long timestamp : anomalyObserved.timestampSet()) {
            // Preventing the append data points outside of view window
            if (!timeSeriesInterval.contains(timestamp)) {
              continue;
            }
            // align timestamp to the begin of the day if Bucket is in DAYS
            long indexTimestamp = timestamp;
            if (TimeUnit.DAYS.toMillis(1l) == bucketMillis) {
              timestamp = alignToStartOfTheDay(timestamp, timeZone).getMillis();
            }
            if (timeSeriesInterval.contains(timestamp)) {
              observed.set(timestamp, anomalyObserved.get(indexTimestamp));
              expected.set(timestamp, anomalyExpected.get(indexTimestamp));
            }
          }
          continue;
        }

        // Strategy 2: use the merged anomaly information for all missing points
        for (long start = mergedAnomalyResult.getStartTime(); start < mergedAnomalyResult.getEndTime();
            start += bucketMillis) {
          if (!observed.hasTimestamp(mergedAnomalyResult.getStartTime())) {
            // if the observed value in the timeseries is not removed, use the original observed value
            observed.set(start, mergedAnomalyResult.getAvgCurrentVal());
          }
          if (!expected.hasTimestamp(mergedAnomalyResult.getStartTime())) {
            // if the expected value in the timeserie is not removed, use the original expected value
            expected.set(start, mergedAnomalyResult.getAvgBaselineVal());
          }
        }
      }
    }
    return TimeSeriesUtils.toAnomalyTimeLinesView(observed, expected, bucketMillis);
  }

  /**
   * Check if the given function contains labeled anomalies
   * @param functionId
   * an id of an anomaly detection function
   * @return
   * true if there are labeled anomalies detected by the function
   */
  private boolean containsLabeledAnomalies(long functionId) {
    List<MergedAnomalyResultDTO> mergedAnomalies = mergedAnomalyResultDAO.findByFunctionId(functionId);

    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      AnomalyFeedback feedback = mergedAnomaly.getFeedback();
      if (feedback == null) {
        continue;
      }
      if (feedback.getFeedbackType().isAnomaly()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Show the content of merged anomalies whose start time is located in the given time ranges
   *
   * @param functionId id of the anomaly function
   * @param startTimeIso The start time of the monitoring window
   * @param endTimeIso The start time of the monitoring window
   * @param applyAlertFiler can choose apply alert filter when query merged anomaly results
   * @return list of anomalyIds (Long)
   */
  @GET
  @Path("anomaly-function/{id}/anomalies")
  @ApiOperation("Show the content of merged anomalies whose start time is located in the given time ranges")
  public List<Long> getAnomaliesByFunctionId(@PathParam("id") Long functionId, @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso,
      @QueryParam("apply-alert-filter") @DefaultValue("false") boolean applyAlertFiler,
      @QueryParam("useNotified") @DefaultValue("false") boolean useNotified) {

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
      throw new WebApplicationException(
          "Unable to parse strings, " + startTimeIso + " and " + endTimeIso + ", in ISO DateTime format", e);
    }

    LOG.info("Retrieving anomaly results for function {} in the time range: {} -- {}", functionId, startTimeIso,
        endTimeIso);

    ArrayList<Long> anomalyIdList = new ArrayList<>();
    List<MergedAnomalyResultDTO> mergedResults =
        mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(startTime, endTime, functionId);

    // apply alert filter
    if (applyAlertFiler) {
      try {
        mergedResults = AlertFilterHelper.applyFiltrationRule(mergedResults, alertFilterFactory);
      } catch (Exception e) {
        LOG.warn("Failed to apply alert filters on anomalies of function id:{}, start:{}, end:{}.",
            functionId, startTimeIso, endTimeIso, e);
      }
    }

    for (MergedAnomalyResultDTO mergedAnomaly : mergedResults) {
      // if use notified flag, only keep anomalies isNotified == true
      if ((useNotified && mergedAnomaly.isNotified()) || !useNotified
          || AnomalyResultSource.USER_LABELED_ANOMALY.equals(mergedAnomaly.getAnomalyResultSource())) {
        anomalyIdList.add(mergedAnomaly.getId());
      }
    }

    return anomalyIdList;
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
  @Deprecated
  @DELETE
  @Path("/anomaly-function")
  @ApiOperation(value = "Delete anomaly function")
  public Response deleteAnomalyFunctions(
      @NotNull @QueryParam("id") Long id,
      @QueryParam("functionName") String functionName) throws IOException {

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
    List<AlertConfigDTO> emailConfigurations = emailConfigurationDAO.findByFunctionId(id);
    for (AlertConfigDTO emailConfiguration : emailConfigurations) {
      emailConfiguration.getEmailConfig().getFunctionIds().remove(anomalyFunctionSpec.getId());
      emailConfigurationDAO.update(emailConfiguration);
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

  /**
   * Delete multiple anomaly functions
   *
   * @param ids a string containing multiple anomaly function ids, separated by comma (e.g. f1,f2,f3)
   * @return HTTP response of this request with the deletion status and skipped warnings
   */
  @DELETE
  @Path("/delete-functions")
  @ApiOperation(value = "Delete anomaly functions")
  public Response deleteAnomalyFunctions(@NotNull @QueryParam("ids") String ids) {
    Map<String, String> responseMessage = new HashMap<>();
    List<String> idsDeleted = new ArrayList<>();
    if (StringUtils.isEmpty(ids)) {
      throw new IllegalArgumentException("ids is a required query param");
    }
    String[] functionIds = ids.split(",");

    for (String idString : functionIds) {
      idString = idString.trim();
      Long id = Long.parseLong(idString);
      AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
      if (anomalyFunctionSpec != null) {
        // Remove function from subscription alert groups
        List<AlertConfigDTO> emailConfigurations = emailConfigurationDAO.findByFunctionId(id);
        for (AlertConfigDTO emailConfiguration : emailConfigurations) {
          emailConfiguration.getEmailConfig().getFunctionIds().remove(anomalyFunctionSpec.getId());
          emailConfigurationDAO.update(emailConfiguration);
        }

        // Delete merged anomalies
        List<MergedAnomalyResultDTO> mergedResults = anomalyMergedResultDAO.findByFunctionId(id);
        for (MergedAnomalyResultDTO result : mergedResults) {
          anomalyMergedResultDAO.delete(result);
        }

        anomalyFunctionDAO.deleteById(id);
        idsDeleted.add(idString);
      } else {
        responseMessage.put("warnings", "true");
        responseMessage.put("id: " + id, "skipped! anomaly function doesn't exist.");
      }
    }

    responseMessage.put("message", "successfully deleted the following anomalies. function ids = " + idsDeleted);
    return Response.ok(responseMessage).build();
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
  @ApiOperation("update anomaly merged result feedback")
  public void updateAnomalyMergedResultFeedback(@PathParam("anomaly_merged_result_id") long anomalyResultId,
      String payload) {
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
      if (feedbackRequest.getFeedbackType() != null){
        feedback.setFeedbackType(feedbackRequest.getFeedbackType());
      }
      anomalyMergedResultDAO.updateAnomalyFeedback(result);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid payload " + payload, e);
    }
  }
}