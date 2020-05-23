/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.apache.pinot.thirdeye.anomaly.alert.util.AlertFilterHelper;
import org.apache.pinot.thirdeye.anomaly.views.AnomalyTimelinesView;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.anomalydetection.context.TimeSeries;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.AnomaliesResource;
import org.apache.pinot.thirdeye.dashboard.views.TimeBucket;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;
import org.apache.pinot.thirdeye.detector.email.filter.BaseAlertFilter;
import org.apache.pinot.thirdeye.detector.email.filter.DummyAlertFilter;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;
import org.apache.pinot.thirdeye.util.TimeSeriesUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


@Path(value = "/dashboard")
@Api(tags = { Constants.ANOMALY_TAG })
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyResource.class);
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String UTF8 = "UTF-8";

  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager anomalyMergedResultDAO;
  private AlertConfigManager emailConfigurationDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private DatasetConfigManager datasetConfigDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AlertFilterFactory alertFilterFactory;
  private LoadingCache<String, Long> collectionMaxDataTimeCache;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public AnomalyResource(AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory) {
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.emailConfigurationDAO = DAO_REGISTRY.getAlertConfigDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    this.alertFilterFactory = alertFilterFactory;
    this.collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getDatasetMaxDataTimeCache();
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
   * @param payload         : Json payload containing feedback @see org.apache.pinot.thirdeye.constant.AnomalyFeedbackType
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