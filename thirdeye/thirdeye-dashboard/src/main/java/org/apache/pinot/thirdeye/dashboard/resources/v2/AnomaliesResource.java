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

package org.apache.pinot.thirdeye.dashboard.resources.v2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.apache.pinot.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import org.apache.pinot.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import org.apache.pinot.thirdeye.anomaly.utils.AnomalyUtils;
import org.apache.pinot.thirdeye.anomaly.views.AnomalyTimelinesView;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.common.metric.MetricTimeSeries;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeRange;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.AnomaliesWrapper;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.AnomalyDetails;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.SearchFilters;
import org.apache.pinot.thirdeye.dashboard.views.TimeBucket;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;
import org.apache.pinot.thirdeye.detector.function.BaseAnomalyFunction;
import org.apache.pinot.thirdeye.detector.metric.transfer.MetricTransfer;
import org.apache.pinot.thirdeye.detector.metric.transfer.ScalingFactor;
import org.apache.pinot.thirdeye.util.AnomalyOffset;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class AnomaliesResource {
  private static final String COL_CURRENT = "current";
  private static final String COL_BASELINE = "baseline";

  private static final Logger LOG = LoggerFactory.getLogger(AnomaliesResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String START_END_DATE_FORMAT_DAYS = "MMM d yyyy";
  private static final String START_END_DATE_FORMAT_HOURS = "MMM d yyyy HH:mm";
  private static final String TIME_SERIES_DATE_FORMAT = "yyyy-MM-dd HH:mm";
  private static final int DEFAULT_PAGE_SIZE = 10;
  private static final int NUM_EXECS = 40;
  private static final DateTimeZone DEFAULT_DASHBOARD_TIMEZONE = DateTimeZone.forID("America/Los_Angeles");
  private static final DateTimeFormatter  timeSeriesDateFormatter = DateTimeFormat.forPattern(TIME_SERIES_DATE_FORMAT).withZone(DEFAULT_DASHBOARD_TIMEZONE);
  private static final DateTimeFormatter startEndDateFormatterDays = DateTimeFormat.forPattern(START_END_DATE_FORMAT_DAYS).withZone(DEFAULT_DASHBOARD_TIMEZONE);
  private static final DateTimeFormatter startEndDateFormatterHours = DateTimeFormat.forPattern(START_END_DATE_FORMAT_HOURS).withZone(DEFAULT_DASHBOARD_TIMEZONE);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();

  private final MetricConfigManager metricConfigDAO;
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final DatasetConfigManager datasetConfigDAO;
  private final DetectionConfigManager detectionDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final EvaluationManager evaluationDAO;

  private final ExecutorService threadPool;
  private final AnomalyFunctionFactory anomalyFunctionFactory;

  private final TimeSeriesLoader timeSeriesLoader;
  private final AggregationLoader aggregationLoader;
  private final DetectionPipelineLoader loader;
  private final DataProvider provider;

  @Inject
  public AnomaliesResource(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
    this.detectionDAO = DAO_REGISTRY.getDetectionConfigManager();
    this.threadPool = Executors.newFixedThreadPool(NUM_EXECS);
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.evaluationDAO = DAO_REGISTRY.getEvaluationManager();

    QueryCache queryCache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    LoadingCache<String, Long> maxTimeCache = ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache();

    this.timeSeriesLoader = new DefaultTimeSeriesLoader(this.metricConfigDAO, this.datasetConfigDAO, queryCache, ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());
    this.aggregationLoader = new DefaultAggregationLoader(this.metricConfigDAO, this.datasetConfigDAO, queryCache, maxTimeCache);
    this.loader = new DetectionPipelineLoader();

    this.provider = new DefaultDataProvider(this.metricConfigDAO, this.datasetConfigDAO, this.eventDAO, this.anomalyDAO,
        this.evaluationDAO, this.timeSeriesLoader, this.aggregationLoader, this.loader,
        TimeSeriesCacheBuilder.getInstance(), AnomaliesCacheBuilder.getInstance());
  }

  /**
   * Search anomalies only by time
   * @param startTime
   * @param endTime
   * @return
   * @throws Exception
   */
  @GET
  @Path("search/time/{startTime}/{endTime}/{pageNumber}")
  public AnomaliesWrapper getAnomaliesByTime(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @PathParam("pageNumber") int pageNumber,
      @QueryParam("searchFilters") String searchFiltersJSON,
      @QueryParam("filterOnly") @DefaultValue("false") boolean filterOnly
      ) throws Exception {

    List<MergedAnomalyResultDTO> mergedAnomalies = mergedAnomalyResultDAO.findByTime(startTime, endTime);
    AnomaliesWrapper anomaliesWrapper =
        constructAnomaliesWrapperFromMergedAnomalies(removeChildren(mergedAnomalies), searchFiltersJSON, pageNumber, filterOnly);
    return anomaliesWrapper;
  }

  /**
   * Find anomalies by anomaly ids
   * @param startTime
   * @param endTime
   * @param anomalyIdsString
   * @return
   * @throws Exception
   */
  @GET
  @Path("search/anomalyIds/{startTime}/{endTime}/{pageNumber}")
  public AnomaliesWrapper getAnomaliesByAnomalyIds(
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @PathParam("pageNumber") int pageNumber,
      @QueryParam("anomalyIds") String anomalyIdsString,
      @QueryParam("searchFilters") String searchFiltersJSON,
      @QueryParam("filterOnly") @DefaultValue("false") boolean filterOnly) throws Exception {

    String[] anomalyIds = anomalyIdsString.split(",");
    List<MergedAnomalyResultDTO> mergedAnomalies = new ArrayList<>();
    for (String id : anomalyIds) {
      Long anomalyId = Long.valueOf(id);
      MergedAnomalyResultDTO anomaly = mergedAnomalyResultDAO.findById(anomalyId);
      if (anomaly != null) {
        mergedAnomalies.add(anomaly);
      }
    }
    AnomaliesWrapper
        anomaliesWrapper = constructAnomaliesWrapperFromMergedAnomalies(mergedAnomalies, searchFiltersJSON, pageNumber, filterOnly);
    return anomaliesWrapper;
  }
  // ----------- HELPER FUNCTIONS

  /**
   * Extract data values form timeseries object
   * @param dataSeries
   * @return
   * @throws JSONException
   */
  private List<String> getDataFromTimeSeriesObject(List<Double> dataSeries) throws JSONException {
    List<String> list = new ArrayList<>();
    for (int i = 0; i < dataSeries.size(); i++) {
        list.add(dataSeries.get(i).toString());
    }
    return list;
  }

  /**
   * Extract date values from time series object
   * @param timeBucket
   * @param timeSeriesDateFormatter
   * @return
   * @throws JSONException
   */
  private List<String> getDateFromTimeSeriesObject(List<TimeBucket> timeBucket, DateTimeFormatter timeSeriesDateFormatter) throws JSONException {
    List<String> list = new ArrayList<>();
    for (int i = 0; i < timeBucket.size(); i++) {
      list.add(timeSeriesDateFormatter.print(timeBucket.get(i).getCurrentStart()));
    }
    return list;
  }

  /**
   * Get formatted date time for anomaly chart
   * @param timestamp
   * @param datasetConfig
   * @return
   */
  private String getFormattedDateTime(long timestamp, DatasetConfigDTO datasetConfig) {
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeUnit dataGranularityUnit = timeSpec.getDataGranularity().getUnit();
    String formattedDateTime = null;
    if (dataGranularityUnit.equals(TimeUnit.MINUTES) || dataGranularityUnit.equals(TimeUnit.HOURS)) {
      formattedDateTime = startEndDateFormatterHours.print(timestamp);
    } else if (dataGranularityUnit.equals(TimeUnit.DAYS)) {
      formattedDateTime = startEndDateFormatterDays.print(timestamp);
    }
    return formattedDateTime;
  }


  /**
   * We want the anomaly point to be in between the shaded anomaly region.
   * Hence we will append some buffer to the start and end of the anomaly point
   * @param startTime
   * @param dateTimeZone
   * @param dataUnit
   * @return new anomaly start time
   */
  private long appendRegionToAnomalyStart(long startTime, DateTimeZone dateTimeZone, TimeUnit dataUnit) {
    Period periodToAppend = new Period(0, 0, 0, 0, 0, 0, 0, 0);
    switch (dataUnit) {
      case DAYS: // append 1 HOUR
        periodToAppend = new Period(0, 0, 0, 0, 1, 0, 0, 0);
        break;
      case HOURS: // append 30 MINUTES
        periodToAppend = new Period(0, 0, 0, 0, 0, 30, 0, 0);
        break;
      default:
        break;
    }
    DateTime startDateTime = new DateTime(startTime, dateTimeZone);
    return startDateTime.minus(periodToAppend).getMillis();
  }

  /**
   * We want the anomaly point to be in between the shaded anomaly region.
   * Hence we will append some buffer to the start and end of the anomaly point
   * @param endTime
   * @param dateTimeZone
   * @param dataUnit
   * @return new anomaly start time
   */
  private long subtractRegionFromAnomalyEnd(long endTime, DateTimeZone dateTimeZone, TimeUnit dataUnit) {
    Period periodToSubtract = new Period(0, 0, 0, 0, 0, 0, 0, 0);
    switch (dataUnit) {
      case DAYS: // subtract 23 HOUR
        periodToSubtract = new Period(0, 0, 0, 0, 23, 0, 0, 0);
        break;
      case HOURS: // subtract 30 MINUTES
        periodToSubtract = new Period(0, 0, 0, 0, 0, 30, 0, 0);
        break;
      default:
        break;
    }
    DateTime endDateTime = new DateTime(endTime, dateTimeZone);
    return endDateTime.minus(periodToSubtract).getMillis();
  }

  /**
   * Removes child anomalies
   */
  private List<MergedAnomalyResultDTO> removeChildren(List<MergedAnomalyResultDTO> mergedAnomalies) {
    mergedAnomalies.removeIf(MergedAnomalyResultBean::isChild);
    return mergedAnomalies;
  }

  /**
   * Constructs AnomaliesWrapper object from a list of merged anomalies
   */
  private AnomaliesWrapper constructAnomaliesWrapperFromMergedAnomalies(List<MergedAnomalyResultDTO> mergedAnomalies,
      String searchFiltersJSON, int pageNumber, boolean filterOnly) throws ExecutionException {

    AnomaliesWrapper anomaliesWrapper = new AnomaliesWrapper();

    //filter the anomalies
    SearchFilters searchFilters = new SearchFilters();
    if (searchFiltersJSON != null) {
      searchFilters = SearchFilters.fromJSON(searchFiltersJSON);
    }

    mergedAnomalies = SearchFilters.applySearchFilters(mergedAnomalies, searchFilters);

    //set the search filters
    anomaliesWrapper.setSearchFilters(SearchFilters.fromAnomalies(mergedAnomalies));
    anomaliesWrapper.setTotalAnomalies(mergedAnomalies.size());

    //set anomalyIds
    List<Long> anomalyIds = new ArrayList<>();
    for(MergedAnomalyResultDTO mergedAnomaly: mergedAnomalies){
      anomalyIds.add(mergedAnomaly.getId());
    }
    anomaliesWrapper.setAnomalyIds(anomalyIds);
    LOG.info("Total anomalies: {}", mergedAnomalies.size());
    if (filterOnly) {
      return anomaliesWrapper;
    }
    // TODO: get page number and page size from client
    int pageSize = DEFAULT_PAGE_SIZE;
    int maxPageNumber = (mergedAnomalies.size() - 1) / pageSize + 1;
    if (pageNumber > maxPageNumber) {
      pageNumber = maxPageNumber;
    }
    if (pageNumber < 1) {
      pageNumber = 1;
    }

    int fromIndex = (pageNumber - 1) * pageSize;
    int toIndex = pageNumber * pageSize;
    if (toIndex > mergedAnomalies.size()) {
      toIndex = mergedAnomalies.size();
    }

    // Show most recent anomalies first, i.e., the anomaly whose end time is most recent then largest id shown at top
    Collections.sort(mergedAnomalies, Collections.reverseOrder(new MergedAnomalyEndTimeComparator()));

    List<MergedAnomalyResultDTO> displayedAnomalies = mergedAnomalies.subList(fromIndex, toIndex);
    anomaliesWrapper.setNumAnomaliesOnPage(displayedAnomalies.size());
    LOG.info("Page number: {} Page size: {} Num anomalies on page: {}", pageNumber, pageSize, displayedAnomalies.size());

    // for each anomaly, create anomaly details
    List<Future<AnomalyDetails>> anomalyDetailsListFutures = new ArrayList<>();
    for (final MergedAnomalyResultDTO mergedAnomaly : displayedAnomalies) {
      Callable<AnomalyDetails> callable = new Callable<AnomalyDetails>() {
        @Override
        public AnomalyDetails call() throws Exception {
          String dataset = mergedAnomaly.getCollection();
          DatasetConfigDTO datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(dataset);
          return getAnomalyDetails(mergedAnomaly, datasetConfig, getExternalURL(mergedAnomaly));
        }
      };
      anomalyDetailsListFutures.add(threadPool.submit(callable));
    }

    List<AnomalyDetails> anomalyDetailsList = new ArrayList<>();
    for (Future<AnomalyDetails> anomalyDetailsFuture : anomalyDetailsListFutures) {
      try {
        AnomalyDetails anomalyDetails = anomalyDetailsFuture.get(120, TimeUnit.SECONDS);
        if (anomalyDetails != null) {
          anomalyDetailsList.add(anomalyDetails);
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        LOG.error("Exception in getting AnomalyDetails", e);
      }
    }
    anomaliesWrapper.setAnomalyDetailsList(anomalyDetailsList);

    return anomaliesWrapper;
  }


  /**
   * Return the natural order of MergedAnomaly by comparing their anomaly's end time.
   * The tie breaker is their anomaly ID.
   */
  private static class MergedAnomalyEndTimeComparator implements Comparator<MergedAnomalyResultDTO> {
    @Override
    public int compare(MergedAnomalyResultDTO anomaly1, MergedAnomalyResultDTO anomaly2) {
      if (anomaly1.getEndTime() != anomaly2.getEndTime()) {
        return (int) (anomaly1.getEndTime() - anomaly2.getEndTime());
      } else {
        return (int) (anomaly1.getId() - anomaly2.getId());
      }
    }
  }

  private String getExternalURL(MergedAnomalyResultDTO mergedAnomaly) {
    return new JSONObject(ResourceUtils.getExternalURLs(mergedAnomaly, this.metricConfigDAO, this.datasetConfigDAO)).toString();
  }

  /**
   * Generate AnomalyTimelinesView for given function with monitoring time range
   *
   * @param anomalyFunctionSpec
   *      The DTO of the anomaly function
   * @param datasetConfig
   *      The dataset configuration which the anomalyFunctionSpec is monitoring
   * @param windowStart
   *      The start time of the monitoring window
   * @param windowEnd
   *      The end time of the monitoring window
   * @param dimensions
   *      The dimension map of the timelineview is surfacing
   * @return
   *      An AnomalyTimelinesView instance with current value and baseline values of given function
   * @throws Exception
   */
  public AnomalyTimelinesView getTimelinesViewInMonitoringWindow(AnomalyFunctionDTO anomalyFunctionSpec,
      DatasetConfigDTO datasetConfig, DateTime windowStart, DateTime windowEnd, DimensionMap dimensions) throws Exception {
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    // get anomaly window range - this is the data to fetch (anomaly region + some offset if required)
    // the function will tell us this range, as how much data we fetch can change depending on which function is being executed
    TimeRange monitoringWindowRange = getAnomalyWindowOffset(windowStart, windowEnd, anomalyFunction, datasetConfig);
    DateTime monitoringWindowStart = new DateTime(monitoringWindowRange.getStart());
    DateTime monitoringWindowEnd = new DateTime(monitoringWindowRange.getEnd());

    List<Pair<Long, Long>> dataRangeIntervals = anomalyFunction
        .getDataRangeIntervals(monitoringWindowStart.getMillis(), monitoringWindowEnd.getMillis());

    return getTimelinesViewInMonitoringWindow(anomalyFunctionSpec, datasetConfig, dataRangeIntervals, dimensions);
  }

  /**
   * Generate AnomalyTimelinesView for given function with user-defined time range
   *
   * If user has its defined time range, use user defined time range to generate teh baselines
   *
   * @param anomalyFunctionSpec
   *      The DTO of the anomaly function
   * @param datasetConfig
   *      The dataset configuration which the anomalyFunctionSpec is monitoring
   * @param dataRangeIntervals
   *      User-defined data range intervals, the first is the monitoring window, the rest are the training window
   * @param dimensions
   *      The dimension map of the timelineview is surfacing
   * @return
   *      An AnomalyTimelinesView instance with current value and baseline values of given function
   * @throws Exception
   */
  public AnomalyTimelinesView getTimelinesViewInMonitoringWindow(AnomalyFunctionDTO anomalyFunctionSpec,
      DatasetConfigDTO datasetConfig, List<Pair<Long, Long>> dataRangeIntervals, DimensionMap dimensions)
      throws Exception{
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    // the first data range interval is always the monitoring data range
    Pair<Long, Long> monitoringDataInterval = dataRangeIntervals.get(0);

    // Dataset time granularity
    String aggGranularity = datasetConfig.bucketTimeGranularity().toAggregationGranularityString();
    TimeGranularity timeGranularity =
        Utils.getAggregationTimeGranularity(aggGranularity, anomalyFunctionSpec.getCollection());

    // Anomaly function time granularity
    TimeGranularity functionTimeGranularity =
        new TimeGranularity(anomalyFunctionSpec.getBucketSize(), anomalyFunctionSpec.getBucketUnit());

    long bucketMillis = functionTimeGranularity.toMillis();
    /*
     If the function-specified time granularity is smaller than the dataset time granularity, use the dataset's.

     Ex: If the time granularity of the dataset is 1-Day and function assigns 1-Hours, than use 1-Day
      */
    if (functionTimeGranularity.toMillis() < timeGranularity.toMillis()) {
      LOG.warn("The time granularity of the function {}, {}, is smaller than the dataset's, {}. Use dataset's setting.",
          functionTimeGranularity.toString(), anomalyFunctionSpec.getId(), aggGranularity.toString());
      bucketMillis = timeGranularity.toMillis();
    }
    AnomalyTimelinesView anomalyTimelinesView = null;
    AnomalyDetectionInputContextBuilder anomalyDetectionInputContextBuilder =
        new AnomalyDetectionInputContextBuilder(anomalyFunctionFactory);
    AnomalyDetectionInputContext adInputContext = anomalyDetectionInputContextBuilder
        .setFunction(anomalyFunctionSpec)
        .fetchTimeSeriesDataByDimension(dataRangeIntervals, dimensions, true)
        .fetchExistingMergedAnomalies(dataRangeIntervals, false)
        .fetchScalingFactors(dataRangeIntervals).build();

    MetricTimeSeries metricTimeSeries = adInputContext.getDimensionMapMetricTimeSeriesMap().get(dimensions);

    // Transform time series with scaling factor
    List<ScalingFactor> scalingFactors = adInputContext.getScalingFactors();
    if (CollectionUtils.isNotEmpty(scalingFactors)) {
      Properties properties = anomalyFunction.getProperties();
      MetricTransfer.rescaleMetric(metricTimeSeries, monitoringDataInterval.getFirst(), scalingFactors,
          anomalyFunctionSpec.getTopicMetric(), properties);
    }

    List<MergedAnomalyResultDTO> knownAnomalies = adInputContext.getKnownMergedAnomalies().get(dimensions);
    // Known anomalies are ignored (the null parameter) because 1. we can reduce users' waiting time and 2. presentation
    // data does not need to be as accurate as the one used for detecting anomalies
    anomalyTimelinesView = anomalyFunction.getTimeSeriesView(metricTimeSeries, bucketMillis, anomalyFunctionSpec.getTopicMetric(),
        monitoringDataInterval.getFirst(), monitoringDataInterval.getSecond(), knownAnomalies);


    return anomalyTimelinesView;
  }

  /**
   * Generates Anomaly Details for each merged anomaly
   * @param mergedAnomaly
   * @param datasetConfig
   * @param externalUrl
   * @return
   */
  private AnomalyDetails getAnomalyDetails(MergedAnomalyResultDTO mergedAnomaly, DatasetConfigDTO datasetConfig,
      String externalUrl) throws Exception {

    String dataset = datasetConfig.getDataset();
    String metricName = mergedAnomaly.getMetric();

    if (mergedAnomaly.getDetectionConfigId() != null) {

      DetectionConfigDTO detectionConfig = this.detectionDAO.findById(mergedAnomaly.getDetectionConfigId());
      if (detectionConfig == null) {
        return null;
      }
      return constructAnomalyDetails(mergedAnomaly, detectionConfig);
    }

    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(mergedAnomaly.getFunctionId());
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    DateTime anomalyStart = new DateTime(mergedAnomaly.getStartTime());
    DateTime anomalyEnd = new DateTime(mergedAnomaly.getEndTime());

    DimensionMap dimensions = mergedAnomaly.getDimensions();
    try {
      AnomalyTimelinesView anomalyTimelinesView = null;
      Map<String, String> anomalyProps = mergedAnomaly.getProperties();
      if(anomalyProps.containsKey("anomalyTimelinesView")) {
        anomalyTimelinesView = AnomalyTimelinesView.fromJsonString(anomalyProps.get("anomalyTimelinesView"));
      } else {
        anomalyTimelinesView = getTimelinesViewInMonitoringWindow(anomalyFunctionSpec, datasetConfig,
            anomalyStart, anomalyEnd, dimensions);
      }

      // get viewing window range - this is the region to display along with anomaly, from all the fetched data.
      // the function will tell us this range, as how much data we display can change depending on which function is being executed
      TimeRange viewWindowRange = getViewWindowOffset(anomalyStart, anomalyEnd, anomalyFunction, datasetConfig);
      long viewWindowStart = viewWindowRange.getStart();
      long viewWindowEnd = viewWindowRange.getEnd();
      return constructAnomalyDetails(metricName, dataset, datasetConfig, mergedAnomaly, anomalyFunctionSpec,
          viewWindowStart, viewWindowEnd, anomalyTimelinesView, externalUrl);
    } catch (Exception e) {
      LOG.error("Exception in constructing anomaly wrapper for anomaly {}", mergedAnomaly.getId(), e);
    }

    return null;
  }

  /** Construct anomaly details using all details fetched from calls
   *
   * @param metricName
   * @param dataset
   * @param datasetConfig
   * @param mergedAnomaly
   * @param anomalyFunction
   * @param currentStartTime inclusive
   * @param currentEndTime inclusive
   * @param anomalyTimelinesView
   * @return
   * @throws JSONException
   */
  private AnomalyDetails constructAnomalyDetails(String metricName, String dataset, DatasetConfigDTO datasetConfig,
      MergedAnomalyResultDTO mergedAnomaly, AnomalyFunctionDTO anomalyFunction, long currentStartTime,
      long currentEndTime, AnomalyTimelinesView anomalyTimelinesView, String externalUrl)
      throws JSONException {

    MetricConfigDTO metricConfigDTO = metricConfigDAO.findByMetricAndDataset(metricName, dataset);
    DateTimeZone dateTimeZone = Utils.getDataTimeZone(dataset);
    TimeUnit dataUnit = datasetConfig.bucketTimeGranularity().getUnit();

    AnomalyDetails anomalyDetails = new AnomalyDetails();
    anomalyDetails.setMetric(metricName);
    anomalyDetails.setDataset(dataset);

    if (metricConfigDTO != null) {
      anomalyDetails.setMetricId(metricConfigDTO.getId());
    }
    anomalyDetails.setTimeUnit(dataUnit.toString());

    // The filter ensures that the returned time series from anomalies function only includes the values that are
    // located inside the request windows (i.e., between currentStartTime and currentEndTime, inclusive).
    List<TimeBucket> timeBuckets = anomalyTimelinesView.getTimeBuckets();
    int timeStartIndex = -1;
    int timeEndIndex = -1;
    for (int i = 0; i < timeBuckets.size(); ++i) {
      long currentTimeStamp = timeBuckets.get(i).getCurrentStart();
      if (timeStartIndex < 0 &&  currentTimeStamp >= currentStartTime) {
        timeStartIndex = i;
        timeEndIndex = i + 1;
      } else if (currentTimeStamp <= currentEndTime) {
        timeEndIndex = i + 1;
      } else if (currentTimeStamp > currentEndTime) {
        break;
      }
    }
    if (timeStartIndex < 0 || timeEndIndex < 0) {
      timeStartIndex = 0;
      timeEndIndex = 0;
    }

    // get this from timeseries calls
    List<String> dateValues = getDateFromTimeSeriesObject(timeBuckets.subList(timeStartIndex, timeEndIndex), timeSeriesDateFormatter);
    anomalyDetails.setDates(dateValues);
    anomalyDetails.setCurrentStart(getFormattedDateTime(currentStartTime, datasetConfig));
    anomalyDetails.setCurrentEnd(getFormattedDateTime(currentEndTime, datasetConfig));
    List<String> baselineValues = getDataFromTimeSeriesObject(anomalyTimelinesView.getBaselineValues().subList(timeStartIndex, timeEndIndex));
    anomalyDetails.setBaselineValues(baselineValues);
    List<String> currentValues = getDataFromTimeSeriesObject(anomalyTimelinesView.getCurrentValues().subList(timeStartIndex, timeEndIndex));
    anomalyDetails.setCurrentValues(currentValues);

    // from function and anomaly
    anomalyDetails.setAnomalyId(mergedAnomaly.getId());
    anomalyDetails.setAnomalyStart(timeSeriesDateFormatter.print(mergedAnomaly.getStartTime()));
    anomalyDetails.setAnomalyEnd(timeSeriesDateFormatter.print(mergedAnomaly.getEndTime()));
    long newAnomalyRegionStart = appendRegionToAnomalyStart(mergedAnomaly.getStartTime(), dateTimeZone, dataUnit);
    long newAnomalyRegionEnd = subtractRegionFromAnomalyEnd(mergedAnomaly.getEndTime(), dateTimeZone, dataUnit);
    anomalyDetails.setAnomalyRegionStart(timeSeriesDateFormatter.print(newAnomalyRegionStart));
    anomalyDetails.setAnomalyRegionEnd(timeSeriesDateFormatter.print(newAnomalyRegionEnd));
    anomalyDetails.setCurrent(ThirdEyeUtils.getRoundedValue(mergedAnomaly.getAvgCurrentVal()));
    anomalyDetails.setBaseline(ThirdEyeUtils.getRoundedValue(mergedAnomaly.getAvgBaselineVal()));
    anomalyDetails.setAnomalyResultSource(mergedAnomaly.getAnomalyResultSource().toString());
    anomalyDetails.setAnomalyFunctionId(anomalyFunction.getId());
    anomalyDetails.setAnomalyFunctionName(anomalyFunction.getFunctionName());
    anomalyDetails.setAnomalyFunctionType(anomalyFunction.getType());
    anomalyDetails.setAnomalyFunctionProps(anomalyFunction.getProperties());

    // Combine dimension map and filter set to construct a new filter set for the time series query of this anomaly
    Multimap<String, String> newFilterSet = AnomalyUtils.generateFilterSetForTimeSeriesQuery(mergedAnomaly);
    try {
      anomalyDetails.setAnomalyFunctionDimension(OBJECT_MAPPER.writeValueAsString(newFilterSet.asMap()));
    } catch (JsonProcessingException e) {
      LOG.warn("Failed to convert the dimension info ({}) to a JSON string; the original dimension info ({}) is used.",
          newFilterSet, mergedAnomaly.getDimensions());
      anomalyDetails.setAnomalyFunctionDimension(mergedAnomaly.getDimensions().toString());
    }
    AnomalyFeedback mergedAnomalyFeedback = mergedAnomaly.getFeedback();
    if (mergedAnomalyFeedback != null) {
      anomalyDetails.setAnomalyFeedback(AnomalyDetails.getFeedbackStringFromFeedbackType(mergedAnomalyFeedback.getFeedbackType()));
      anomalyDetails.setAnomalyFeedbackComments(mergedAnomalyFeedback.getComment());
    }
    anomalyDetails.setExternalUrl(externalUrl);
    if (MapUtils.isNotEmpty(mergedAnomaly.getProperties()) && mergedAnomaly.getProperties()
        .containsKey(MergedAnomalyResultDTO.ISSUE_TYPE_KEY)) {
      anomalyDetails.setIssueType(mergedAnomaly.getProperties().get(MergedAnomalyResultDTO.ISSUE_TYPE_KEY));
    }

    return anomalyDetails;
  }

  /**
   * Construct legacy anomaly details for detection config anomalies.
   * <br/><b>NOTE:</b> This method was written for (empirical) backward compatibility with the
   * behavior of the method above.
   *
   * @see AnomaliesResource#constructAnomalyDetails(String, String, DatasetConfigDTO, MergedAnomalyResultDTO, AnomalyFunctionDTO, long, long, AnomalyTimelinesView, String)
   *
   * @param anomaly merged anomaly
   * @param config detection config
   * @return
   * @throws Exception
   */
  private AnomalyDetails constructAnomalyDetails(MergedAnomalyResultDTO anomaly, DetectionConfigDTO config) throws Exception {
    MetricConfigDTO metric = this.metricConfigDAO.findByMetricAndDataset(anomaly.getMetric(), anomaly.getCollection());
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric '%s' in dataset '%s' for anomaly id %d", anomaly.getMetric(), anomaly.getCollection(), anomaly.getId()));
    }

    DatasetConfigDTO dataset = this.datasetConfigDAO.findByDataset(anomaly.getCollection());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for anomaly id %d", anomaly.getCollection(), anomaly.getId()));
    }

    DateTimeZone dataTimeZone = DateTimeZone.forID(dataset.getTimezone());
    TimeUnit dataTimeUnit = dataset.bucketTimeGranularity().getUnit();
    Multimap<String, String> filters = ResourceUtils.getAnomalyFilters(anomaly, this.datasetConfigDAO);

    AnomalyDetails details = new AnomalyDetails();

    // feedback
    AnomalyFeedback feedback = anomaly.getFeedback();

    if (feedback != null) {
      details.setAnomalyFeedback(AnomalyDetails.getFeedbackStringFromFeedbackType(feedback.getFeedbackType()));
      details.setAnomalyFeedbackStatus(null); // not used apparently
      details.setAnomalyFeedbackComments(feedback.getComment());
    }

    // function
    details.setAnomalyFunctionId(-1L);
    details.setAnomalyFunctionName(config.getName());
    details.setAnomalyFunctionProps(""); // empty for detection config
    details.setAnomalyFunctionType("DETECTION_CONFIG");
    details.setAnomalyFunctionDimension(OBJECT_MAPPER.writeValueAsString(filters.asMap()));

    // anomaly
    details.setAnomalyId(anomaly.getId());
    details.setAnomalyStart(timeSeriesDateFormatter.print(anomaly.getStartTime()));
    details.setAnomalyEnd(timeSeriesDateFormatter.print(anomaly.getEndTime()));

    long newAnomalyRegionStart = appendRegionToAnomalyStart(anomaly.getStartTime(), dataTimeZone, dataTimeUnit);
    long newAnomalyRegionEnd = subtractRegionFromAnomalyEnd(anomaly.getEndTime(), dataTimeZone, dataTimeUnit);

    details.setAnomalyRegionStart(timeSeriesDateFormatter.print(newAnomalyRegionStart));
    details.setAnomalyRegionEnd(timeSeriesDateFormatter.print(newAnomalyRegionEnd));

    details.setAnomalyResultSource(anomaly.getAnomalyResultSource().toString());
    details.setIssueType(null); // null for detection config

    // metric
    details.setMetric(anomaly.getMetric());
    details.setMetricId(metric.getId());
    details.setDataset(anomaly.getCollection());

    // values and time series
    // NOTE: always use week-over-week as baseline

    // TODO parallel load
    // TODO type from agg function
    MetricSlice sliceAnomalyCurrent = MetricSlice.from(metric.getId(), anomaly.getStartTime(), anomaly.getEndTime(), filters);

    details.setCurrent(makeStringValue(new DataFrame().addSeries(DataFrame.COL_VALUE, anomaly.getAvgCurrentVal())));
    details.setBaseline(makeStringValue(new DataFrame().addSeries(DataFrame.COL_VALUE, anomaly.getAvgBaselineVal())));

    AnomalyOffset offsets = BaseAnomalyFunction.getDefaultOffsets(dataset);

    MetricSlice sliceViewCurrent = sliceAnomalyCurrent
        .withStart(new DateTime(sliceAnomalyCurrent.getStart(), dataTimeZone).minus(offsets.getPreOffsetPeriod()).getMillis())
        .withEnd(new DateTime(sliceAnomalyCurrent.getEnd(), dataTimeZone).plus(offsets.getPostOffsetPeriod()).getMillis());

    DataFrame dfBaseline = DetectionUtils.getBaselineTimeseries(anomaly, filters, metric.getId(), config, sliceViewCurrent.getStart(), sliceViewCurrent.getEnd(), this.loader, this.provider).getDataFrame();
    DataFrame dfAligned;
    if (dfBaseline.contains(COL_CURRENT)) {
      // if baseline provider returns both current values and baseline values, using them as the result
      dfAligned = dfBaseline;
      dfAligned.renameSeries(DataFrame.COL_VALUE, COL_BASELINE);
    } else {
      // otherwise fetch current values and join the time series to generate the result
      DataFrame dfCurrent = this.timeSeriesLoader.load(sliceViewCurrent);
      dfAligned = dfCurrent.renameSeries(DataFrame.COL_VALUE, COL_CURRENT).joinOuter(dfBaseline.renameSeries(
          DataFrame.COL_VALUE, COL_BASELINE));
    }

    details.setDates(makeStringDates(dfAligned.getLongs(DataFrame.COL_TIME)));
    details.setCurrentValues(makeStringValues(dfAligned.getDoubles(COL_CURRENT)));
    details.setBaselineValues(makeStringValues(dfAligned.getDoubles(COL_BASELINE)));

    details.setTimeUnit(dataTimeUnit.toString());
    details.setCurrentStart(getFormattedDateTime(anomaly.getStartTime(), dataset));
    details.setCurrentEnd(getFormattedDateTime(anomaly.getEndTime(), dataset));

    return details;
  }

  private static String makeStringValue(DataFrame aggregate) {
    if (aggregate.isEmpty()) {
      return String.valueOf(Double.NaN);
    }
    if (aggregate.get(DataFrame.COL_VALUE).isNull(0)) {
      return String.valueOf(Double.NaN);
    }
    if (Double.isInfinite(aggregate.getDouble(DataFrame.COL_VALUE, 0))) {
      return String.valueOf(Double.NaN);
    }
    return formatDoubleValue(aggregate.getDouble(DataFrame.COL_VALUE, 0));
  }

  private static List<String> makeStringValues(DoubleSeries timeseries) {
    List<String> dates = new ArrayList<>();
    for (Double value : timeseries.toList()) {
      if (value == null) {
        dates.add(String.valueOf(Double.NaN));
        continue;
      }

      dates.add(formatDoubleValue(value));
    }
    return dates;
  }

  private static List<String> makeStringDates(LongSeries timeseries) {
    List<String> dates = new ArrayList<>();
    for (Long timestamp : timeseries.toList()) {
      dates.add(timeSeriesDateFormatter.print(timestamp));
    }
    return dates;
  }

  private static String formatDoubleValue(double value) {
    if (Double.isInfinite(value) || Double.isNaN(value)) {
      return String.valueOf(Double.NaN);
    }
    if (Math.abs(value) >= 1000) {
      return String.format("%.0f", value);
    }
    if (Math.abs(value) >= 0.001) {
      return String.format("%.3f", value);
    }
    return String.valueOf(value);
  }

  private TimeRange getAnomalyWindowOffset(DateTime windowStart, DateTime windowEnd, BaseAnomalyFunction anomalyFunction,
      DatasetConfigDTO datasetConfig) {
    AnomalyOffset anomalyWindowOffset = anomalyFunction.getAnomalyWindowOffset(datasetConfig);
    TimeRange anomalyWindowRange = getTimeRangeWithOffsets(anomalyWindowOffset, windowStart, windowEnd, datasetConfig);
    return anomalyWindowRange;
  }


  private TimeRange getViewWindowOffset(DateTime windowStart, DateTime windowEnd, BaseAnomalyFunction anomalyFunction,
      DatasetConfigDTO datasetConfig) {
    AnomalyOffset viewWindowOffset = anomalyFunction.getViewWindowOffset(datasetConfig);
    TimeRange viewWindowRange = getTimeRangeWithOffsets(viewWindowOffset, windowStart, windowEnd, datasetConfig);
    return viewWindowRange;
  }

  private TimeRange getTimeRangeWithOffsets(AnomalyOffset offset, DateTime windowStart, DateTime windowEnd,
      DatasetConfigDTO datasetConfig) {
    Period preOffsetPeriod = offset.getPreOffsetPeriod();
    Period postOffsetPeriod = offset.getPostOffsetPeriod();

    DateTimeZone dateTimeZone = DateTimeZone.forID(datasetConfig.getTimezone());
    DateTime windowStartDateTime = new DateTime(windowStart, dateTimeZone);
    DateTime windowEndDateTime = new DateTime(windowEnd, dateTimeZone);
    windowStartDateTime = windowStartDateTime.minus(preOffsetPeriod);
    windowEndDateTime = windowEndDateTime.plus(postOffsetPeriod);
    long windowStartTime = windowStartDateTime.getMillis();
    long windowEndTime = windowEndDateTime.getMillis();
    try {
      Long maxDataTime = CACHE_REGISTRY.getDatasetMaxDataTimeCache().get(datasetConfig.getDataset());
      if (windowEndTime > maxDataTime) {
        windowEndTime = maxDataTime;
      }
    } catch (ExecutionException e) {
      LOG.error("Exception when reading max time for {}", datasetConfig.getDataset(), e);
    }
    TimeRange range = new TimeRange(windowStartTime, windowEndTime);
    return range;

  }

}
