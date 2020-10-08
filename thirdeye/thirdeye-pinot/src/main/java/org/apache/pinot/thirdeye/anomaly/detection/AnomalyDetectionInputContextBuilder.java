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

package org.apache.pinot.thirdeye.anomaly.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.apache.pinot.thirdeye.anomaly.override.OverrideConfigHelper;
import org.apache.pinot.thirdeye.common.dimension.DimensionKey;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.common.metric.MetricTimeSeries;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.util.ThirdEyeDataUtils;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.ResponseParserUtils;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.timeseries.AnomalyDetectionTimeSeriesResponseParser;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesHandler;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesRequest;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesResponse;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesResponseConverter;
import org.apache.pinot.thirdeye.datasource.timeseries.TimeSeriesRow;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;
import org.apache.pinot.thirdeye.detector.function.BaseAnomalyFunction;
import org.apache.pinot.thirdeye.detector.metric.transfer.ScalingFactor;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyDetectionInputContextBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionInputContextBuilder.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private AnomalyDetectionInputContext anomalyDetectionInputContext;
  private AnomalyFunctionDTO anomalyFunctionSpec;
  private BaseAnomalyFunction anomalyFunction;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private List<String> collectionDimensions;
  private String dataset;

  public AnomalyDetectionInputContextBuilder(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  public AnomalyDetectionInputContextBuilder setFunction(AnomalyFunctionDTO anomalyFunctionSpec) throws Exception {
    return setFunction(anomalyFunctionSpec, new AnomalyDetectionInputContext());
  }

  public AnomalyDetectionInputContextBuilder setFunction(AnomalyFunctionDTO anomalyFunctionSpec, AnomalyDetectionInputContext anomalyDetectionInputContext)
      throws Exception {
    this.anomalyFunctionSpec = anomalyFunctionSpec;
    this.anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);
    this.anomalyDetectionInputContext = anomalyDetectionInputContext;
    this.dataset = this.anomalyFunctionSpec.getCollection();
    DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
    if (datasetConfig == null) {
      LOG.error("Dataset [" + dataset + "] is not found");
      throw new IllegalArgumentException(
          "Dataset [" + dataset + "] is not found with function : " + anomalyFunctionSpec
              .toString());
    }
    this.collectionDimensions = datasetConfig.getDimensions();
    return this;
  }

  public AnomalyDetectionInputContext build() {
    return this.anomalyDetectionInputContext;
  }

  /**
   * Fetch TimeSeriese data from Pinot in the startEndTimeRanges
   * @param startEndTimeRanges
   * the time range when we actually fetch timeseries
   * @return
   * the builder of the AnomalyDetectionInputContext
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesData(List<Pair<Long, Long>> startEndTimeRanges, boolean endTimeInclusive)
      throws JobExecutionException, ExecutionException {
    Map<DimensionKey, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap =
        getTimeSeriesForAnomalyDetection(anomalyFunctionSpec, startEndTimeRanges, endTimeInclusive);

    Map<DimensionMap, MetricTimeSeries> dimensionMapMetricTimeSeriesMap = new HashMap<>();
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : dimensionKeyMetricTimeSeriesMap.entrySet()) {
      DimensionKey dimensionKey = entry.getKey();

      // If the current time series belongs to OTHER dimension, which consists of time series whose
      // sum of all its values belows 1% of sum of all time series values, then its anomaly is
      // meaningless and hence we don't want to detection anomalies on it.
      String[] dimensionValues = dimensionKey.getDimensionValues();
      boolean isOTHERDimension = false;
      for (String dimensionValue : dimensionValues) {
        if (dimensionValue.equalsIgnoreCase(ResponseParserUtils.OTHER) || dimensionValue.equalsIgnoreCase(
            ResponseParserUtils.UNKNOWN)) {
          isOTHERDimension = true;
          break;
        }
      }
      if (isOTHERDimension) {
        continue;
      }

      DimensionMap dimensionMap = DimensionMap.fromDimensionKey(dimensionKey, collectionDimensions);
      dimensionMapMetricTimeSeriesMap.put(dimensionMap, entry.getValue());

      if (entry.getValue().getTimeWindowSet().size() < 1) {
        LOG.warn("Insufficient data for {} to run anomaly detection function", dimensionMap);
      }
    }
    this.anomalyDetectionInputContext.setDimensionMapMetricTimeSeriesMap(dimensionMapMetricTimeSeriesMap);

    return this;
  }
  /**
   * Fetch TimeSeriese data from Pinot with given monitoring window start and end time
   * The data range is calculated by anomalyFunction.getDataRangeIntervals
   * If endTimeInclusive is set true, the windowEnd is included in the timeseries; otherwise, not.
   * @param windowStart
   * The start time of the monitoring window
   * @param windowEnd
   * The start time of the monitoring window
   * @param endTimeInclusive
   * true, if the end time is included in the data fetching process
   * @return
   * the builder of the AnomalyDetectionInputContext
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesData(DateTime windowStart, DateTime windowEnd, boolean endTimeInclusive)
      throws JobExecutionException, ExecutionException {
    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis());
    return fetchTimeSeriesData(startEndTimeRanges, endTimeInclusive);
  }
  /**
   * Fetch TimeSeriese data from Pinot with given monitoring window start and end time
   * The data range is calculated by anomalyFunction.getDataRangeIntervals
   * endTimeInclusive is set to false in default
   * @param windowStart
   * The start time of the monitoring window
   * @param windowEnd
   * The start time of the monitoring window
   * @return
   * the builder of the AnomalyDetectionInputContext
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesData(DateTime windowStart, DateTime windowEnd)
      throws JobExecutionException, ExecutionException {
    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis());
    return fetchTimeSeriesData(startEndTimeRanges, false);
  }
  /**
   * Fetch TimeSeriese data from Pinot with given time range
   * endTimeInclusive is set to false in default
   *
   * @param startEndTimeRanges
   * The time range when we should fetch data
   * @return
   * the builder of the AnomalyDetectionInputContext
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesData(List<Pair<Long, Long>> startEndTimeRanges)
      throws JobExecutionException, ExecutionException {
    return fetchTimeSeriesData(startEndTimeRanges, false);
  }


  /**
   * Fetch time series, known merged anomalies, and scaling factor for the specified dimension. Note that scaling
   * factor has no dimension information, so all scaling factor in the specified time range will be retrieved.
   *
   * @param windowStart the start time for retrieving the data
   * @param windowEnd the end time for retrieving the data
   * @param dimensions the dimension of the data
   * @param endTimeInclusive set to true if the end time should be inclusive; mainly used by the queries from UI
   * @return
   * the builder of the AnomalyDetectionInputContext
   * @throws Exception if it fails to retrieve time series from DB.
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesDataByDimension(DateTime windowStart, DateTime windowEnd,
      DimensionMap dimensions, boolean endTimeInclusive)
      throws Exception {
    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis());
    return fetchTimeSeriesDataByDimension(startEndTimeRanges, dimensions, endTimeInclusive);
  }

  /**
   * Fetch time series, known merged anomalies, and scaling factor for the specified dimension. Note that scaling
   * factor has no dimension information, so all scaling factor in the specified time range will be retrieved.
   *
   * @param startEndTimeRanges the start and end time range for retrieving the data
   * @param dimensions the dimension of the data
   * @param endTimeInclusive set to true if the end time should be inclusive; mainly used by the queries from UI
   * @return
   * the builder of the AnomalyDetectionInputContext
   * @throws Exception if it fails to retrieve time series from DB.
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesDataByDimension(List<Pair<Long, Long>> startEndTimeRanges,
      DimensionMap dimensions, boolean endTimeInclusive)
      throws Exception {
    TimeGranularity timeGranularity = new TimeGranularity(anomalyFunctionSpec.getBucketSize(), anomalyFunctionSpec.getBucketUnit());

    // Retrieve Time Series
    MetricTimeSeries metricTimeSeries =
        getTimeSeriesByDimension(anomalyFunctionSpec, startEndTimeRanges, dimensions, timeGranularity, endTimeInclusive);
    Map<DimensionMap, MetricTimeSeries> metricTimeSeriesMap = new HashMap<>();
    metricTimeSeriesMap.put(dimensions, metricTimeSeries);
    this.anomalyDetectionInputContext.setDimensionMapMetricTimeSeriesMap(metricTimeSeriesMap);

    return this;
  }

  /**
   * Fetch the global metric without dimension and filter in a given monitoring start and end time
   * the actual data fetching time range is calculated by anomalyFunction.getDataRangeIntervals
   * @param windowStart
   * the start time of the monitoring window
   * @param windowEnd
   * the end time of the monitoring window
   * @return
   * the builder of the AnomalyDetectionInputContext
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesGlobalMetric(DateTime windowStart, DateTime windowEnd)
      throws JobExecutionException, ExecutionException {
    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis());
    this.fetchTimeSeriesGlobalMetric(startEndTimeRanges);

    return this;
  }

  /**
   * Fetch the global metric without dimension and filter in a given time range
   * @param startEndTimeRanges
   * the time range when we should fetch the timeseries data
   * @return
   * the builder of the AnomalyDetectionInputContext
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesGlobalMetric(List<Pair<Long, Long>> startEndTimeRanges)
      throws JobExecutionException, ExecutionException {
    MetricTimeSeries metricSumTimeSeries = getGlobalMetric(anomalyFunctionSpec, startEndTimeRanges);
    this.anomalyDetectionInputContext.setGlobalMetric(metricSumTimeSeries);

    return this;
  }

  /**
   * Fetch existing MergedAnomalyResults in the training window
   * @param windowStart
   * the start time of the monitoring window
   * @param windowEnd
   * the end time of the monitoring window
   * @return
   */
  public AnomalyDetectionInputContextBuilder fetchExistingMergedAnomalies(DateTime windowStart, DateTime windowEnd, boolean loadRawAnomalies) {
// Get existing anomalies for this time range and this function id for all combinations of dimensions
    List<MergedAnomalyResultDTO> knownMergedAnomalies;
    if (anomalyFunction.useHistoryAnomaly()) {
      // if this anomaly function uses history data, then we get all time ranges
      return fetchExistingMergedAnomalies(
          anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis()), loadRawAnomalies);
    } else {
      // otherwise, we only get the merge anomaly for current window in order to remove duplicate raw anomalies
      List<Pair<Long, Long>> currentTimeRange = new ArrayList<>();
      currentTimeRange.add(new Pair<>(windowStart.getMillis(), windowEnd.getMillis()));
      return fetchExistingMergedAnomalies(currentTimeRange, loadRawAnomalies);
    }
  }

  public AnomalyDetectionInputContextBuilder fetchExistingMergedAnomalies(List<Pair<Long, Long>> startEndTimeRanges, boolean loadRawAnomalies) {
// Get existing anomalies for this time range and this function id for all combinations of dimensions
    List<MergedAnomalyResultDTO> knownMergedAnomalies;
    knownMergedAnomalies = getKnownMergedAnomalies(anomalyFunctionSpec.getId(), startEndTimeRanges, loadRawAnomalies);
    // Sort the known merged and raw anomalies by their dimension names
    ArrayListMultimap<DimensionMap, MergedAnomalyResultDTO> dimensionMapToKnownMergedAnomalies = ArrayListMultimap.create();
    for (MergedAnomalyResultDTO knownMergedAnomaly : knownMergedAnomalies) {
      dimensionMapToKnownMergedAnomalies.put(knownMergedAnomaly.getDimensions(), knownMergedAnomaly);
    }
    this.anomalyDetectionInputContext.setKnownMergedAnomalies(dimensionMapToKnownMergedAnomalies);

    return this;
  }

  public AnomalyDetectionInputContextBuilder fetchExistingMergedAnomaliesByDimension(DateTime windowStart,
      DateTime windowEnd, DimensionMap dimensions) {
    return fetchExistingMergedAnomaliesByDimension(
        anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis()), dimensions);
  }

  public AnomalyDetectionInputContextBuilder fetchExistingMergedAnomaliesByDimension(List<Pair<Long, Long>> startEndTimeRanges,
      DimensionMap dimensions) {
    ArrayListMultimap<DimensionMap, MergedAnomalyResultDTO> dimensionMapToKnownMergedAnomalies = ArrayListMultimap.create();
    dimensionMapToKnownMergedAnomalies.putAll(dimensions,
        getKnownMergedAnomaliesByDimension(anomalyFunctionSpec.getId(), startEndTimeRanges, dimensions));

    this.anomalyDetectionInputContext.setKnownMergedAnomalies(dimensionMapToKnownMergedAnomalies);
    return this;
  }
  /**
   * Fetch Scaling Factors in the training window
   * @param windowStart
   * the start time of the monitoring window
   * @param windowEnd
   * the end time of the monitoring window
   * @return
   */
  public AnomalyDetectionInputContextBuilder fetchScalingFactors(DateTime windowStart, DateTime windowEnd) {
    return fetchScalingFactors(anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis()));
  }
  public AnomalyDetectionInputContextBuilder fetchScalingFactors(List<Pair<Long, Long>> dataRangeIntervals) {
    List<ScalingFactor> scalingFactors = OverrideConfigHelper
        .getTimeSeriesScalingFactors(DAO_REGISTRY.getOverrideConfigDAO(), anomalyFunctionSpec.getCollection(),
            anomalyFunctionSpec.getMetric(), anomalyFunctionSpec.getId(),
            dataRangeIntervals);
    this.anomalyDetectionInputContext.setScalingFactors(scalingFactors);
    return this;
  }

  /**
   * Returns all known merged anomalies of the function id that are needed for anomaly detection, i.e., the merged
   * anomalies that overlap with the monitoring window and baseline windows.
   *
   * @param functionId the id of the anomaly function
   * @param startEndTimeRanges the time ranges for retrieving the known merge anomalies

   * @return known merged anomalies of the function id that are needed for anomaly detection
   */
  private List<MergedAnomalyResultDTO> getKnownMergedAnomalies(long functionId, List<Pair<Long, Long>> startEndTimeRanges, boolean loadRawAnomalies) {

    List<MergedAnomalyResultDTO> results = new ArrayList<>();
    for (Pair<Long, Long> startEndTimeRange : startEndTimeRanges) {
      try {
        results.addAll(
            DAO_REGISTRY.getMergedAnomalyResultDAO().findOverlappingByFunctionId(functionId, startEndTimeRange.getFirst(),
                startEndTimeRange.getSecond()));
      } catch (Exception e) {
        LOG.error("Exception in getting merged anomalies", e);
      }
    }

    return results;
  }

  /**
   * Returns all known merged anomalies of the function id that are needed for anomaly detection, i.e., the merged
   * anomalies that overlap with the monitoring window and baseline windows.
   *
   * @param functionId the id of the anomaly function
   * @param startEndTimeRanges the time ranges for retrieving the known merge anomalies

   * @return known merged anomalies of the function id that are needed for anomaly detection
   */
  private List<MergedAnomalyResultDTO> getKnownMergedAnomaliesByDimension(long functionId,
      List<Pair<Long, Long>> startEndTimeRanges, DimensionMap dimensions) {

    List<MergedAnomalyResultDTO> results = new ArrayList<>();
    for (Pair<Long, Long> startEndTimeRange : startEndTimeRanges) {
      try {
        results.addAll(
            DAO_REGISTRY.getMergedAnomalyResultDAO().findOverlappingByFunctionIdDimensions(functionId,
                startEndTimeRange.getFirst(), startEndTimeRange.getSecond(), dimensions.toString()));
      } catch (Exception e) {
        LOG.error("Exception in getting merged anomalies", e);
      }
    }

    return results;
  }

  /**
   * Get the metric filter setting for an anomaly function
   * @param filterString
   * @return
   */
  public static Multimap<String, String> getFiltersForFunction(String filterString) {
    // Get the original filter
    Multimap<String, String> filters;
    if (StringUtils.isNotBlank(filterString)) {
      filters = ThirdEyeDataUtils.getFilterSet(filterString);
    } else {
      filters = HashMultimap.create();
    }
    return filters;
  }

  /**
   * Get the explore dimensions for an anomaly function
   * @param anomalyFunctionSpec
   * @return
   */
  public static List<String> getDimensionsForFunction(AnomalyFunctionDTO anomalyFunctionSpec) {
    List<String> groupByDimensions;
    String exploreDimensionString = anomalyFunctionSpec.getExploreDimensions();
    if (StringUtils.isNotBlank(exploreDimensionString)) {
      groupByDimensions = Arrays.asList(exploreDimensionString.trim().split(","));
    } else {
      groupByDimensions = Collections.emptyList();
    }
    return groupByDimensions;
  }

  /**
   * Returns the set of metric time series that are needed by the given anomaly function for detecting anomalies.
   *
   * The time granularity is the granularity of the function's collection, i.e., the buckets are not aggregated,
   * in order to increase the accuracy for detecting anomalies.
   *
   * @param anomalyFunctionSpec spec of the anomaly function
   * @param startEndTimeRanges the time ranges to retrieve the data for constructing the time series
   * @param endTimeInclusive if the end time is included
   *
   * @return the data that is needed by the anomaly function for detecting anomalies.
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public Map<DimensionKey, MetricTimeSeries> getTimeSeriesForAnomalyDetection(
      AnomalyFunctionDTO anomalyFunctionSpec, List<Pair<Long, Long>> startEndTimeRanges, boolean endTimeInclusive)
      throws JobExecutionException, ExecutionException {

    Multimap<String, String> filters = getFiltersForFunction(anomalyFunctionSpec.getFilters());

    List<String> groupByDimensions = getDimensionsForFunction(anomalyFunctionSpec);

    TimeGranularity timeGranularity = new TimeGranularity(anomalyFunctionSpec.getBucketSize(),
        anomalyFunctionSpec.getBucketUnit());

    TimeSeriesResponse timeSeriesResponse =
        getTimeSeriesResponseImpl(anomalyFunctionSpec, startEndTimeRanges,
            timeGranularity, filters, groupByDimensions, endTimeInclusive);

    try {
      Map<DimensionKey, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap =
          TimeSeriesResponseConverter.toMap(timeSeriesResponse, Utils.getSchemaDimensionNames(anomalyFunctionSpec.getCollection()));
      return dimensionKeyMetricTimeSeriesMap;
    } catch (Exception e) {
      LOG.info("Failed to get schema dimensions for constructing dimension keys:", e.toString());
      return Collections.emptyMap();
    }
  }

  /**
   * Returns the metric time series that were given to the anomaly function for anomaly detection. If the dimension to
   * retrieve is OTHER, this method retrieves all combinations of dimensions and calculate the metric time series for
   * OTHER dimension on-the-fly.
   *
   * @param anomalyFunctionSpec spec of the anomaly function
   * @param startEndTimeRanges the time ranges to retrieve the data for constructing the time series
   * @param dimensionMap a dimension map that is used to construct the filter for retrieving the corresponding data
   *                     that was used to detected the anomaly
   * @param timeGranularity time granularity of the time series
   * @param endTimeInclusive set to true if the end time should be inclusive; mainly used by the query for UI
   * @return the time series in the same format as those used by the given anomaly function for anomaly detection
   *
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public MetricTimeSeries getTimeSeriesByDimension(AnomalyFunctionDTO anomalyFunctionSpec,
      List<Pair<Long, Long>> startEndTimeRanges, DimensionMap dimensionMap, TimeGranularity timeGranularity,
      boolean endTimeInclusive)
      throws JobExecutionException, ExecutionException {

    // Get the original filter
    Multimap<String, String> filters = getFiltersForFunction(anomalyFunctionSpec.getFilters());

    // Decorate filters according to dimensionMap
    filters = ThirdEyeUtils.getFilterSetFromDimensionMap(dimensionMap, filters);

    boolean hasOTHERDimensionName = false;
    for (String dimensionValue : dimensionMap.values()) {
      if (dimensionValue.equalsIgnoreCase(ResponseParserUtils.OTHER)) {
        hasOTHERDimensionName = true;
        break;
      }
    }

    // groupByDimensions (i.e., exploreDimensions) is empty by default because the query for getting the time series
    // will have the decorated filters according to anomalies' explore dimensions.
    // However, if there exists any dimension with value "OTHER, then we need to honor the origin groupBy in order to
    // construct the data for OTHER
    List<String> groupByDimensions = Collections.emptyList();
    if (hasOTHERDimensionName && StringUtils.isNotBlank(anomalyFunctionSpec.getExploreDimensions().trim())) {
      groupByDimensions = Arrays.asList(anomalyFunctionSpec.getExploreDimensions().trim().split(","));
    }

    TimeSeriesResponse response =
        getTimeSeriesResponseImpl(anomalyFunctionSpec, startEndTimeRanges,
            timeGranularity, filters, groupByDimensions, endTimeInclusive);
    try {
      Map<DimensionKey, MetricTimeSeries> metricTimeSeriesMap = TimeSeriesResponseConverter.toMap(response,
          Utils.getSchemaDimensionNames(anomalyFunctionSpec.getCollection()));
      return extractMetricTimeSeriesByDimension(metricTimeSeriesMap);
    } catch (Exception e) {
      LOG.warn("Unable to get schema dimension name for retrieving metric time series: {}", e.toString());
      return null;
    }
  }

  /**
   * Return a global metric for an anomaly function to calculate the contribution of anomalies to the global metric
   * @param anomalyFunctionSpec spec of the anomaly function
   * @param startEndTimeRanges the time ranges to retrieve the data for constructing the time series
   * @return the data that is needed by the anomaly function for knowing the global metric
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public MetricTimeSeries getGlobalMetric(AnomalyFunctionDTO anomalyFunctionSpec, List<Pair<Long, Long>> startEndTimeRanges)
      throws JobExecutionException, ExecutionException {
    // Set empty dimension and global metric filter
    Multimap<String, String> filters = getFiltersForFunction(anomalyFunctionSpec.getGlobalMetricFilters());
    List<String> groupByDimensions = Collections.emptyList();

    TimeGranularity timeGranularity = new TimeGranularity(anomalyFunctionSpec.getBucketSize(),
        anomalyFunctionSpec.getBucketUnit());
    DatasetConfigDTO dataset = DAORegistry.getInstance().getDatasetConfigDAO().findByDataset(anomalyFunctionSpec.getCollection());

    List<String> metricsToFetch = new ArrayList<>();
    if(StringUtils.isNotEmpty(anomalyFunctionSpec.getGlobalMetric())) {
      metricsToFetch.add(anomalyFunctionSpec.getGlobalMetric());
    } else {
      metricsToFetch.add(anomalyFunctionSpec.getMetric());
    }
    TimeSeriesResponse timeSeriesResponse =
        getTimeSeriesResponseImpl(anomalyFunctionSpec, metricsToFetch, startEndTimeRanges,
            timeGranularity, filters, groupByDimensions, false);

    MetricTimeSeries globalMetric = null;
    try {
      Map<DimensionKey, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap =
          TimeSeriesResponseConverter.toMap(timeSeriesResponse, Utils.getSchemaDimensionNames(anomalyFunctionSpec.getCollection()));

      if (MapUtils.isEmpty(dimensionKeyMetricTimeSeriesMap)) {
        LOG.error("Unable to fetch global metric for {}", anomalyFunctionSpec);
      }
      if (dimensionKeyMetricTimeSeriesMap.size() > 2) {
        LOG.warn("More than 1 dimensions when fetching traffic data for {}; take the 1st dimension", anomalyFunctionSpec);
      }
      globalMetric = dimensionKeyMetricTimeSeriesMap.values().iterator().next();
    } catch (Exception e) {
      LOG.warn("Failed to get schema dimensions for constructing dimension keys:", e.toString());
    }
    return globalMetric;
  }

  /**
   * Extract current and baseline values from the parsed Pinot results. There are two possible time series for presenting
   * the time series after anomaly detection: 1. the time series with a specific dimension and 2. the time series for
   * OTHER dimension.
   *
   * For case 1, the input map should contain only one time series and hence we can just return it. For case 2, the
   * input map would contain all combination of explored dimension and hence we need to filter out the one for OTHER
   * dimension.
   *
   * @param metricTimeSeriesMap
   *
   * @return the time series when the anomaly is detected
   */
  private MetricTimeSeries extractMetricTimeSeriesByDimension(Map<DimensionKey, MetricTimeSeries> metricTimeSeriesMap) {
    MetricTimeSeries metricTimeSeries = null;
    if (MapUtils.isNotEmpty(metricTimeSeriesMap)) {
      // For most anomalies, there should be only one time series due to its dimensions. The exception is the OTHER
      // dimension, in which time series of different dimensions are returned due to the calculation of OTHER dimension.
      // Therefore, we need to get the time series of OTHER dimension manually.
      if (metricTimeSeriesMap.size() == 1) {
        Iterator<MetricTimeSeries> ite = metricTimeSeriesMap.values().iterator();
        if (ite.hasNext()) {
          metricTimeSeries = ite.next();
        }
      } else { // Retrieve the time series of OTHER dimension
        Iterator<Map.Entry<DimensionKey, MetricTimeSeries>> ite = metricTimeSeriesMap.entrySet().iterator();
        while (ite.hasNext()) {
          Map.Entry<DimensionKey, MetricTimeSeries> entry = ite.next();
          DimensionKey dimensionKey = entry.getKey();
          boolean foundOTHER = false;
          for (String dimensionValue : dimensionKey.getDimensionValues()) {
            if (dimensionValue.equalsIgnoreCase(ResponseParserUtils.OTHER)) {
              metricTimeSeries = entry.getValue();
              foundOTHER = true;
              break;
            }
          }
          if (foundOTHER) {
            break;
          }
        }
      }
    }
    return metricTimeSeries;
  }

  private TimeSeriesResponse getTimeSeriesResponseImpl(AnomalyFunctionDTO anomalyFunctionSpec,
      List<Pair<Long, Long>> startEndTimeRanges, TimeGranularity timeGranularity, Multimap<String, String> filters,
      List<String> groupByDimensions, boolean endTimeInclusive)
      throws JobExecutionException, ExecutionException {
    return getTimeSeriesResponseImpl(anomalyFunctionSpec, anomalyFunctionSpec.getMetrics(), startEndTimeRanges,
        timeGranularity, filters, groupByDimensions, endTimeInclusive);
  }

  private TimeSeriesResponse getTimeSeriesResponseImpl(AnomalyFunctionDTO anomalyFunctionSpec, List<String> metrics,
      List<Pair<Long, Long>> startEndTimeRanges, TimeGranularity timeGranularity, Multimap<String, String> filters,
      List<String> groupByDimensions, boolean endTimeInclusive)
      throws JobExecutionException, ExecutionException {

    TimeSeriesHandler timeSeriesHandler =
        new TimeSeriesHandler(ThirdEyeCacheRegistry.getInstance().getQueryCache());

    // Seed request with top-level...
    TimeSeriesRequest seedRequest = new TimeSeriesRequest();
    seedRequest.setCollectionName(anomalyFunctionSpec.getCollection());
    // TODO: Check low level support for multiple metrics retrieval
    String metricsToRetrieve = StringUtils.join(metrics, ",");
    List<MetricExpression> metricExpressions = Utils
        .convertToMetricExpressions(metricsToRetrieve,
            anomalyFunctionSpec.getMetricFunction(), anomalyFunctionSpec.getCollection());
    seedRequest.setMetricExpressions(metricExpressions);
    seedRequest.setAggregationTimeGranularity(timeGranularity);
    seedRequest.setEndDateInclusive(false);
    seedRequest.setFilterSet(filters);
    seedRequest.setGroupByDimensions(groupByDimensions);
    seedRequest.setEndDateInclusive(endTimeInclusive);

    LOG.info("Found [{}] time ranges to fetch data for metric(s): {}, with filter: {}", startEndTimeRanges.size(), metricsToRetrieve, filters);

    // NOTE: another ThirdEye-esque hack. This code is to be deprecated, so no value in refactoring it.
    DateTimeZone timeZone = Utils.getDataTimeZone(anomalyFunctionSpec.getCollection());

    // MultiQuery request
    List<Future<TimeSeriesResponse>> futureResponses = new ArrayList<>();
    List<TimeSeriesRequest> requests = new ArrayList<>();
    Set<TimeSeriesRow> timeSeriesRowSet = new HashSet<>();
    for (Pair<Long, Long> startEndInterval : startEndTimeRanges) {
      TimeSeriesRequest request = new TimeSeriesRequest(seedRequest);
      DateTime startTime = new DateTime(startEndInterval.getFirst(), timeZone);
      DateTime endTime = new DateTime(startEndInterval.getSecond(), timeZone);
      request.setStart(startTime);
      request.setEnd(endTime);

      Future<TimeSeriesResponse> response = timeSeriesHandler.asyncHandle(request, new AnomalyDetectionTimeSeriesResponseParser());
      if (response != null) {
        futureResponses.add(response);
        requests.add(request);
        LOG.info("Fetching time series for range: [{} -- {}], metricExpressions: [{}], timeGranularity: [{}]",
            startTime, endTime, metricExpressions, timeGranularity);
      }
    }

    for (int i = 0; i < futureResponses.size(); i++) {
      Future<TimeSeriesResponse> futureResponse = futureResponses.get(i);
      TimeSeriesRequest request = requests.get(i);
      try {
        TimeSeriesResponse response = futureResponse.get();
        timeSeriesRowSet.addAll(response.getRows());
      } catch (InterruptedException e) {
        LOG.warn("Failed to fetch data with request: [{}]", request);
      }
    }

    timeSeriesHandler.shutdownAsyncHandler();

    List<TimeSeriesRow> timeSeriesRows = new ArrayList<>();
    timeSeriesRows.addAll(timeSeriesRowSet);

    return new TimeSeriesResponse(timeSeriesRows);
  }
}
