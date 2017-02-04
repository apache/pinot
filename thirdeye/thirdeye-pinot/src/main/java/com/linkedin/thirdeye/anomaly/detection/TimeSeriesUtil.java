package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ResponseParserUtils;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class TimeSeriesUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesUtil.class);

  private TimeSeriesUtil() {
  }

  /**
   * Returns the set of metric time series that are needed by the given anomaly function for detecting anomalies.
   *
   * The time granularity is the granularity of the function's collection, i.e., the buckets are not aggregated,
   * in order to increase the accuracy for detecting anomalies.
   *
   * @param anomalyFunctionSpec spec of the anomaly function
   * @param startEndTimeRanges the time ranges to retrieve the data for constructing the time series
   *
   * @return the data that is needed by the anomaly function for detecting anomalies.
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public static Map<DimensionKey, MetricTimeSeries> getTimeSeriesForAnomalyDetection(
      AnomalyFunctionDTO anomalyFunctionSpec, List<Pair<Long, Long>> startEndTimeRanges)
      throws JobExecutionException, ExecutionException {

    String filterString = anomalyFunctionSpec.getFilters();
    Multimap<String, String> filters;
    if (StringUtils.isNotBlank(filterString)) {
      filters = ThirdEyeUtils.getFilterSet(filterString);
    } else {
      filters = HashMultimap.create();
    }

    List<String> groupByDimensions;
    String exploreDimensionString = anomalyFunctionSpec.getExploreDimensions();
    if (StringUtils.isNotBlank(exploreDimensionString)) {
      groupByDimensions = Arrays.asList(exploreDimensionString.trim().split(","));
    } else {
      groupByDimensions = Collections.emptyList();
    }

    TimeGranularity timeGranularity = new TimeGranularity(anomalyFunctionSpec.getBucketSize(),
        anomalyFunctionSpec.getBucketUnit());

    TimeSeriesResponse timeSeriesResponse =
      getTimeSeriesResponseImpl(anomalyFunctionSpec, startEndTimeRanges, timeGranularity, filters, groupByDimensions);

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
   *
   * @return the time series in the same format as those used by the given anomaly function for anomaly detection
   *
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public static MetricTimeSeries getTimeSeriesByDimension(AnomalyFunctionDTO anomalyFunctionSpec,
      List<Pair<Long, Long>> startEndTimeRanges, DimensionMap dimensionMap, TimeGranularity timeGranularity)
      throws JobExecutionException, ExecutionException {

    // Get the original filter
    Multimap<String, String> filters;
    String filterString = anomalyFunctionSpec.getFilters();
    if (StringUtils.isNotBlank(filterString)) {
      filters = ThirdEyeUtils.getFilterSet(filterString);
    } else {
      filters = HashMultimap.create();
    }

    // Decorate filters according to dimensionMap
    filters = ThirdEyeUtils.getFilterSetFromDimensionMap(dimensionMap, filters);

    boolean hasOTHERDimensionName = false;
    for (String dimensionValue : dimensionMap.values()) {
      if (dimensionValue.equals(ResponseParserUtils.OTHER)) {
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
        getTimeSeriesResponseImpl(anomalyFunctionSpec, startEndTimeRanges, timeGranularity, filters, groupByDimensions);
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
  private static MetricTimeSeries extractMetricTimeSeriesByDimension(Map<DimensionKey, MetricTimeSeries> metricTimeSeriesMap) {
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

            if (dimensionValue.equals(ResponseParserUtils.OTHER)) {
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

  private static TimeSeriesResponse getTimeSeriesResponseImpl(AnomalyFunctionDTO anomalyFunctionSpec,
      List<Pair<Long, Long>> startEndTimeRanges, TimeGranularity timeGranularity, Multimap<String, String> filters,
      List<String> groupByDimensions)
      throws JobExecutionException, ExecutionException {

    TimeSeriesHandler timeSeriesHandler =
        new TimeSeriesHandler(ThirdEyeCacheRegistry.getInstance().getQueryCache());

    // Seed request with top-level...
    TimeSeriesRequest request = new TimeSeriesRequest();
    request.setCollectionName(anomalyFunctionSpec.getCollection());
    List<MetricExpression> metricExpressions = Utils
        .convertToMetricExpressions(anomalyFunctionSpec.getMetric(),
            anomalyFunctionSpec.getMetricFunction(), anomalyFunctionSpec.getCollection());
    request.setMetricExpressions(metricExpressions);
    request.setAggregationTimeGranularity(timeGranularity);
    request.setEndDateInclusive(false);
    request.setFilterSet(filters);
    request.setGroupByDimensions(groupByDimensions);

    LOG.info("Found [{}] time ranges to fetch data", startEndTimeRanges.size());
    for (Pair<Long, Long> timeRange : startEndTimeRanges) {
      LOG.info("Start Time [{}], End Time [{}] for anomaly analysis", new DateTime(timeRange.getFirst()),
          new DateTime(timeRange.getSecond()));
    }

    Set<TimeSeriesRow> timeSeriesRowSet = new HashSet<>();
    // TODO : replace this with Pinot MultiQuery Request
    for (Pair<Long, Long> startEndInterval : startEndTimeRanges) {
      DateTime startTime = new DateTime(startEndInterval.getFirst());
      DateTime endTime = new DateTime(startEndInterval.getSecond());
      request.setStart(startTime);
      request.setEnd(endTime);

      LOG.info(
          "Fetching data with startTime: [{}], endTime: [{}], metricExpressions: [{}], timeGranularity: [{}]",
          startTime, endTime, metricExpressions, timeGranularity);

      try {
        LOG.debug("Executing {}", request);
        TimeSeriesResponse response = timeSeriesHandler.handle(request);
        timeSeriesRowSet.addAll(response.getRows());
      } catch (Exception e) {
        throw new JobExecutionException(e);
      }
    }
    List<TimeSeriesRow> timeSeriesRows = new ArrayList<>();
    timeSeriesRows.addAll(timeSeriesRowSet);

    return new TimeSeriesResponse(timeSeriesRows);
  }
}
