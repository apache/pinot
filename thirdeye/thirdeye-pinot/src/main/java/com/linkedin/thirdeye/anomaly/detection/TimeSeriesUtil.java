package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
   * Returns the time series that are needed by the given anomaly function for detecting anomalies.
   *
   * @param anomalyFunction the anomaly function for detecting anomalies
   * @param monitoringWindowStart inclusive
   * @param monitoringWindowEnd exclusive
   * @return the data that is needed by the anomaly function for detecting anomalies.
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public static TimeSeriesResponse getTimeSeriesResponseForAnomalyDetection(BaseAnomalyFunction anomalyFunction,
      long monitoringWindowStart, long monitoringWindowEnd)
      throws JobExecutionException, ExecutionException {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunction.getSpec();

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

    return getTimeSeriesResponseImpl(anomalyFunction, timeGranularity, filters, groupByDimensions, monitoringWindowStart,
        monitoringWindowEnd);
  }

  /**
   * Returns the time series that were used by the given anomaly function for detecting the anomaly.
   *
   * @param anomalyFunction the anomaly function that detects the anomaly
   * @param dimensionKeyString the string representation of {@link com.linkedin.thirdeye.api.DimensionKey}, which is
   *                           used to construct the filter for retrieving the corresponding data that was used to
   *                           detected the anomaly
   * @param timeGranularity time granularity for the frontend
   * @param viewWindowStart inclusive
   * @param viewWindowEnd exclusive
   * @return the time series that were used by the given anomaly function for detecting the anomaly
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public static TimeSeriesResponse getTimeSeriesResponseForPresentation(BaseAnomalyFunction anomalyFunction,
      String dimensionKeyString, TimeGranularity timeGranularity, long viewWindowStart, long viewWindowEnd)
      throws JobExecutionException, ExecutionException {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunction.getSpec();

    // Get the original filter
    Multimap<String, String> filters;
    String filterString = anomalyFunctionSpec.getFilters();
    if (StringUtils.isNotBlank(filterString)) {
      filters = ThirdEyeUtils.getFilterSet(filterString);
    } else {
      filters = HashMultimap.create();
    }

    // Decorate the filter according to dimensionKeyString
    filters = ThirdEyeUtils.getFilterSetFromDimensionKeyString(dimensionKeyString, anomalyFunctionSpec.getCollection(), filters);

    // groupByDimensions (i.e., exploreDimensions) should be empty when retrieving time series for anomalies, because
    // each anomaly should have a time series with a specific set of dimension values, which is specified in filters.
    List<String> groupByDimensions = Collections.emptyList();

    return getTimeSeriesResponseImpl(anomalyFunction, timeGranularity, filters, groupByDimensions, viewWindowStart,
        viewWindowEnd);
  }

  private static TimeSeriesResponse getTimeSeriesResponseImpl(BaseAnomalyFunction anomalyFunction,
      TimeGranularity timeGranularity, Multimap<String, String> filters, List<String> groupByDimensions,
      long monitoringWindowStart, long monitoringWindowEnd)
      throws JobExecutionException, ExecutionException {

    TimeSeriesHandler timeSeriesHandler =
        new TimeSeriesHandler(ThirdEyeCacheRegistry.getInstance().getQueryCache());

    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunction.getSpec();

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

    List<Pair<Long, Long>> startEndTimeRanges =
        anomalyFunction.getDataRangeIntervals(monitoringWindowStart, monitoringWindowEnd);

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
