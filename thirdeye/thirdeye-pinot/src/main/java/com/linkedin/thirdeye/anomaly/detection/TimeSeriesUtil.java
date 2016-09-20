package com.linkedin.thirdeye.anomaly.detection;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TimeSeriesUtil {

  private static TimeSeriesHandler timeSeriesHandler =
      new TimeSeriesHandler(ThirdEyeCacheRegistry.getInstance().getQueryCache());

  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesUtil.class);

  private TimeSeriesUtil() {
  }

  public static TimeSeriesResponse getTimeSeriesResponse(AnomalyFunctionDTO anomalyFunctionSpec,
      BaseAnomalyFunction anomalyFunction, String groupByDimension, long windowStart,
      long windowEnd) throws JobExecutionException {
    // Compute metric function
    TimeGranularity timeGranularity = new TimeGranularity(anomalyFunctionSpec.getBucketSize(),
        anomalyFunctionSpec.getBucketUnit());

    // Filters
    String filters = anomalyFunctionSpec.getFilters();

    // Seed request with top-level...
    TimeSeriesRequest request = new TimeSeriesRequest();
    request.setCollectionName(anomalyFunctionSpec.getCollection());
    List<MetricExpression> metricExpressions = Utils
        .convertToMetricExpressions(anomalyFunctionSpec.getMetric(),
            anomalyFunctionSpec.getMetricFunction(), anomalyFunctionSpec.getCollection());
    request.setMetricExpressions(metricExpressions);
    request.setAggregationTimeGranularity(timeGranularity);
    request.setEndDateInclusive(false);

    if (StringUtils.isNotBlank(filters)) {
      request.setFilterSet(ThirdEyeUtils.getFilterSet(filters));
    }
    if (StringUtils.isNotBlank(groupByDimension)) {
      request.setGroupByDimensions(Collections.singletonList(groupByDimension));
    }
    List<Pair<Long, Long>> startEndTimeRanges =
        anomalyFunction.getDataRangeIntervals(windowStart, windowEnd);

    Set<TimeSeriesRow> timeSeriesRowSet = new HashSet<>();
    // TODO : replace this with Pinot MultiQuery Request
    for (Pair<Long, Long> startEndInterval : getOptimizedTimeRanges(startEndTimeRanges)) {
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

  private static List<Pair<Long, Long>> getOptimizedTimeRanges(
      List<Pair<Long, Long>> startEndTimeRanges) {
    List<Pair<Long, Long>> optimizedTimeRanges;
    if (startEndTimeRanges.size() > 1) {
      // optimize timeRange pairs for overlapped time
      optimizedTimeRanges = new ArrayList<>();
      Pair<Long, Long> lastTimePair = startEndTimeRanges.get(0);
      for (int i = 1; i < startEndTimeRanges.size(); i++) {
        Pair<Long, Long> currentTimePair = startEndTimeRanges.get(i);
        if (lastTimePair.getSecond() < currentTimePair.getFirst()) {
          optimizedTimeRanges.add(lastTimePair);
          lastTimePair = currentTimePair;
        } else {
          if (lastTimePair.getSecond() < currentTimePair.getSecond()) {
            lastTimePair.setSecond(currentTimePair.getSecond());
          }
        }
        if (i == startEndTimeRanges.size() - 1) {
          optimizedTimeRanges.add(lastTimePair);
        }
      }
    } else {
      optimizedTimeRanges = startEndTimeRanges;
    }
    return optimizedTimeRanges;
  }
}
