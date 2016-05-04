package com.linkedin.thirdeye.client.timeseries;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;

/**
 * TODO figure out a better name.
 * TODO neither of these methods currently support grouping (by dimension nor time)? Is it even
 * supposed to? looks like handler might do that.
 * TODO should this class even be visible outside the package?
 * @author jteoh
 */
public class TimeSeriesThirdEyeRequestGenerator {
  // copied mostly from com.linkedin.thirdeye.client.comparison.ThirdEyeRequestGenerator

  public static final String ALL = "ALL";

  /**
   * Generates requests for the total query in addition to a groupBy for each dimension specified in
   * the TimeSeriesRequest.
   * TODO currently does not support time granularity aggregation (simply add in?)
   * TODO validate input to ensure groupBy exists.
   */
  public List<ThirdEyeRequest> generateRequestsForGroupByDimensions(
      TimeSeriesRequest timeSeriesRequest) {
    List<ThirdEyeRequest> requests = new ArrayList<>();
    ThirdEyeRequest allRequest = generateRequest(ALL, timeSeriesRequest,
        timeSeriesRequest.getStart(), timeSeriesRequest.getEnd(), null, null);
    requests.add(allRequest);
    for (String dimension : timeSeriesRequest.getGroupByDimensions()) {
      ThirdEyeRequest dimensionRequest = generateRequest(dimension, timeSeriesRequest,
          timeSeriesRequest.getStart(), timeSeriesRequest.getEnd(), dimension, null);
      requests.add(dimensionRequest);
    }
    return requests;
  }

  /**
   * Creates a single ThirdEyeRequest for the full time range specified by the TimeSeriesRequest.
   * TODO currently does not support time granularity aggregation.
   * TODO need to validate that the input doesn't have a groupBy.
   */
  public ThirdEyeRequest generateRequestsForAggregation(TimeSeriesRequest timeSeriesRequest) {
    ThirdEyeRequest request = generateRequest(ALL, timeSeriesRequest, timeSeriesRequest.getStart(),
        timeSeriesRequest.getEnd(), null, null);
    return request;
  }

  /** Internal helper method for request generation. */
  ThirdEyeRequest generateRequest(String name, TimeSeriesRequest timeSeriesRequest, DateTime start,
      DateTime end, String groupByDimension, TimeGranularity aggTimeGranularity) {
    ThirdEyeRequestBuilder requestBuilder = new ThirdEyeRequestBuilder();
    // COMMON to ALL REQUESTS
    requestBuilder.setCollection(timeSeriesRequest.getCollectionName());
    requestBuilder.setFilterSet(timeSeriesRequest.getFilterSet());
    requestBuilder.setFilterClause(timeSeriesRequest.getFilterClause());
    requestBuilder.setMetricFunctions(timeSeriesRequest.getMetricFunctions());

    // REQUEST to get total value with out break down.
    requestBuilder.setStartTimeInclusive(start);
    requestBuilder.setEndTimeExclusive(end);
    if (groupByDimension != null) {
      requestBuilder.setGroupBy(groupByDimension);
    }
    if (aggTimeGranularity != null) {
      requestBuilder.setGroupByTimeGranularity(aggTimeGranularity);
    }
    ThirdEyeRequest request = requestBuilder.build(name);
    return request;
  }
}
