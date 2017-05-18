package com.linkedin.thirdeye.datasource.comparison;

import static com.linkedin.thirdeye.datasource.comparison.TimeOnTimeConstants.ALL_BASELINE;
import static com.linkedin.thirdeye.datasource.comparison.TimeOnTimeConstants.ALL_CURRENT;
import static com.linkedin.thirdeye.datasource.comparison.TimeOnTimeConstants.DIMENSION_BASELINE;
import static com.linkedin.thirdeye.datasource.comparison.TimeOnTimeConstants.DIMENSION_CURRENT;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest.ThirdEyeRequestBuilder;

public class ThirdEyeRequestGenerator {

  public static List<ThirdEyeRequest> generateRequestsForGroupByDimensions(
      TimeOnTimeComparisonRequest comparisonRequest) {
    List<ThirdEyeRequest> requests = new ArrayList<>();
    ThirdEyeRequest baselineALLRequest = generateRequest(ALL_BASELINE, comparisonRequest,
        comparisonRequest.getBaselineStart(), comparisonRequest.getBaselineEnd(), null, null);
    requests.add(baselineALLRequest);
    ThirdEyeRequest currentALLRequest = generateRequest(ALL_CURRENT, comparisonRequest,
        comparisonRequest.getCurrentStart(), comparisonRequest.getCurrentEnd(), null, null);
    requests.add(currentALLRequest);
    for (String dimension : comparisonRequest.getGroupByDimensions()) {
      ThirdEyeRequest baselineDimensionRequest = generateRequest(DIMENSION_BASELINE + dimension,
          comparisonRequest, comparisonRequest.getBaselineStart(),
          comparisonRequest.getBaselineEnd(), dimension, null);
      requests.add(baselineDimensionRequest);
      ThirdEyeRequest currentDimensionRequest = generateRequest(DIMENSION_CURRENT + dimension,
          comparisonRequest, comparisonRequest.getCurrentStart(), comparisonRequest.getCurrentEnd(),
          dimension, null);
      requests.add(currentDimensionRequest);
    }
    return requests;
  }

  public static List<ThirdEyeRequest> generateRequestsForAggregation(
      TimeOnTimeComparisonRequest comparisonRequest) {
    List<ThirdEyeRequest> requests = new ArrayList<>();

    ThirdEyeRequest baselineALLRequest =
        ThirdEyeRequestGenerator.generateRequest(ALL_BASELINE, comparisonRequest,
            comparisonRequest.getBaselineStart(), comparisonRequest.getBaselineEnd(), null, null);
    requests.add(baselineALLRequest);
    ThirdEyeRequest currentALLRequest =
        ThirdEyeRequestGenerator.generateRequest(ALL_CURRENT, comparisonRequest,
            comparisonRequest.getCurrentStart(), comparisonRequest.getCurrentEnd(), null, null);
    requests.add(currentALLRequest);
    return requests;
  }

  public static ThirdEyeRequest generateRequest(String name,
      TimeOnTimeComparisonRequest comparisonRequest, DateTime start, DateTime end,
      String groupByDimension, TimeGranularity aggTimeGranularity) {
    ThirdEyeRequestBuilder requestBuilder = new ThirdEyeRequestBuilder();
    // COMMON to ALL REQUESTS
    requestBuilder.setFilterSet(comparisonRequest.getFilterSet());
    requestBuilder.setFilterClause(comparisonRequest.getFilterClause());

    List<MetricFunction> metricFunctionsFromExpressions =
        Utils.computeMetricFunctionsFromExpressions(comparisonRequest.getMetricExpressions());
    requestBuilder.setMetricFunctions(metricFunctionsFromExpressions);

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
