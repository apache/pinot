package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.dashboard.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;

import com.google.common.collect.Maps;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponseRow;
import com.linkedin.thirdeye.constant.MetricAggFunction;

public class SeverityComputationUtil {

  private ThirdEyeClient thirdEyeClient;
  private String collectionName;
  private String metricName;

  public SeverityComputationUtil(ThirdEyeClient thirdEyeClient, String collectionName, String metricName) {
    this.thirdEyeClient = thirdEyeClient;
    this.collectionName = collectionName;
    this.metricName = metricName;
  }

  public Map<String, Object> computeSeverity(long currentWindowStart, long currentWindowEnd, String compareMode) throws Exception {
    // CURRENT
    ThirdEyeRequest thirdEyeRequest = createThirdEyeRequest(currentWindowStart, currentWindowEnd);
    double currentSum = getSum(thirdEyeRequest);

    List<Pair<DateTime, DateTime>> intervals = getIntervals(currentWindowStart, currentWindowEnd, compareMode);
    double baselineSum = 0;
    for (Pair<DateTime, DateTime> pair : intervals) {
      thirdEyeRequest = createThirdEyeRequest(pair.getLeft().getMillis(), pair.getRight().getMillis());
      baselineSum += getSum(thirdEyeRequest);
    }
    double baselineSumAvg = baselineSum / intervals.size();
    double severity = (currentSum - baselineSumAvg) / baselineSumAvg;
    HashMap<String, Object> hashMap = Maps.newHashMap();
    hashMap.put("severity", severity);
    hashMap.put("currentSum", currentSum);
    hashMap.put("baselineSumAvg", baselineSumAvg);
    return hashMap;
  }

  private double getSum(ThirdEyeRequest thirdEyeRequest) throws Exception {
    double sum = 0;
    ThirdEyeResponse response = thirdEyeClient.execute(thirdEyeRequest);
    if (response.getNumRows() == 1) {
      ThirdEyeResponseRow row = response.getRow(0);
      sum = row.getMetrics().get(0);
    }
    return sum;
  }

  private ThirdEyeRequest createThirdEyeRequest(long currentWindowStart, long currentWindowEnd)
      throws ExecutionException {
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setCollection(collectionName);
    requestBuilder.setStartTimeInclusive(new DateTime(currentWindowStart));
    requestBuilder.setEndTimeExclusive(new DateTime(currentWindowEnd));
    // requestBuilder.setFilterSet(comparisonRequest.getFilterSet());
    // requestBuilder.addGroupBy(comparisonRequest.getGroupByDimensions());
    requestBuilder.setGroupByTimeGranularity(null);

    List<MetricExpression> metricExpressions =
        Utils.convertToMetricExpressions(metricName, MetricAggFunction.SUM, collectionName);
    requestBuilder.setMetricFunctions(metricExpressions.get(0).computeMetricFunctions());

    ThirdEyeRequest thirdEyeRequest = requestBuilder.build("test-" + System.currentTimeMillis());
    return thirdEyeRequest;
  }

  // TODO: Add more compare mode
  private List<Pair<DateTime, DateTime>> getIntervals(long currentWindowStart, long currentWindowEnd, String compareMode) {
    if (compareMode.equals("WO4WMean")) {
      List<Pair<DateTime, DateTime>> intervals = new ArrayList<>();
      intervals.add(ImmutablePair.of(new DateTime(currentWindowStart).minusDays(7), new DateTime(currentWindowEnd).minusDays(7)));
      intervals.add(ImmutablePair.of(new DateTime(currentWindowStart).minusDays(14), new DateTime(currentWindowEnd).minusDays(14)));
      intervals.add(ImmutablePair.of(new DateTime(currentWindowStart).minusDays(21), new DateTime(currentWindowEnd).minusDays(21)));
      intervals.add(ImmutablePair.of(new DateTime(currentWindowStart).minusDays(28), new DateTime(currentWindowEnd).minusDays(28)));
      return intervals;
    }
    return Collections.emptyList();
  }
}
