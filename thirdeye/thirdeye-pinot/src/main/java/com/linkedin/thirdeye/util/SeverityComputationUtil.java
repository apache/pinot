package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.client.MetricExpression;
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
import org.joda.time.DateTimeZone;

import com.google.common.collect.Maps;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponseRow;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeResponse;
import com.linkedin.thirdeye.constant.MetricAggFunction;

public class SeverityComputationUtil {

  private PinotThirdEyeClient pinotThirdEyeClient;
  private String collectionName;
  private String metricName;

  public SeverityComputationUtil(PinotThirdEyeClient pinotThirdEyeClient, String collectionName, String metricName) {
    this.pinotThirdEyeClient = pinotThirdEyeClient;
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
    PinotThirdEyeResponse response = pinotThirdEyeClient.execute(thirdEyeRequest);
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

  private List<Pair<DateTime, DateTime>> getIntervals(long currentWindowStart, long currentWindowEnd, String compareMode) {
    if (compareMode.equals("WO3WMean")) {
      List<Pair<DateTime, DateTime>> intervals = new ArrayList<>();
      intervals.add(ImmutablePair.of(new DateTime(currentWindowStart).minusDays(7), new DateTime(currentWindowEnd).minusDays(7)));
      intervals.add(ImmutablePair.of(new DateTime(currentWindowStart).minusDays(14), new DateTime(currentWindowEnd).minusDays(14)));
      intervals.add(ImmutablePair.of(new DateTime(currentWindowStart).minusDays(21), new DateTime(currentWindowEnd).minusDays(21)));
      return intervals;
    }
    return Collections.emptyList();

  }

  public static void main(String[] args) throws Exception {
    PinotThirdEyeClient thirdEyeClient = PinotThirdEyeClient.getDefaultTestClient();
//    SeverityComputationUtil util = new SeverityComputationUtil(thirdEyeClient, "mny-ads-su-kpi-alerts", "id542749");
    SeverityComputationUtil util = new SeverityComputationUtil(thirdEyeClient, "mny-ads-su-kpi-alerts", "SU_Impression_Count_on_iOS");
    long currentWindowStart = new DateTime(2016, 11, 02, 0, 0, DateTimeZone.UTC).getMillis();
    long currentWindowEnd = new DateTime(2016, 11, 02, 4, 0, DateTimeZone.UTC).getMillis();
    String compareMode = "WO3WMean";
    Map<String, Object> severity = util.computeSeverity(currentWindowStart, currentWindowEnd, compareMode);
    System.out.println(severity);
  }
}
