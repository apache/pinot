/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;

import com.google.common.collect.Maps;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.ThirdEyeResponseRow;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest.ThirdEyeRequestBuilder;

public class SeverityComputationUtil {

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();
  private String collectionName;
  private String metricName;

  public SeverityComputationUtil(String collectionName, String metricName) {
    this.collectionName = collectionName;
    this.metricName = metricName;
  }

  public Map<String, Object> computeSeverity(long currentWindowStart, long currentWindowEnd, long seasonalPeriod,
      long seasonCount)
      throws Exception {
    // CURRENT
    ThirdEyeRequest thirdEyeRequest = createThirdEyeRequest(currentWindowStart, currentWindowEnd);
    double currentSum = getSum(thirdEyeRequest);

    List<Pair<DateTime, DateTime>> intervals =
        getHistoryIntervals(currentWindowStart, currentWindowEnd, seasonalPeriod, seasonCount);
    double baselineSum = 0;
    int count = 0;
    for (Pair<DateTime, DateTime> pair : intervals) {
      thirdEyeRequest = createThirdEyeRequest(pair.getLeft().getMillis(), pair.getRight().getMillis());
      double sum = getSum(thirdEyeRequest);
      if (sum != 0d) {
        ++count;
        baselineSum += sum;
      }
    }
    double baselineSumAvg = baselineSum / count;
    double weight = (currentSum - baselineSumAvg) / baselineSumAvg;
    HashMap<String, Object> hashMap = Maps.newHashMap();
    hashMap.put("weight", weight);
    hashMap.put("currentSum", currentSum);
    hashMap.put("baselineSumAvg", baselineSumAvg);
    return hashMap;
  }

  private double getSum(ThirdEyeRequest thirdEyeRequest) throws Exception {
    double sum = 0;
    ThirdEyeResponse response = CACHE_REGISTRY.getQueryCache().getQueryResult(thirdEyeRequest);
    if (response.getNumRows() == 1) {
      ThirdEyeResponseRow row = response.getRow(0);
      sum = row.getMetrics().get(0);
    }
    return sum;
  }

  private ThirdEyeRequest createThirdEyeRequest(long currentWindowStart, long currentWindowEnd)
      throws ExecutionException {
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setStartTimeInclusive(new DateTime(currentWindowStart));
    requestBuilder.setEndTimeExclusive(new DateTime(currentWindowEnd));
    // requestBuilder.setFilterSet(comparisonRequest.getFilterSet());
    // requestBuilder.addGroupBy(comparisonRequest.getGroupByDimensions());
    requestBuilder.setGroupByTimeGranularity(null);

    List<MetricExpression> metricExpressions =
        Utils.convertToMetricExpressions(metricName, MetricAggFunction.SUM, collectionName);
    List<MetricFunction> metricFunctions = metricExpressions.get(0).computeMetricFunctions();
    requestBuilder.setMetricFunctions(metricFunctions);
    requestBuilder.setDataSource(ThirdEyeUtils.getDataSourceFromMetricFunctions(metricFunctions));
    ThirdEyeRequest thirdEyeRequest = requestBuilder.build("test-" + System.currentTimeMillis());
    return thirdEyeRequest;
  }

  private List<Pair<DateTime, DateTime>> getHistoryIntervals(long currentWindowStart, long currentWindowEnd,
      long seasonalPeriod, long seasonCount) {
    List<Pair<DateTime, DateTime>> intervals = new ArrayList<>();
    for (int i = 1; i <= seasonCount; ++i) {
      intervals.add(ImmutablePair.of(new DateTime(currentWindowStart).minus(seasonalPeriod * i), new DateTime(currentWindowEnd).minus(seasonalPeriod * i)));
    }
    return intervals;
  }
}
