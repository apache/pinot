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

package org.apache.pinot.thirdeye.datasource.comparison;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.comparison.Row.Metric;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;

/** Manual test for verifying code works as expected (ie without exceptions thrown) */
public class TimeOnTimeTest {
  public static void main(String[] args) throws Exception {
    PinotThirdEyeDataSource pinotThirdEyeDataSource = PinotThirdEyeDataSource.getDefaultTestDataSource(); // TODO
                                                                                          // make
                                                                                          // this
    // configurable;
    // PinotThirdEyeDataSource pinotThirdEyeDataSource =
    // PinotThirdEyeDataSource.fromHostList("localhost", 8100, "localhost:8099");
    Map<String, ThirdEyeDataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put(PinotThirdEyeDataSource.class.getSimpleName(), pinotThirdEyeDataSource);

    QueryCache queryCache = new QueryCache(dataSourceMap, Executors.newFixedThreadPool(10));
    // QueryCache queryCache = new QueryCache(pinotThirdEyeDataSource, Executors.newCachedThreadPool());

    TimeOnTimeComparisonRequest comparisonRequest;
    comparisonRequest = generateGroupByTimeRequest();
    // comparisonRequest = generateGroupByDimensionRequest();
    // comparisonRequest = generateGroupByTimeAndDimension();

    TimeOnTimeComparisonHandler handler = new TimeOnTimeComparisonHandler(queryCache);
    // long start;
    // long end;
    // // Thread.sleep(30000);
    // for (int i = 0; i < 5; i++) {
    // start = System.currentTimeMillis();
    // handler.handle(comparisonRequest);
    // end = System.currentTimeMillis();
    // System.out.println("Time taken:" + (end - start));
    // }
    // System.exit(0);

    long start = System.currentTimeMillis();
    TimeOnTimeComparisonResponse response = handler.handle(comparisonRequest);
    long end = System.currentTimeMillis();
    System.out.println("Time taken:" + (end - start));
    for (Metric metric : response.getRow(0).getMetrics()) {
      System.out.print(metric.getMetricName() + "\t\t");
    }
    System.out.println();
    for (int i = 0; i < response.getNumRows(); i++) {
      System.out.println(response.getRow(i));
    }
    System.exit(0);
  }

  // TABULAR
  private static TimeOnTimeComparisonRequest generateGroupByTimeRequest() {
    TimeOnTimeComparisonRequest comparisonRequest = new TimeOnTimeComparisonRequest();
    String collection = "thirdeyeAbook";
    comparisonRequest.setCollectionName(collection);
    comparisonRequest.setBaselineStart(new DateTime(2016, 4, 1, 00, 00));
    comparisonRequest.setBaselineEnd(new DateTime(2016, 4, 2, 00, 00));

    comparisonRequest.setCurrentStart(new DateTime(2016, 4, 8, 00, 00));
    comparisonRequest.setCurrentEnd(new DateTime(2016, 4, 9, 00, 00));
    List<MetricFunction> metricFunctions = new ArrayList<>();
    metricFunctions.add(new MetricFunction(MetricAggFunction.SUM, "__COUNT", null, collection, null, null));
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metricFunctions);
    metricExpressions.add(new MetricExpression("submit_rate", "submits/impressions"));
    comparisonRequest.setMetricExpressions(metricExpressions);
    comparisonRequest.setAggregationTimeGranularity(new TimeGranularity(1, TimeUnit.HOURS));
    return comparisonRequest;
  }

  // HEATMAP
  private static TimeOnTimeComparisonRequest generateGroupByDimensionRequest() {
    TimeOnTimeComparisonRequest comparisonRequest = new TimeOnTimeComparisonRequest();
    String collection = "thirdeyeAbook";
    comparisonRequest.setCollectionName(collection);
    comparisonRequest.setBaselineStart(new DateTime(2016, 4, 1, 00, 00));
    comparisonRequest.setBaselineEnd(new DateTime(2016, 4, 1, 01, 00));

    comparisonRequest.setCurrentStart(new DateTime(2016, 4, 8, 00, 00));
    comparisonRequest.setCurrentEnd(new DateTime(2016, 4, 8, 01, 00));
    comparisonRequest.setGroupByDimensions(
        Lists.newArrayList("browserName", "contactsOrigin", "deviceName", "continent",
            "countryCode", "environment", "locale", "osName", "pageKey", "source", "sourceApp"));
    List<MetricFunction> metricFunctions = new ArrayList<>();
    metricFunctions.add(new MetricFunction(MetricAggFunction.SUM, "__COUNT", null, collection, null, null));
    comparisonRequest.setMetricExpressions(Utils.convertToMetricExpressions(metricFunctions));
    comparisonRequest.setAggregationTimeGranularity(null);
    return comparisonRequest;
  }

  // CONTRIBUTOR
  private static TimeOnTimeComparisonRequest generateGroupByTimeAndDimension() {
    TimeOnTimeComparisonRequest comparisonRequest = new TimeOnTimeComparisonRequest();
    String collection = "thirdeyeAbook";
    comparisonRequest.setCollectionName(collection);
    comparisonRequest.setBaselineStart(new DateTime(2016, 4, 1, 00, 00));
    comparisonRequest.setBaselineEnd(new DateTime(2016, 4, 2, 00, 00));

    comparisonRequest.setCurrentStart(new DateTime(2016, 4, 8, 00, 00));
    comparisonRequest.setCurrentEnd(new DateTime(2016, 4, 9, 00, 00));
    comparisonRequest.setGroupByDimensions(
        Lists.newArrayList("browserName", "contactsOrigin", "deviceName", "continent",
            "countryCode", "environment", "locale", "osName", "pageKey", "source", "sourceApp"));
    comparisonRequest.setGroupByDimensions(Lists.newArrayList("environment"));

    List<MetricFunction> metricFunctions = new ArrayList<>();
    metricFunctions.add(new MetricFunction(MetricAggFunction.SUM, "__COUNT", null, collection, null, null));
    comparisonRequest.setMetricExpressions(Utils.convertToMetricExpressions(metricFunctions));
    comparisonRequest.setAggregationTimeGranularity(new TimeGranularity(1, TimeUnit.HOURS));
    return comparisonRequest;
  }
}
