package com.linkedin.thirdeye.client.comparison;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.comparison.Row.Metric;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;

/** Manual test for verifying code works as expected (ie without exceptions thrown) */
public class TimeOnTimeTest {
  public static void main(String[] args) throws Exception {
    PinotThirdEyeClient pinotThirdEyeClient = PinotThirdEyeClient.getDefaultTestClient(); // TODO
                                                                                          // make
                                                                                          // this
    // configurable;
    // PinotThirdEyeClient pinotThirdEyeClient =
    // PinotThirdEyeClient.fromHostList("localhost", 8100, "localhost:8099");

    QueryCache queryCache = new QueryCache(pinotThirdEyeClient, Executors.newFixedThreadPool(10));
    // QueryCache queryCache = new QueryCache(pinotThirdEyeClient, Executors.newCachedThreadPool());

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
    metricFunctions.add(new MetricFunction(MetricAggFunction.SUM, "__COUNT", null, collection, null));
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
    metricFunctions.add(new MetricFunction(MetricAggFunction.SUM, "__COUNT", null, collection, null));
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
    metricFunctions.add(new MetricFunction(MetricAggFunction.SUM, "__COUNT", null, collection, null));
    comparisonRequest.setMetricExpressions(Utils.convertToMetricExpressions(metricFunctions));
    comparisonRequest.setAggregationTimeGranularity(new TimeGranularity(1, TimeUnit.HOURS));
    return comparisonRequest;
  }
}
