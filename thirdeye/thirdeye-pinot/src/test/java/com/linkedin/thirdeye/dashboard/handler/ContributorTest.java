package com.linkedin.thirdeye.dashboard.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewHandler;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewRequest;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;

/** Manual test for verifying code works as expected (ie without exceptions thrown) */
public class ContributorTest {
  public static void main(String[] args) throws Exception {
    ContributorViewRequest request = new ContributorViewRequest();

    String collection = "thirdeyeAbook";
    DateTime baselineStart = new DateTime(2016, 3, 23, 00, 00);
    List<MetricExpression> metricExpressions = new ArrayList<>();
    metricExpressions.add(new MetricExpression("__COUNT", "__COUNT"));

    request.setCollection(collection);
    request.setBaselineStart(baselineStart);
    request.setBaselineEnd(baselineStart.plusDays(1));
    request.setCurrentStart(baselineStart.plusDays(7));
    request.setCurrentEnd(baselineStart.plusDays(8));
    request.setTimeGranularity(new TimeGranularity(1, TimeUnit.HOURS));
    request.setMetricExpressions(metricExpressions);

    PinotThirdEyeClient pinotThirdEyeClient = PinotThirdEyeClient.getDefaultTestClient(); // TODO
                                                                                          // make
                                                                                          // this
    // configurable;
    Map<String, ThirdEyeClient> clientMap = new HashMap<>();
    clientMap.put(PinotThirdEyeClient.class.getSimpleName(), pinotThirdEyeClient);
    QueryCache queryCache = new QueryCache(clientMap, Executors.newFixedThreadPool(10));

    ContributorViewHandler handler = new ContributorViewHandler(queryCache);
    ContributorViewResponse response = handler.process(request);
    ObjectMapper mapper = new ObjectMapper();
    String jsonResponse = mapper.writeValueAsString(response);
    System.out.println(jsonResponse);
  }
}
