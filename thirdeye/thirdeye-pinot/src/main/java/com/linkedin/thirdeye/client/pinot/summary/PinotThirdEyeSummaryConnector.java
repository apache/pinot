package com.linkedin.thirdeye.client.pinot.summary;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.client.pinot.summary.calculator.SummaryCalculator;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;


public class PinotThirdEyeSummaryConnector {
  private final static String oFileName = "Cube.json";

  private static Cube buildDataCube(ThirdEyeResponse responseGa, ThirdEyeResponse responseGb,
      ThirdEyeResponse responseTa, ThirdEyeResponse responseTb) {
    Cube records = new Cube();
    records.setGa(responseGa.getRow(0).getMetrics().get(0));
    records.setGb(responseGb.getRow(0).getMetrics().get(0));
    records.setRg(records.getGb() / records.getGa());
    Map<String, Integer> tabsTable = new HashMap<>();
    for (int i = 0; i < responseTa.getNumRows(); ++i) {
      Record newTab = new Record();
      newTab.dimensionName = responseTa.getRow(i).getDimensions().get(0);
      newTab.metricA = responseTa.getRow(i).getMetrics().get(0);
      records.addRecord(newTab);
      tabsTable.put(newTab.dimensionName, i);
    }
    for (int i = 0; i < responseTb.getNumRows(); ++i) {
      String dimensionName = responseTb.getRow(i).getDimensions().get(0);
      if (tabsTable.containsKey(dimensionName)) {
        int idx = tabsTable.get(dimensionName);
        records.get(idx).metricB = responseTb.getRow(i).getMetrics().get(0);
      } else {
        Record newTab = new Record();
        newTab.dimensionName = responseTb.getRow(i).getDimensions().get(0);
        newTab.metricB = responseTb.getRow(i).getMetrics().get(0);
        records.addRecord(newTab);
      }
    }

    return records;
  }

  public static void main(String[] argc) throws Exception {
    boolean dumpCubeToFile = true;
    String collection = "thirdeyeKbmi";
    String metricName = "desktopPageViews";
    String dimensionName = "continent";
    DateTime baselineStart = new DateTime(2016, 7, 5, 21, 00);
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
    int answerSize = 4;

    // create ThirdEye client
    ThirdEyeConfiguration thirdEyeConfig = new ThirdEyeDashboardConfiguration();
    thirdEyeConfig.setWhitelistCollections(collection);

    PinotThirdEyeClientConfig pinotThirdEyeClientConfig = new PinotThirdEyeClientConfig();
    pinotThirdEyeClientConfig.setControllerHost("lva1-app0086.corp.linkedin.com");
    pinotThirdEyeClientConfig.setControllerPort(11984);
    pinotThirdEyeClientConfig.setZookeeperUrl("zk-lva1-pinot.corp.linkedin.com:12913/pinot-cluster");
    pinotThirdEyeClientConfig.setClusterName("mpSprintDemoCluster");

    ThirdEyeCacheRegistry.initializeWebappCaches(thirdEyeConfig, pinotThirdEyeClientConfig);

    ThirdEyeClient thirdEyeClient = ThirdEyeCacheRegistry.getInstance().getQueryCache().getClient();

    // query for testing
    ThirdEyeRequestBuilder builder = new ThirdEyeRequestBuilder();
    builder.setCollection(collection);
    builder.setMetricFunctions(Lists.newArrayList(new MetricFunction("SUM", metricName)));
    builder.setGroupByTimeGranularity(timeGranularity);

    // Ga
    builder.setStartTimeInclusive(baselineStart);
    builder.setEndTimeExclusive(baselineStart.plusHours(1));
    ThirdEyeResponse responseGa = thirdEyeClient.execute(builder.build("asd"));

    // Gb
    builder.setStartTimeInclusive(baselineStart.plusDays(7));
    builder.setEndTimeExclusive(baselineStart.plusDays(7).plusHours(1));
    ThirdEyeResponse responseGb = thirdEyeClient.execute(builder.build("asd"));

    // retrieve Ta and Tb
    builder.setGroupBy(dimensionName);
    builder.setStartTimeInclusive(baselineStart);
    builder.setEndTimeExclusive(baselineStart.plusHours(1));
    ThirdEyeResponse responseTa = thirdEyeClient.execute(builder.build("asd"));

    builder.setStartTimeInclusive(baselineStart.plusDays(7));
    builder.setEndTimeExclusive(baselineStart.plusDays(7).plusHours(1));
    ThirdEyeResponse responseTb = thirdEyeClient.execute(builder.build("asd"));

    Cube initCube = buildDataCube(responseGa, responseGb, responseTa, responseTb);
    Cube cube;
    if (dumpCubeToFile) {
      try {
        initCube.toJson(oFileName);
        cube = Cube.fromJson(oFileName);
        System.out.println("Restored Cube:");
        System.out.println(cube);
      } catch (IOException e) {
        cube = initCube;
        System.out.println("WARN: Unable to save the cube to the file: " + oFileName);
      }
    } else {
      cube = initCube;
    }

    Summary summary = SummaryCalculator.computeSummary(cube, answerSize);
    System.out.println(summary.toString());

    // closing
    thirdEyeClient.close();
    System.exit(0);
  }
}
