package com.linkedin.thirdeye.client.pinot.summary;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;


/**
 * Provide low-level operation of query to Pinot.
 */
public class PinotThirdEyeSummaryClient implements OLAPDataBaseClient {
  private final static DateTime NULL_DATETIME = new DateTime();
  private final static TimeGranularity NULL_TIME_GRANULARITY = new TimeGranularity(0, TimeUnit.HOURS);

  QueryCache queryCache;
  ThirdEyeRequest.ThirdEyeRequestBuilder builder = new ThirdEyeRequest.ThirdEyeRequestBuilder();
  TimeGranularity timeGranularity = NULL_TIME_GRANULARITY;
  DateTime baselineStartInclusive = NULL_DATETIME;
  DateTime baselineEndExclusive = NULL_DATETIME;
  DateTime currentStartInclusive = NULL_DATETIME;
  DateTime currentEndExclusive = NULL_DATETIME;

  public PinotThirdEyeSummaryClient(ThirdEyeClient thirdEyeClient) {
    queryCache = new QueryCache(thirdEyeClient, Executors.newFixedThreadPool(10));
    builder.setGroupByTimeGranularity(this.timeGranularity);
  }

  @Override
  public void setCollection(String collection) {
    builder.setCollection(collection);
  }

  @Override
  public void setGroupByTimeGranularity(TimeGranularity timeGranularity) {
    if (!this.timeGranularity.equals(timeGranularity)) {
      this.timeGranularity = timeGranularity;
      builder.setGroupByTimeGranularity(timeGranularity);
      updateBaselineEndExclusive();
      updateCurrentEndExclusive();
    }
  }

  @Override
  public void setBaselineStartInclusive(DateTime dateTime) {
    if (!this.baselineStartInclusive.equals(dateTime)) {
      baselineStartInclusive = dateTime;
      updateBaselineEndExclusive();
    }
  }

  @Override
  public void setCurrentStartInclusive(DateTime dateTime) {
    if (!this.currentStartInclusive.equals(dateTime)) {
      currentStartInclusive = dateTime;
      updateCurrentEndExclusive();
    }
  }

  private void updateBaselineEndExclusive() {
    baselineEndExclusive = baselineStartInclusive.plus(timeGranularity.toMillis());
  }

  private void updateCurrentEndExclusive() {
    currentEndExclusive = currentStartInclusive.plus(timeGranularity.toMillis());
  }

  @Override
  public void setMetricName(String metricName) {
    builder.setMetricFunctions(Lists.newArrayList(new MetricFunction(MetricAggFunction.SUM, metricName)));
  }

  @Override
  public Pair<Double, Double> getTopAggregatedValues() {
    Pair<ThirdEyeResponse, ThirdEyeResponse> responses;
    try {
      responses = getTimeOnTimeResponse(Collections.<String>emptyList());
    } catch (Exception e) {
      e.printStackTrace();
      return Pair.of(.0, .0);
    }

    return new ImmutablePair<Double, Double>(responses.getLeft().getRow(0).getMetrics().get(0), responses.getRight()
        .getRow(0).getMetrics().get(0));
  }

  @Override
  public List<Pair<Double, Double>> getAggregatedValuesInOneDimension(String dimensionName) {
    Pair<ThirdEyeResponse, ThirdEyeResponse> responses;
    try {
      responses = getTimeOnTimeResponse(Arrays.asList(dimensionName));
    } catch (Exception e) {
      e.printStackTrace();
      return Collections.<Pair<Double, Double>>emptyList();
    }

    List<Pair<Double, Double>> tab = new ArrayList<>();
    Map<String, Integer> tabTable = new HashMap<>();
    {
      ThirdEyeResponse responseTa = responses.getLeft();

      for (int j = 0; j < responseTa.getNumRows(); ++j) {
        String dimensionValue = responseTa.getRow(j).getDimensions().get(0);
        double value = responseTa.getRow(j).getMetrics().get(0);
        if (Double.compare(.0, value) < 0) {
          tabTable.put(dimensionValue, tab.size());
          tab.add(new MutablePair<Double, Double>(value, .0));
        }
      }
    }

    {
      ThirdEyeResponse responseTb = responses.getRight();

      for (int j = 0; j < responseTb.getNumRows(); ++j) {
        String dimensionValue = responseTb.getRow(j).getDimensions().get(0);
        double value = responseTb.getRow(j).getMetrics().get(0);
        if (Double.compare(.0, value) < 0) {
          if (tabTable.containsKey(dimensionValue)) {
            ((MutablePair<Double, Double>) tab.get(tabTable.get(dimensionValue))).setRight(value);
          } else {
            tab.add(new MutablePair<Double, Double>(.0, value));
          }
        }
      }
    }

    return tab;
  }

  // TODO: (Performance) Multi-threaded this method
  // TODO: (Design) Merge with getAggregatedValuesInOneDimension() and move to a dedicated parser class
  public List<Row> getAggregatedValuesAtLevel(Dimensions dimensions, int level) {
    Pair<ThirdEyeResponse, ThirdEyeResponse> responses;
    try {
      List<String> groupBy = dimensions.getGroupByStringsAtLevel(level);
      responses = getTimeOnTimeResponse(groupBy);
    } catch (Exception e) {
      e.printStackTrace();
      return Collections.<Row>emptyList();
    }

    List<Row> rows = new ArrayList<>();
    // TODO: (Performance) Replace the key: List<String>
    Map<List<String>, Integer> rowTable = new HashMap<>();
    {
      ThirdEyeResponse responseTa = responses.getLeft();

      for (int j = 0; j < responseTa.getNumRows(); ++j) {
        List<String> dimensionValues = responseTa.getRow(j).getDimensions();
        double value = responseTa.getRow(j).getMetrics().get(0);
        if (Double.compare(.0, value) < 0) {
          Row row = new Row();
          row.dimensions = dimensions;
          row.dimensionValues = new DimensionValues(responseTa.getRow(j).getDimensions());
          row.level = level;
          row.baselineValue = value;
          rowTable.put(dimensionValues, rows.size());
          rows.add(row);
        }
      }
    }

    {
      ThirdEyeResponse responseTb = responses.getRight();

      for (int j = 0; j < responseTb.getNumRows(); ++j) {
        List<String> dimensionValues = responseTb.getRow(j).getDimensions();
        double value = responseTb.getRow(j).getMetrics().get(0);
        if (Double.compare(.0, value) < 0) {
          if (rowTable.containsKey(dimensionValues)) {
            rows.get(rowTable.get(dimensionValues)).currentValue = value;
          } else {
            Row row = new Row();
            row.dimensions = dimensions;
            row.dimensionValues = new DimensionValues(responseTb.getRow(j).getDimensions());
            row.level = level;
            row.currentValue = value;
            rows.add(row);
          }
        }
      }
    }

    return rows;
  }

  // TODO: (Performance) (Safety) Make this function thread-safe
  private Pair<ThirdEyeResponse, ThirdEyeResponse> getTimeOnTimeResponse(List<String> groupBy) throws Exception {
    builder.setGroupBy(groupBy);

    builder.setStartTimeInclusive(baselineStartInclusive);
    builder.setEndTimeExclusive(baselineEndExclusive);
    ThirdEyeRequest baselineRequest = builder.build("baseline");

    builder.setStartTimeInclusive(currentStartInclusive);
    builder.setEndTimeExclusive(currentEndExclusive);
    ThirdEyeRequest currentRequest = builder.build("current");

    Map<ThirdEyeRequest, Future<ThirdEyeResponse>> queryResponses =
        queryCache.getQueryResultsAsync(Lists.newArrayList(baselineRequest, currentRequest));

    return new ImmutablePair<ThirdEyeResponse, ThirdEyeResponse>(queryResponses.get(baselineRequest).get(),
        queryResponses.get(currentRequest).get());
  }

  @SuppressWarnings("deprecation")
  public static void main(String[] argc) throws Exception {
    String oFileName = "MLCube.json";
    boolean dumpCubeToFile = true;
    int answerSize = 10;
    String collection = "thirdeyeKbmi";
    String metricName = "desktopPageViews";
//    String[] dimensionNames = { "continent", "environment", "osName", "countryCode" };
    String[] dimensionNames = { "continent", "environment", "osName" };
    DateTime baselineStart = new DateTime(2016, 7, 5, 21, 00);
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);

    // Create ThirdEye client
    ThirdEyeConfiguration thirdEyeConfig = new ThirdEyeDashboardConfiguration();
    thirdEyeConfig.setWhitelistCollections(collection);

    PinotThirdEyeClientConfig pinotThirdEyeClientConfig = new PinotThirdEyeClientConfig();
    pinotThirdEyeClientConfig.setControllerHost("lva1-app0086.corp.linkedin.com");
    pinotThirdEyeClientConfig.setControllerPort(11984);
    pinotThirdEyeClientConfig.setZookeeperUrl("zk-lva1-pinot.corp.linkedin.com:12913/pinot-cluster");
    pinotThirdEyeClientConfig.setClusterName("mpSprintDemoCluster");

    ThirdEyeCacheRegistry.initializeWebappCaches(thirdEyeConfig, pinotThirdEyeClientConfig);

    ThirdEyeClient thirdEyeClient = ThirdEyeCacheRegistry.getInstance().getQueryCache().getClient();

    OLAPDataBaseClient pinotClient = new PinotThirdEyeSummaryClient(thirdEyeClient);
    pinotClient.setCollection(collection);
    pinotClient.setMetricName(metricName);
    pinotClient.setGroupByTimeGranularity(timeGranularity);
    pinotClient.setBaselineStartInclusive(baselineStart);
    pinotClient.setCurrentStartInclusive(baselineStart.plusDays(7));

    // Build the cube for computing the summary
    Cube initCube = new Cube();
    initCube.buildFromOALPDataBase(pinotClient, new Dimensions(Lists.newArrayList(dimensionNames)));

    Cube cube;
    if (dumpCubeToFile) {
      try {
        initCube.toJson(oFileName);
        cube = Cube.fromJson(oFileName);
        System.out.println("Restored Cube:");
        System.out.println(cube);
      } catch (IOException e) {
        System.err.println("WARN: Unable to save the cube to the file: " + oFileName);
        e.printStackTrace();
        cube = initCube;
      }
    } else {
      cube = initCube;
    }

    cube.removeEmptyRecords();
    Summary summary = SummaryCalculator.computeSummary(cube, answerSize);
    System.out.println(summary.toString());

    // closing
    thirdEyeClient.close();
    System.exit(0);
  }
}
