package com.linkedin.thirdeye.client.diffsummary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;
import com.linkedin.thirdeye.dashboard.views.diffsummary.Summary;


/**
 * Provide low-level operation of query to Pinot.
 */
public class PinotThirdEyeSummaryClient implements OLAPDataBaseClient {
  private final static DateTime NULL_DATETIME = new DateTime();

  private QueryCache queryCache;
  private String collection;
  private List<MetricFunction> metricFunctions = new ArrayList<>();
  private DateTime baselineStartInclusive = NULL_DATETIME;
  private DateTime baselineEndExclusive = NULL_DATETIME;
  private DateTime currentStartInclusive = NULL_DATETIME;
  private DateTime currentEndExclusive = NULL_DATETIME;

  public PinotThirdEyeSummaryClient(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  public PinotThirdEyeSummaryClient(ThirdEyeClient thirdEyeClient) {
    this(new QueryCache(thirdEyeClient, Executors.newFixedThreadPool(10)));
  }

  @Override
  public void setCollection(String collection) {
    this.collection = collection;
  }

  @Override
  public void setMetricName(String metricName) {
    metricFunctions.clear();
    metricFunctions.add(new MetricFunction(MetricAggFunction.SUM, metricName));
  }

  @Override
  public void setBaselineStartInclusive(DateTime dateTime) {
    baselineStartInclusive = dateTime;
  }

  @Override
  public void setBaselineEndExclusive(DateTime dateTime) {
    baselineEndExclusive = dateTime;
  }

  @Override
  public void setCurrentStartInclusive(DateTime dateTime) {
    currentStartInclusive = dateTime;
  }

  @Override
  public void setCurrentEndExclusive(DateTime dateTime) {
    currentEndExclusive = dateTime;
  }

  @Override
  public Row getTopAggregatedValues() throws Exception {
    List<ThirdEyeRequest> bulkRequests = new ArrayList<>();
    List<String> groupBy = Collections.emptyList();
    Pair<ThirdEyeRequest, ThirdEyeRequest> timeOnTimeRequests = constructTimeOnTimeRequest(groupBy);
    bulkRequests.add(timeOnTimeRequests.getLeft());
    bulkRequests.add(timeOnTimeRequests.getRight());

    return constructMultiLevelAggregatedValues(null, bulkRequests).get(0).get(0);
  }

  @Override
  public List<List<Row>> getAggregatedValuesOfDimension(Dimensions dimensions) throws Exception {
    List<ThirdEyeRequest> bulkRequests = new ArrayList<>();
    for (int level = 0; level < dimensions.size(); ++level) {
      List<String> groupBy = Lists.newArrayList(dimensions.get(level));
      Pair<ThirdEyeRequest, ThirdEyeRequest> timeOnTimeRequests = constructTimeOnTimeRequest(groupBy);
      bulkRequests.add(timeOnTimeRequests.getLeft());
      bulkRequests.add(timeOnTimeRequests.getRight());
    }

    return constructMultiLevelAggregatedValues(dimensions, bulkRequests);
  }

  @Override
  public List<List<Row>> getAggregatedValuesOfLevels(Dimensions dimensions) throws Exception {
    List<ThirdEyeRequest> bulkRequests = new ArrayList<>();
    for (int level = 0; level < dimensions.size() + 1; ++level) {
      List<String> groupBy = new ArrayList<>(dimensions.groupByStringsAtLevel(level));
      Pair<ThirdEyeRequest, ThirdEyeRequest> timeOnTimeRequests = constructTimeOnTimeRequest(groupBy);
      bulkRequests.add(timeOnTimeRequests.getLeft());
      bulkRequests.add(timeOnTimeRequests.getRight());
    }

    return constructMultiLevelAggregatedValues(dimensions, bulkRequests);
  }

  private List<List<Row>> constructMultiLevelAggregatedValues(Dimensions dimensions, List<ThirdEyeRequest> bulkRequests)
      throws Exception {
    List<List<Row>> res = new ArrayList<>();

    Map<ThirdEyeRequest, Future<ThirdEyeResponse>> queryResponses = queryCache.getQueryResultsAsync(bulkRequests);
    for (int i = 0; i < bulkRequests.size(); i += 2) {
      ThirdEyeResponse responseTa = queryResponses.get(bulkRequests.get(i)).get();
      ThirdEyeResponse responseTb = queryResponses.get(bulkRequests.get(i+1)).get();
      if (responseTa.getNumRows() == 0) {
        throw new Exception("Failed to retrieve results from database with this request: " + bulkRequests.get(i));
      }
      if (responseTb.getNumRows() == 0) {
        throw new Exception("Failed to retrieve results from database with this request: " + bulkRequests.get(i+1));
      }
      List<Row> singleLevelRows = constructSingleLevelAggregatedValues(dimensions, responseTa, responseTb);
      res.add(singleLevelRows);
    }

    return res;
  }

  private List<Row> constructSingleLevelAggregatedValues(Dimensions dimensions, ThirdEyeResponse responseTa, ThirdEyeResponse responseTb) {
    List<Row> rows = new ArrayList<>();
    // TODO: (Performance) Replace the key: List<String>
    Map<List<String>, Integer> rowTable = new HashMap<>();
    {
      for (int j = 0; j < responseTa.getNumRows(); ++j) {
        List<String> dimensionValues = responseTa.getRow(j).getDimensions();
        double value = responseTa.getRow(j).getMetrics().get(0);
        if (Double.compare(.0, value) < 0) {
          Row row = new Row();
          row.dimensions = dimensions;
          row.dimensionValues = new DimensionValues(dimensionValues);
          row.baselineValue = value;
          rowTable.put(dimensionValues, rows.size());
          rows.add(row);
        }
      }
    }

    {
      for (int j = 0; j < responseTb.getNumRows(); ++j) {
        List<String> dimensionValues = responseTb.getRow(j).getDimensions();
        double value = responseTb.getRow(j).getMetrics().get(0);
        if (Double.compare(.0, value) < 0) {
          if (rowTable.containsKey(dimensionValues)) {
            rows.get(rowTable.get(dimensionValues)).currentValue = value;
          } else {
            Row row = new Row();
            row.dimensions = dimensions;
            row.dimensionValues = new DimensionValues(dimensionValues);
            row.currentValue = value;
            rows.add(row);
          }
        }
      }
    }

    return rows;
  }

  private Pair<ThirdEyeRequest, ThirdEyeRequest> constructTimeOnTimeRequest(List<String> groupBy) {
    ThirdEyeRequestBuilder builder = ThirdEyeRequest.newBuilder();
    builder.setCollection(collection);
    builder.setMetricFunctions(metricFunctions);
    builder.setGroupBy(groupBy);

    builder.setStartTimeInclusive(baselineStartInclusive);
    builder.setEndTimeExclusive(baselineEndExclusive);
    ThirdEyeRequest baselineRequest = builder.build("baseline");

    builder.setStartTimeInclusive(currentStartInclusive);
    builder.setEndTimeExclusive(currentEndExclusive);
    ThirdEyeRequest currentRequest = builder.build("current");

    return new ImmutablePair<ThirdEyeRequest, ThirdEyeRequest>(baselineRequest, currentRequest);
  }

  @SuppressWarnings("deprecation")
  public static void main(String[] argc) throws Exception {
    String oFileName = "Cube.json";

    // An interesting data set that difficult to tell because too many dark reds and blues (Granularity: DAYS)
//    String collection = "thirdeyeKbmi";
//    String metricName = "pageViews";
//    DateTime baselineStart = new DateTime(1467788400000L);
//    DateTime baselineEnd =   new DateTime(1469862000000L);
//    DateTime currentStart =  new DateTime(1468393200000L);
//    DateTime currentEnd =    new DateTime(1470466800000L);

    // An interesting data set that difficult to tell because most cells are light red or blue (Granularity: HOURS)
//    String collection = "thirdeyeKbmi";
//    String metricName = "mobilePageViews";
//    DateTime baselineStart = new DateTime(1469628000000L);
//    DateTime baselineEnd =   new DateTime(1469714400000L);
//    DateTime currentStart =  new DateTime(1470232800000L);
//    DateTime currentEnd =    new DateTime(1470319200000L);

    // A migration of Asia connections from other data center to lsg data center (Granularity: DAYS)
    // Most contributors: India and China
//    String collection = "thirdeyeAbook";
//    String metricName = "totalFlows";
//    DateTime baselineStart = new DateTime(2016, 7, 11, 00, 00);
//    DateTime baselineEnd =   new DateTime(2016, 7, 12, 00, 00);
//    DateTime currentStart =  new DateTime(2016, 7, 18, 00, 00);
//    DateTime currentEnd =    new DateTime(2016, 7, 19, 00, 00);

    // National Holidays in India and several countries in Europe and Latin America. (Granularity: DAYS)
    String collection = "thirdeyeKbmi";
    String metricName = "desktopPageViews";
//    DateTime baselineStart = new DateTime(1471244400000L);
//    DateTime baselineEnd =   new DateTime(1471330800000L);
//    DateTime currentStart =  new DateTime(1470639600000L);
//    DateTime currentEnd =    new DateTime(1470726000000L);

    DateTime baselineStart = new DateTime(1470769200000L);
    DateTime baselineEnd =   new DateTime(1470855600000L);
    DateTime currentStart =  new DateTime(1471374000000L);
    DateTime currentEnd =    new DateTime(1471460400000L);

    String[] dimensionNames = { "browserName", "continent", "countryCode",
                                "deviceName", "environment", "locale", "osName",
                                "pageKey", "service", "sourceApp" };
    List<List<String>> hierarchies = new ArrayList<>();
    hierarchies.add(Lists.newArrayList("continent", "countryCode"));

    // Create ThirdEye client
    ThirdEyeConfiguration thirdEyeConfig = new ThirdEyeDashboardConfiguration();
    thirdEyeConfig.setWhitelistCollections(collection);

    PinotThirdEyeClientConfig pinotThirdEyeClientConfig = new PinotThirdEyeClientConfig();
    pinotThirdEyeClientConfig.setControllerHost("lva1-app0086.corp.linkedin.com");
    pinotThirdEyeClientConfig.setControllerPort(11984);
    pinotThirdEyeClientConfig.setZookeeperUrl("zk-lva1-pinot.corp.linkedin.com:12913/pinot-cluster");
    pinotThirdEyeClientConfig.setClusterName("mpSprintDemoCluster");

    ThirdEyeCacheRegistry.initializeWebappCaches(thirdEyeConfig, pinotThirdEyeClientConfig);

    OLAPDataBaseClient pinotClient = new PinotThirdEyeSummaryClient(ThirdEyeCacheRegistry.getInstance().getQueryCache());
    pinotClient.setCollection(collection);
    pinotClient.setMetricName(metricName);
    pinotClient.setCurrentStartInclusive(currentStart);
    pinotClient.setCurrentEndExclusive(currentEnd);
    pinotClient.setBaselineStartInclusive(baselineStart);
    pinotClient.setBaselineEndExclusive(baselineEnd);


    int maxDimensionSize = 3;

    // Build the cube for computing the summary
    Cube initCube = new Cube();
    initCube.buildWithAutoDimensionOrder(pinotClient, new Dimensions(Lists.newArrayList(dimensionNames)),
        maxDimensionSize, hierarchies);
//    initCube.buildWithManualDimensionOrder(pinotClient, new Dimensions(Lists.newArrayList(dimensionNames)));


    int answerSize = 10;
    boolean oneSideErrors = false;
    Summary summary = new Summary(initCube);
    System.out.println(summary.computeSummary(answerSize, oneSideErrors, maxDimensionSize));

    try {
      initCube.toJson(oFileName);
      Cube cube = Cube.fromJson(oFileName);
      System.out.println("Restored Cube:");
      System.out.println(cube);
      summary = new Summary(cube);
      System.out.println(summary.computeSummary(answerSize, oneSideErrors, maxDimensionSize));
    } catch (IOException e) {
      System.err.println("WARN: Unable to save the cube to the file: " + oFileName);
      e.printStackTrace();
    }

    // closing
    System.exit(0);
  }
}
