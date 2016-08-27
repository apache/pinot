package com.linkedin.thirdeye.client.diffsummary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jfree.util.Log;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.views.diffsummary.Summary;
import com.linkedin.thirdeye.util.ThirdEyeUtils;


/**
 * This class generates query requests to the backend database and retrieve the data for summary algorithm.
 *
 * The generated requests are organized the following tree structure:
 *   Root level by GroupBy dimensions.
 *   Mid  level by "baseline" or "current"; The "baseline" request is ordered before the "current" request.
 *   Leaf level by metric functions; This level is handled by the request itself, i.e., a request can gather multiple
 *   metric functions at the same time.
 * The generated requests are store in a List. Because of the tree structure, the requests belong to the same
 * timeline (baseline or current) are located together. Then, the requests belong to the same GroupBy dimension are
 * located together.
 */
public class PinotThirdEyeSummaryClient implements OLAPDataBaseClient {
  private final static DateTime NULL_DATETIME = new DateTime();
  private final static int TIME_OUT_VALUE = 120;
  private final static TimeUnit TIME_OUT_UNIT = TimeUnit.SECONDS;

  private QueryCache queryCache;
  private String collection;
  private DateTime baselineStartInclusive = NULL_DATETIME;
  private DateTime baselineEndExclusive = NULL_DATETIME;
  private DateTime currentStartInclusive = NULL_DATETIME;
  private DateTime currentEndExclusive = NULL_DATETIME;

  private MetricExpression metricExpression;
  private List<MetricFunction> metricFunctions;
  private MetricExpressionsContext context;

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
  public void setMetricExpression(MetricExpression metricExpression) {
    this.metricExpression = metricExpression;
    metricFunctions = metricExpression.computeMetricFunctions();
    if (metricFunctions.size() > 1) {
      context = new MetricExpressionsContext();
    } else {
      context = null;
    }
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
    List<String> groupBy = Collections.emptyList();
      List<ThirdEyeRequest> timeOnTimeBulkRequests = constructTimeOnTimeBulkRequests(groupBy);
      Row row = constructAggregatedValues(null, timeOnTimeBulkRequests).get(0).get(0);
      return row;
  }

  @Override
  public List<List<Row>> getAggregatedValuesOfDimension(Dimensions dimensions) throws Exception {
      List<ThirdEyeRequest> timeOnTimeBulkRequests = new ArrayList<>();
      for (int level = 0; level < dimensions.size(); ++level) {
        List<String> groupBy = Lists.newArrayList(dimensions.get(level));
        timeOnTimeBulkRequests.addAll(constructTimeOnTimeBulkRequests(groupBy));
      }
      List<List<Row>> rows = constructAggregatedValues(dimensions, timeOnTimeBulkRequests);
      return rows;
  }

  @Override
  public List<List<Row>> getAggregatedValuesOfLevels(Dimensions dimensions) throws Exception {
      List<ThirdEyeRequest> timeOnTimeBulkRequests = new ArrayList<>();
      for (int level = 0; level < dimensions.size() + 1; ++level) {
        List<String> groupBy = Lists.newArrayList(dimensions.groupByStringsAtLevel(level));
        timeOnTimeBulkRequests.addAll(constructTimeOnTimeBulkRequests(groupBy));
      }
      List<List<Row>> rows = constructAggregatedValues(dimensions, timeOnTimeBulkRequests);
      return rows;
  }

  /**
   * Returns the baseline and current requests for the given GroupBy dimensions.
   *
   * @param groupBy the dimensions to do GroupBy queries
   * @return Baseline and Current requests.
   */
  private List<ThirdEyeRequest> constructTimeOnTimeBulkRequests(List<String> groupBy) {
    List<ThirdEyeRequest> requests = new ArrayList<>();;

    // baseline requests
    ThirdEyeRequestBuilder builder = ThirdEyeRequest.newBuilder();
    builder.setCollection(collection);
    builder.setMetricFunctions(metricFunctions);
    builder.setGroupBy(groupBy);
    builder.setStartTimeInclusive(baselineStartInclusive);
    builder.setEndTimeExclusive(baselineEndExclusive);
    ThirdEyeRequest baselineRequest = builder.build("baseline");
    requests.add(baselineRequest);

    // current requests
    builder = ThirdEyeRequest.newBuilder();
    builder.setCollection(collection);
    builder.setMetricFunctions(metricFunctions);
    builder.setGroupBy(groupBy);
    builder.setStartTimeInclusive(currentStartInclusive);
    builder.setEndTimeExclusive(currentEndExclusive);
    ThirdEyeRequest currentRequest = builder.build("current");
    requests.add(currentRequest);

    return requests;
  }

  /**
   * @throws Exception Throws exceptions when no useful data is retrieved, i.e., time out, failed to connect
   * to the backend database, no non-zero data returned from the database, etc.
   */
  private List<List<Row>> constructAggregatedValues(Dimensions dimensions, List<ThirdEyeRequest> bulkRequests)
      throws Exception {
    Map<ThirdEyeRequest, Future<ThirdEyeResponse>> queryResponses = queryCache.getQueryResultsAsync(bulkRequests);

    List<List<Row>> res = new ArrayList<>();
    for (int i = 0; i < bulkRequests.size(); ) {
      ThirdEyeRequest baselineRequest = bulkRequests.get(i++);
      ThirdEyeRequest currentRequest = bulkRequests.get(i++);
      ThirdEyeResponse baselineResponses = queryResponses.get(baselineRequest).get(TIME_OUT_VALUE, TIME_OUT_UNIT);
      ThirdEyeResponse currentResponses = queryResponses.get(currentRequest).get(TIME_OUT_VALUE, TIME_OUT_UNIT);
      if (baselineResponses.getNumRows() == 0 || currentResponses.getNumRows() == 0) {
        throw new Exception("Failed to retrieve results with this request: "
            + (baselineResponses.getNumRows() == 0 ? baselineRequest : currentRequest));
      }

      Map<List<String>, Row> rowTable = new HashMap<>();
      buildMetricFunctionOrExpressionsRows(dimensions, baselineResponses, rowTable, true);
      buildMetricFunctionOrExpressionsRows(dimensions, currentResponses, rowTable, false);
      if (rowTable.size() == 0) {
        throw new Exception("Failed to retrieve non-zero results with these requests: "
            + baselineRequest + ", " + currentRequest);
      }
      List<Row> rows = new ArrayList<>(rowTable.values());
      res.add(rows);
    }

    return res;
  }

  /**
   * Returns a list of rows. The value of each row is evaluated and no further processing is needed.
   * @param dimensions dimensions of the response
   * @param response the response from backend database
   * @param rowTable the storage for rows
   * @param isBaseline true if the response is for baseline values
   */
  private void buildMetricFunctionOrExpressionsRows(Dimensions dimensions, ThirdEyeResponse response,
      Map<List<String>, Row> rowTable, boolean isBaseline) {
    for (int rowIdx = 0; rowIdx < response.getNumRows(); ++rowIdx) {
      double value = 0d;
      // If the metric expression is a single metric function, then we get the value immediately
      if (metricFunctions.size() <= 1) {
        value = response.getRow(rowIdx).getMetrics().get(0);
      } else { // Otherwise, we need to evaluate the expression
        context.reset();
        for (int metricFuncIdx = 0; metricFuncIdx < metricFunctions.size(); ++metricFuncIdx) {
          double contextValue = response.getRow(rowIdx).getMetrics().get(metricFuncIdx);
          context.set(metricFunctions.get(metricFuncIdx).getMetricName(), contextValue);
        }
        try {
          value = MetricExpression.evaluateExpression(metricExpression, context.getContext());
        } catch (Exception e) {
          Log.warn(e);
        }
      }
      if (Double.compare(0d, value) < 0 && Double.isFinite(value)) {
        List<String> dimensionValues = response.getRow(rowIdx).getDimensions();
        Row row = rowTable.get(dimensionValues);
        if (row == null) {
          row = new Row();
          row.setDimensions(dimensions);
          row.setDimensionValues(new DimensionValues(dimensionValues));
          rowTable.put(dimensionValues, row);
        }
        if (isBaseline) {
          row.baselineValue = value;
        } else {
          row.currentValue = value;
        }
      }
    }
  }

  private class MetricExpressionsContext {
    private Map<String, Double> context;
    public MetricExpressionsContext () {
      context = new HashMap<>();
      for (MetricFunction metricFunction : metricFunctions) {
        context.put(metricFunction.getMetricName(), 0d);
      }
    }
    public void set(String metricName, double value) {
      context.put(metricName, value);
    }
    public Map<String, Double> getContext() {
      return context;
    }
    public void reset() {
      for (Map.Entry<String, Double> entry : context.entrySet()) {
        entry.setValue(0d);
      }
    }
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
    DateTime baselineStart = new DateTime(1470589200000L);
    DateTime baselineEnd =   new DateTime(1470675600000L);
    DateTime currentStart =  new DateTime(1471194000000L);
    DateTime currentEnd =    new DateTime(1471280400000L);

//    String collection = "ptrans_additive";
//    String metricName = "txProcessTime/__COUNT";
//    DateTime baselineStart = new DateTime(1470938400000L);
//    DateTime baselineEnd =   new DateTime(1471024800000L);
//    DateTime currentStart =  new DateTime(1471543200000L);
//    DateTime currentEnd =    new DateTime(1471629600000L);

    // Create ThirdEye client
    ThirdEyeConfiguration thirdEyeConfig = new ThirdEyeDashboardConfiguration();
    thirdEyeConfig.setWhitelistCollections(collection);

    PinotThirdEyeClientConfig pinotThirdEyeClientConfig = new PinotThirdEyeClientConfig();
    pinotThirdEyeClientConfig.setControllerHost("lva1-app0086.corp.linkedin.com");
    pinotThirdEyeClientConfig.setControllerPort(11984);
    pinotThirdEyeClientConfig.setZookeeperUrl("zk-lva1-pinot.corp.linkedin.com:12913/pinot-cluster");
    pinotThirdEyeClientConfig.setClusterName("mpSprintDemoCluster");

    ThirdEyeCacheRegistry.initializeWebappCaches(thirdEyeConfig, pinotThirdEyeClientConfig);
    ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
    collection = ThirdEyeUtils.getCollectionFromAlias(collection);
    CollectionConfig collectionConfig = CACHE_REGISTRY_INSTANCE.getCollectionConfigCache().getIfPresent(collection);
    if (collectionConfig != null && collectionConfig.getDerivedMetrics() != null
        && collectionConfig.getDerivedMetrics().containsKey(metricName)) {
      metricName = collectionConfig.getDerivedMetrics().get(metricName);
    }
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metricName, collection);
    System.out.println(metricExpressions);

    OLAPDataBaseClient pinotClient = new PinotThirdEyeSummaryClient(CACHE_REGISTRY_INSTANCE.getQueryCache());
    pinotClient.setCollection(collection);
    pinotClient.setMetricExpression(metricExpressions.get(0));
    pinotClient.setCurrentStartInclusive(currentStart);
    pinotClient.setCurrentEndExclusive(currentEnd);
    pinotClient.setBaselineStartInclusive(baselineStart);
    pinotClient.setBaselineEndExclusive(baselineEnd);

//    String[] dimensionNames =
//        { "browserName", "continent", "countryCode", "deviceName", "environment", "locale", "osName", "pageKey", "service", "sourceApp" };
    List<List<String>> hierarchies = new ArrayList<>();
    hierarchies.add(Lists.newArrayList("continent", "countryCode"));
    hierarchies.add(Lists.newArrayList("browser_name", "browser_version"));

//    Dimensions dimensions = new Dimensions(Lists.newArrayList(dimensionNames));
    Dimensions dimensions = new Dimensions(Utils.getDimensions(CACHE_REGISTRY_INSTANCE.getQueryCache(), collection));

    int maxDimensionSize = 3;

    // Build the cube for computing the summary
    Cube initCube = new Cube();
    initCube.buildWithAutoDimensionOrder(pinotClient, dimensions, maxDimensionSize, hierarchies);
//    initCube.buildWithManualDimensionOrder(pinotClient, dimensions);


    int answerSize = 10;
    boolean oneSideErrors = true;
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
