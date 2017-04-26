package com.linkedin.thirdeye.client.diffsummary;

import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.File;
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

import org.jfree.util.Log;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.diffsummary.Summary;

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
    builder.setMetricFunctions(metricFunctions);
    builder.setGroupBy(groupBy);
    builder.setStartTimeInclusive(baselineStartInclusive);
    builder.setEndTimeExclusive(baselineEndExclusive);
    ThirdEyeRequest baselineRequest = builder.build("baseline");
    requests.add(baselineRequest);

    // current requests
    builder = ThirdEyeRequest.newBuilder();
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
      if (Double.compare(0d, value) < 0 && !Double.isInfinite(value)) {
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
}
