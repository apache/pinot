package com.linkedin.thirdeye.client.diffsummary;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.jfree.util.Log;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeSummaryClient.class);

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
  public Row getTopAggregatedValues(Multimap<String, String> filterSets) throws Exception {
    List<String> groupBy = Collections.emptyList();
      List<ThirdEyeRequest> timeOnTimeBulkRequests = constructTimeOnTimeBulkRequests(groupBy, filterSets);
      Row row = constructAggregatedValues(new Dimensions(), timeOnTimeBulkRequests).get(0).get(0);
      return row;
  }

  @Override
  public List<List<Row>> getAggregatedValuesOfDimension(Dimensions dimensions, Multimap<String, String> filterSets)
      throws Exception {
      List<ThirdEyeRequest> timeOnTimeBulkRequests = new ArrayList<>();
      for (int level = 0; level < dimensions.size(); ++level) {
        List<String> groupBy = Lists.newArrayList(dimensions.get(level));
        timeOnTimeBulkRequests.addAll(constructTimeOnTimeBulkRequests(groupBy, filterSets));
      }
      List<List<Row>> rows = constructAggregatedValues(dimensions, timeOnTimeBulkRequests);
      return rows;
  }

  @Override
  public List<List<Row>> getAggregatedValuesOfLevels(Dimensions dimensions, Multimap<String, String> filterSets)
      throws Exception {
      List<ThirdEyeRequest> timeOnTimeBulkRequests = new ArrayList<>();
      for (int level = 0; level < dimensions.size() + 1; ++level) {
        List<String> groupBy = Lists.newArrayList(dimensions.namesToDepth(level));
        timeOnTimeBulkRequests.addAll(constructTimeOnTimeBulkRequests(groupBy, filterSets));
      }
      List<List<Row>> rows = constructAggregatedValues(dimensions, timeOnTimeBulkRequests);
      return rows;
  }

  /**
   * Returns the baseline and current requests for the given GroupBy dimensions.
   *
   * @param groupBy the dimensions to do GroupBy queries
   * @param filterSets the filter to apply on the DB queries (e.g., country=US, etc.)
   * @return Baseline and Current requests.
   */
  private List<ThirdEyeRequest> constructTimeOnTimeBulkRequests(List<String> groupBy,
      Multimap<String, String> filterSets) {
    List<ThirdEyeRequest> requests = new ArrayList<>();;

    // baseline requests
    ThirdEyeRequestBuilder builder = ThirdEyeRequest.newBuilder();
    builder.setMetricFunctions(metricFunctions);
    builder.setGroupBy(groupBy);
    builder.setStartTimeInclusive(baselineStartInclusive);
    builder.setEndTimeExclusive(baselineEndExclusive);
    builder.setDataSource(ThirdEyeUtils.getDataSourceFromMetricFunctions(metricFunctions));
    builder.setFilterSet(filterSets);
    ThirdEyeRequest baselineRequest = builder.build("baseline");
    requests.add(baselineRequest);

    // current requests
    builder = ThirdEyeRequest.newBuilder();
    builder.setMetricFunctions(metricFunctions);
    builder.setGroupBy(groupBy);
    builder.setStartTimeInclusive(currentStartInclusive);
    builder.setEndTimeExclusive(currentEndExclusive);
    builder.setDataSource(ThirdEyeUtils.getDataSourceFromMetricFunctions(metricFunctions));
    builder.setFilterSet(filterSets);
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
      if (baselineResponses.getNumRows() == 0) {
        LOG.warn("Get 0 rows from the request(s): {}", baselineRequest);
      }
      if (currentResponses.getNumRows() == 0) {
        LOG.warn("Get 0 rows from the request(s): {}", currentRequest);
      }

      Map<List<String>, Row> rowTable = new HashMap<>();
      buildMetricFunctionOrExpressionsRows(dimensions, baselineResponses, rowTable, true);
      buildMetricFunctionOrExpressionsRows(dimensions, currentResponses, rowTable, false);
      if (rowTable.size() == 0) {
        LOG.warn("Failed to retrieve non-zero results with these requests: " + baselineRequest + ", " + currentRequest);
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
          row = new Row(dimensions, new DimensionValues(dimensionValues));
          rowTable.put(dimensionValues, row);
        }
        if (isBaseline) {
          row.setBaselineValue(value);
        } else {
          row.setCurrentValue(value);
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
