package com.linkedin.thirdeye.client.diffsummary.teradata;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.diffsummary.DimensionValues;
import com.linkedin.thirdeye.client.diffsummary.Dimensions;
import com.linkedin.thirdeye.client.diffsummary.OLAPDataBaseClient;
import com.linkedin.thirdeye.client.diffsummary.Row;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TeradataThirdEyeSummaryClient implements OLAPDataBaseClient {
  protected final Logger LOG = LoggerFactory.getLogger(this.getClass());
  private final static DateTime NULL_DATETIME = new DateTime();
  private final static int TIME_OUT_VALUE = 120;
  private final static TimeUnit TIME_OUT_UNIT = TimeUnit.SECONDS;

  private String _tableName;
  private DateTime baselineStartInclusive = NULL_DATETIME;
  private DateTime baselineEndExclusive = NULL_DATETIME;
  private DateTime currentStartInclusive = NULL_DATETIME;
  private DateTime currentEndExclusive = NULL_DATETIME;
  private ExecutorService _executorService;

  private QueryTera _queryTera;

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public TeradataThirdEyeSummaryClient(QueryTera queryTera, ExecutorService executorService) {
    _queryTera = queryTera;
    _executorService = executorService;
  }

  @Override
  public void setCollection(String collection) {
    _tableName = collection;
  }

  @Override
  public void setMetricExpression(MetricExpression metricExpressions) {
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
    Map<List<String>, GroupByCallable> requestsBaseline = new HashMap<>();
    Map<List<String>, GroupByCallable> requestsCurrent = new HashMap<>();
    requestsBaseline.put(groupBy, constructCallable(groupBy, true));
    requestsCurrent.put(groupBy, constructCallable(groupBy, false));
    return constructAggregatedValues(null, requestsBaseline, requestsCurrent, false).get(0).get(0);
  }

  @Override
  public List<List<Row>> getAggregatedValuesOfDimension(Dimensions dimensions)
      throws Exception {
    Map<List<String>, GroupByCallable> requestsBaseline = new HashMap<>();
    Map<List<String>, GroupByCallable> requestsCurrent = new HashMap<>();
    for (int level = 0; level < dimensions.size(); ++level) {
      List<String> groupBy = Lists.newArrayList(dimensions.get(level));
      requestsBaseline.put(groupBy, constructCallable(groupBy, true));
      requestsCurrent.put(groupBy, constructCallable(groupBy, false));
    }
    return constructAggregatedValues(dimensions, requestsBaseline, requestsCurrent, true);
  }

  @Override
  public List<List<Row>> getAggregatedValuesOfLevels(Dimensions dimensions)
      throws Exception {
    Map<List<String>, GroupByCallable> requestsBaseline = new HashMap<>();
    Map<List<String>, GroupByCallable> requestsCurrent = new HashMap<>();
    for (int level = 0; level < dimensions.size()+1; ++level) {
      List<String> groupBy = Lists.newArrayList(dimensions.groupByStringsAtLevel(level));
      requestsBaseline.put(groupBy, constructCallable(groupBy, true));
      requestsCurrent.put(groupBy, constructCallable(groupBy, false));
    }
    return constructAggregatedValues(dimensions, requestsBaseline, requestsCurrent, false);
  }

  private GroupByCallable constructCallable(List<String> groupByString, Boolean isBase) {
    if (isBase) {
          return new GroupByCallable(_queryTera, groupByString, _tableName, baselineStartInclusive, baselineEndExclusive);
    } else {
          return new GroupByCallable(_queryTera, groupByString, _tableName, currentStartInclusive, currentEndExclusive);
    }
  }

  private List<List<Row>> constructAggregatedValues(Dimensions dimensions,
      Map<List<String>, GroupByCallable> requestsBaseline,
      Map<List<String>, GroupByCallable> requestsCurrent,
      Boolean isOfDimension) throws Exception {
    Map<List<String>, Future<Map<List<String>, Double>>> futureMapBaseline = new HashMap<>();
    Map<List<String>, Future<Map<List<String>, Double>>> futureMapCurrent = new HashMap<>();
    for (Map.Entry<List<String>, GroupByCallable> entry : requestsBaseline.entrySet()) {
      futureMapBaseline.put(entry.getKey(), _executorService.submit(entry.getValue()));
    }

    for (Map.Entry<List<String>, GroupByCallable> entry : requestsCurrent.entrySet()) {
      futureMapCurrent.put(entry.getKey(), _executorService.submit(entry.getValue()));
    }

    List<List<Row>> rowTable = new ArrayList<>();
    for (int i = 0; i < futureMapBaseline.size(); i++) {
      rowTable.add(new ArrayList<>());
    }
    int k = 0;
    for (List<String> groupBy : futureMapCurrent.keySet()) {
      int ind = isOfDimension ? (k++) : groupBy.size();
      List<Row> rows = rowTable.get(ind);
      if (!futureMapCurrent.containsKey(groupBy)) {
        LOG.error("Mismatch of dimension between baseline and current value");
      }
      Map<List<String>, Double> baseline = futureMapBaseline.get(groupBy).get(TIME_OUT_VALUE, TIME_OUT_UNIT);
      Map<List<String>, Double> current = futureMapCurrent.get(groupBy).get(TIME_OUT_VALUE, TIME_OUT_UNIT);

      Set<List<String>> allDimValues = new HashSet<>();
      allDimValues.addAll(baseline.keySet());
      allDimValues.addAll(current.keySet());

      for (List<String> values: allDimValues) {
        DimensionValues dimensionValues = new DimensionValues(values);
        Row oneRow = new Row();
        oneRow.setDimensions(dimensions);
        oneRow.setDimensionValues(dimensionValues);
        if (current.containsKey(values)) {
          oneRow.setCurrentValue(current.get(values));
        } else {
          oneRow.setCurrentValue(0.);
        }
        if (baseline.containsKey(values)) {
          oneRow.setBaselineValue(baseline.get(values));
        } else {
          oneRow.setBaselineValue(0.);
        }
        rows.add(oneRow);
      }
    }

    return rowTable;
  }

  public static void main(String[] args) {

    System.setProperty("dw.rootDir", "/Users/jswang/tmp/thirdeye-configs/prod-configs");
    Injector injector = Guice.createInjector(new TeradataSourceModel());
    QueryTera queryTera = injector.getInstance(QueryTera.class);

    ExecutorService executorService = Executors.newFixedThreadPool(4);

    TeradataThirdEyeSummaryClient ttsc = new TeradataThirdEyeSummaryClient(queryTera, executorService);

    String tableName = "dm_biz.yoy_bookings";
    DateTime b_start = new DateTime("2015-10-01T21:39:45.618-08:00");
    DateTime b_end = new DateTime("2015-10-28T21:39:45.618-08:00");
    DateTime c_start = new DateTime("2016-10-18T21:39:45.618-08:00");
    DateTime c_end = new DateTime("2016-10-25T21:39:45.618-08:00");
    ttsc.setBaselineStartInclusive(b_start);
    ttsc.setBaselineEndExclusive(b_end);
    ttsc.setCurrentStartInclusive(c_start);
    ttsc.setCurrentEndExclusive(c_end);
    ttsc.setTableName(tableName);

    List<String> d1 = new ArrayList<>();
    d1.add("poster_type");
    d1.add("job_type");
    d1.add("job_tier");
    d1.add("member_country_grp4");
    Dimensions dim = new Dimensions(d1);
    List<List<Row>> rowTable = null;
    try {
      rowTable = ttsc.getAggregatedValuesOfDimension(dim);
      rowTable.stream().forEach(e -> {
        e.stream().forEach(row -> System.out.println(row.toString()));
      });
//      Row topRow = ttsc.getTopAggregatedValues();
//      System.out.println(topRow.toString());
    } catch (Exception e) {
      if (!executorService.isShutdown()) {
        executorService.shutdown();
      }
      e.printStackTrace();
    }
    executorService.shutdown();
  }

}
