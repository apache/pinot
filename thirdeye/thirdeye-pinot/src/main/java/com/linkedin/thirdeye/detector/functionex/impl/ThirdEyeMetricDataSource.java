package com.linkedin.thirdeye.detector.functionex.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import com.linkedin.thirdeye.detector.functionex.dataframe.LongSeries;
import com.linkedin.thirdeye.detector.functionex.dataframe.Series;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ThirdEyeMetricDataSource implements AnomalyFunctionExDataSource<String, DataFrame> {

  private static final int TOP_K = 10000;

  public static final String COLUMN_TIMESTAMP = "timestamp";
  public static final String COLUMN_METRIC = "metric";

  AnomalyFunctionExDataSource<String, DataFrame> pinot;
  DatasetConfigManager manager;

  // dataset/metric/sum/dimension1=v1,dimension2=v2

  public ThirdEyeMetricDataSource(AnomalyFunctionExDataSource<String, DataFrame> pinot, DatasetConfigManager manager) {
    this.pinot = pinot;
    this.manager = manager;
  }

  @Override
  public DataFrame query(String query, AnomalyFunctionExContext context) throws Exception {
    String[] fragments = query.split("/");
    if(fragments.length < 3)
      throw new IllegalArgumentException("requires at least dataset, metric, and function name");

    String dataset = fragments[0];
    String metric = fragments[1];
    String function = fragments[2];
    String filterString = null;
    if(fragments.length >= 4)
      filterString =  fragments[3];

    Collection<Filter> filters = parseFilters(filterString);

    // fetch dataset config
    DatasetConfigDTO dto = this.manager.findByDataset(dataset);
    if(dto == null)
      throw new IllegalArgumentException(String.format("Could not find dataset '%s'", dataset));

    // Time filter
    TimeGranularity tg = new TimeGranularity(dto.getTimeDuration(), dto.getTimeUnit());
    augmentFiltersWithTime(filters, dto.getTimeColumn(), tg, context.getMonitoringWindowStart(), context.getMonitoringWindowEnd());

    // build query
    String pinotQuery = makeQuery(dataset, metric, function, filters);

    DataFrame result = this.pinot.query(pinotQuery, context);

    result.renameSeries(dto.getTimeColumn(),COLUMN_TIMESTAMP);
    result.renameSeries(function + "_" + metric, COLUMN_METRIC);

    Series s = result.toLongs(COLUMN_TIMESTAMP).map(new LongSeries.LongFunction() {
      @Override
      public long apply(long value) {
        return tg.toMillis(value);
      }
    });
    result.addSeries(COLUMN_TIMESTAMP, s);

    return result.sortBySeries();
  }

  static Collection<Filter> augmentFiltersWithTime(Collection<Filter> filters, String column, TimeGranularity tg, long start, long stop) {
    String minTime = String.valueOf(tg.convertToUnit(start));
    String maxTime = String.valueOf(tg.convertToUnit(stop));

    filters.add(new Filter(column, ">", minTime));
    filters.add(new Filter(column, "<=", maxTime));
    return filters;
  }

  static List<Filter> parseFilters(String filterString) {
    if(filterString == null || filterString.isEmpty())
      return new ArrayList<>();

    List<Filter> filters = new ArrayList<>();
    String[] fragments = filterString.split(",");
    for(String s : fragments) {
      String[] sfrag = s.split("<=|>=|=|<|>|!=", 2);
      if(sfrag.length != 2)
        throw new IllegalArgumentException(String.format("Could not parse filter expression '%s'", s));
      String key = sfrag[0];
      String value = sfrag[1];
      String op = s.substring(key.length(), s.length() - value.length());
      filters.add(new Filter(key, op, value));
    }
    return filters;
  }

  static String makeDimensions(Collection<Filter> filters) {
    Set<String> keySet = new HashSet<>();
    for(Filter f : filters) {
      keySet.add(f.key);
    }
    return String.join(", ", keySet);
  }

  static String makeFilters(Collection<Filter> filters) {
    Multimap<String, Filter> mmap = ArrayListMultimap.create();
    for(Filter f : filters) {
      mmap.put(f.key, f);
    }

    List<String> exprAnd = new ArrayList<>();
    for(Map.Entry<String, Collection<Filter>> e : mmap.asMap().entrySet()) {
      boolean isAlternativesClause = true;
      List<String> exprOr = new ArrayList<>();
      for(Filter f : e.getValue()) {
        if(!"=".equals(f.operator))
          isAlternativesClause = false;
        exprOr.add(filter2string(f));
      }

      String operator = "AND";
      if(isAlternativesClause)
        operator = "OR";

      exprAnd.add(joinExpressions(operator, exprOr));
    }
    return joinExpressions("AND", exprAnd);
  }

  static String filter2string(Filter f) {
    return String.format("(%s%s'%s')", f.key, f.operator, f.value);
  }

  static String joinExpressions(String operator, List<String> expressions) {
    if(expressions.isEmpty())
      return "";
    return String.format("(%s)", String.join(String.format(" %s ", operator), expressions));
  }

  static String makeQuery(String dataset, String metric, String function, Collection<Filter> filters) {
    String filterString = "";
    String groupbyString = "";
    if(!filters.isEmpty()) {
      filterString = String.format("WHERE %s", makeFilters(filters));
      groupbyString = String.format("GROUP BY %s", makeDimensions(filters));
    }
    return String.format("SELECT %s(%s) FROM %s %s %s TOP %d", function, metric, dataset, filterString, groupbyString, TOP_K);
  }

  static class Filter {
    final String key;
    final String operator;
    final String value;

    public Filter(String key, String operator, String value) {
      this.key = key;
      this.operator = operator;
      this.value = value;
    }
  }
}
