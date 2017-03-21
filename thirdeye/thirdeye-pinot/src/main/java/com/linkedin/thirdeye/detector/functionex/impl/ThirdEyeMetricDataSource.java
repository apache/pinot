package com.linkedin.thirdeye.detector.functionex.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdEyeMetricDataSource implements AnomalyFunctionExDataSource<String, DataFrame> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeMetricDataSource.class);

  public static final int TOP_K = 10000;

  public static final String COLUMN_TIMESTAMP = "timestamp";
  public static final String COLUMN_METRIC = "metric";

  AnomalyFunctionExDataSource<String, DataFrame> pinot;
  DatasetConfigManager manager;

  // metric://pinot/dataset/metric/sum/?k1=v2&k2=v2&filters=dimension1=v1,dimension2%3Cv2

  public ThirdEyeMetricDataSource(AnomalyFunctionExDataSource<String, DataFrame> pinot, DatasetConfigManager manager) {
    this.pinot = pinot;
    this.manager = manager;
  }

  @Override
  public DataFrame query(String query, AnomalyFunctionExContext context) throws Exception {
    URI uri = URI.create(query);
    if(uri.getHost() == null || !uri.getHost().equals("pinot"))
      throw new IllegalArgumentException("source required (only 'pinot' allowed)");
    if(uri.getPath() == null || uri.getPath().split("/").length < 4)
      throw new IllegalArgumentException("dataset, metric and function required");

    String[] fragments = uri.getPath().split("/");

    String dataset = fragments[1]; // NOTE: path starts with '/'
    String metric = fragments[2];
    String function = fragments[3];

    // user filters
    Collection<Filter> filters = new ArrayList<>();
    if(uri.getQuery() != null) {
      List<NameValuePair> params = URLEncodedUtils.parse(uri, "UTF-8");
      filters.addAll(getBasicFilters(params));
      filters.addAll(getAdvancedFilters(params));
    }

    // time filter
    DatasetConfigDTO dto = this.manager.findByDataset(dataset);
    if(dto == null)
      throw new IllegalArgumentException(String.format("Could not find dataset '%s'", dataset));

    final TimeGranularity tg = new TimeGranularity(dto.getTimeDuration(), dto.getTimeUnit());
    filters.addAll(getTimeFilters(dto.getTimeColumn(), tg, context.getMonitoringWindowStart(), context.getMonitoringWindowEnd()));

    // build query
    String pinotQuery = makeQuery(dataset, metric, function, filters);

    DataFrame result = this.pinot.query(pinotQuery, context);

    // check for truncation
    if(result.size() >= TOP_K)
      throw new IllegalArgumentException(String.format("Got more than %d rows. Results may be truncated.", TOP_K));

    // standardize dataframe format
    result.renameSeries(dto.getTimeColumn(),COLUMN_TIMESTAMP);
    result.renameSeries(function + "_" + metric, COLUMN_METRIC);

    Series s = result.getLongs(COLUMN_TIMESTAMP).map(new LongSeries.LongFunction() {
      @Override
      public long apply(long... values) {
        return tg.toMillis(values[0]);
      }
    });
    result.addSeries(COLUMN_TIMESTAMP, s);

    return result.sortedBy(COLUMN_TIMESTAMP);
  }

  static Collection<Filter> getAdvancedFilters(List<NameValuePair> params) {
    for(NameValuePair p : params) {
      if(p.getName().equals("filters"))
        return parseFilters(p.getValue());
    }
    return Collections.emptyList();
  }

  static Collection<Filter> getBasicFilters(List<NameValuePair> params) {
    Collection<Filter> filters = new ArrayList<>();
    for(NameValuePair p : params) {
      if(!p.getName().equals("filters"))
        filters.add(new Filter(p.getName(), "=", p.getValue()));
    }
    return filters;
  }

  static Collection<Filter> getTimeFilters(String column, TimeGranularity tg, long start, long stop) {
    String minTime = String.valueOf(tg.convertToUnit(start));
    String maxTime = String.valueOf(tg.convertToUnit(stop));

    Collection<Filter> filters = new ArrayList<>();
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
    Set<String> s = new HashSet<>();
    for(Filter f : filters) {
      s.add(f.key);
    }
    return String.join(", ", s);
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
