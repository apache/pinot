package com.linkedin.thirdeye.client.diffsummary;

import com.google.common.collect.Multimap;
import java.util.List;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.datasource.MetricExpression;

public interface OLAPDataBaseClient {

  void setCollection(String collection);

  void setMetricExpression(MetricExpression metricExpressions);

  void setBaselineStartInclusive(DateTime dateTime);

  void setBaselineEndExclusive(DateTime dateTime);

  void setCurrentStartInclusive(DateTime dateTime);

  void setCurrentEndExclusive(DateTime dateTime);

  Row getTopAggregatedValues(Multimap<String, String> filterSets) throws Exception;

  List<List<Row>> getAggregatedValuesOfDimension(Dimensions dimensions, Multimap<String, String> filterSets)
      throws Exception;

  List<List<Row>> getAggregatedValuesOfLevels(Dimensions dimensions, Multimap<String, String> filterSets)
      throws Exception;
}
