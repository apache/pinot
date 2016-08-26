package com.linkedin.thirdeye.client.pinot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;

/**
 * Util class for generated PQL queries (pinot).
 */
public class PqlUtils {
  private static final Joiner AND = Joiner.on(" AND ");
  private static final Joiner COMMA = Joiner.on(",");
  private static final Joiner EQUALS = Joiner.on(" = ");
  private static final Logger LOGGER = LoggerFactory.getLogger(PqlUtils.class);
  private static final int DEFAULT_TOP = 300000;

  /**
   * Returns sql to calculate the sum of all raw metrics required for <tt>request</tt>, grouped by
   * time within the requested date range. </br>
   * Due to the summation, all metric column values can be assumed to be doubles.
   */
  public static String getPql(ThirdEyeRequest request, TimeSpec dataTimeSpec) {
    // TODO handle request.getFilterClause()
    return getPql(request.getCollection(), request.getMetricFunctions(),
        request.getStartTimeInclusive(), request.getEndTimeExclusive(), request.getFilterSet(),
        request.getGroupBy(), request.getGroupByTimeGranularity(), dataTimeSpec);
  }

  /**
   * Returns pqls to handle tables where metric names are a single dimension column,
   * and the metric values are all in a single value column
   * @param request
   * @param dataTimeSpec
   * @param collectionConfig
   * @return
   */
  public static List<String> getMetricAsDimensionPqls(ThirdEyeRequest request, TimeSpec dataTimeSpec, CollectionConfig collectionConfig) {

    // select sum(metric_values_column) from collection
    // where time_clause and metric_names_column=function.getMetricName

    String metricValuesColumn = collectionConfig.getMetricValuesColumn();
    String metricNamesColumn = collectionConfig.getMetricNamesColumn();

    List<String> metricAsDimensionPqls = getMetricAsDimensionPqls(request.getCollection(), request.getMetricFunctions(),
        request.getStartTimeInclusive(), request.getEndTimeExclusive(), request.getFilterSet(),
        request.getGroupBy(), request.getGroupByTimeGranularity(), dataTimeSpec,
        metricValuesColumn, metricNamesColumn);

    return metricAsDimensionPqls;
  }

  static List<String> getMetricAsDimensionPqls(String collection, List<MetricFunction> metricFunctions, DateTime startTime,
      DateTime endTimeExclusive, Multimap<String, String> filterSet, List<String> groupBy,
      TimeGranularity timeGranularity, TimeSpec dataTimeSpec, String metricValuesColumn,
      String metricNamesColumn) {

    List<String> metricAsDimensionPqls = new ArrayList<>();
    for (MetricFunction metricFunction : metricFunctions) {
      String metricAsDimensionPql = getMetricAsDimensionPql(collection, metricFunction, startTime, endTimeExclusive, filterSet, groupBy, timeGranularity,
          dataTimeSpec, metricValuesColumn, metricNamesColumn);
      metricAsDimensionPqls.add(metricAsDimensionPql);
    }
    return metricAsDimensionPqls;
  }

  static String getMetricAsDimensionPql(String collection, MetricFunction metricFunction, DateTime startTime,
      DateTime endTimeExclusive, Multimap<String, String> filterSet, List<String> groupBy,
      TimeGranularity timeGranularity, TimeSpec dataTimeSpec, String metricValuesColumn,
      String metricNamesColumn) {

    StringBuilder sb = new StringBuilder();
    String selectionClause = getMetricAsDimensionSelectionClause(metricFunction, metricValuesColumn);

    sb.append("SELECT ").append(selectionClause).append(" FROM ").append(collection);
    String betweenClause = getBetweenClause(startTime, endTimeExclusive, dataTimeSpec);
    sb.append(" WHERE ").append(betweenClause);

    String metricWhereClause = getMetricWhereClause(metricFunction, metricNamesColumn);
    sb.append(metricWhereClause);

    String dimensionWhereClause = getDimensionWhereClause(filterSet);
    if (StringUtils.isNotBlank(dimensionWhereClause)) {
      sb.append(" AND ").append(dimensionWhereClause);
    }
    String groupByClause = getDimensionGroupByClause(groupBy, timeGranularity, dataTimeSpec);
    if (StringUtils.isNotBlank(groupByClause)) {
      sb.append(" ").append(groupByClause);
      sb.append(" TOP ").append(DEFAULT_TOP);
    }

    return sb.toString();
  }

  static String getPql(String collection, List<MetricFunction> metricFunctions, DateTime startTime,
      DateTime endTimeExclusive, Multimap<String, String> filterSet, List<String> groupBy,
      TimeGranularity timeGranularity, TimeSpec dataTimeSpec) {

    StringBuilder sb = new StringBuilder();
    String selectionClause = getSelectionClause(metricFunctions);

    sb.append("SELECT ").append(selectionClause).append(" FROM ").append(collection);
    String betweenClause = getBetweenClause(startTime, endTimeExclusive, dataTimeSpec);
    sb.append(" WHERE ").append(betweenClause);

    String dimensionWhereClause = getDimensionWhereClause(filterSet);
    if (StringUtils.isNotBlank(dimensionWhereClause)) {
      sb.append(" AND ").append(dimensionWhereClause);
    }
    String groupByClause = getDimensionGroupByClause(groupBy, timeGranularity, dataTimeSpec);
    if (StringUtils.isNotBlank(groupByClause)) {
      sb.append(" ").append(groupByClause);
      sb.append(" TOP ").append(DEFAULT_TOP);
    }

    return sb.toString();
  }

  static String getMetricWhereClause(MetricFunction metricFunction, String metricNameColumn) {
    StringBuilder builder = new StringBuilder();
    if (!metricFunction.getMetricName().equals("*")) {
      builder.append(" AND ");
      builder.append(String.format("%s='%s'", metricNameColumn, metricFunction.getMetricName()));
    }
    return builder.toString();
  }

  /**
   * SUM each metric over the current time bucket.
   */
  static String getSelectionClause(List<MetricFunction> metricFunctions) {
    StringBuilder builder = new StringBuilder();
    String delim = "";
    for (MetricFunction function : metricFunctions) {
      builder.append(delim);
      builder.append(function.getFunctionName()).append("(").append(function.getMetricName())
          .append(")");
      delim = ", ";
    }
    return builder.toString();
  }

  static String getMetricAsDimensionSelectionClause(MetricFunction metricFunction, String metricValueColumn) {
    StringBuilder builder = new StringBuilder();
    String metricName = metricValueColumn;
    if (metricFunction.getMetricName().equals("*")) {
      metricName = "*";
    }
    builder.append(metricFunction.getFunctionName()).append("(").append(metricName).append(")");
    return builder.toString();
  }

  static String getBetweenClause(DateTime start, DateTime endExclusive, TimeSpec timeFieldSpec) {
    TimeGranularity dataGranularity = timeFieldSpec.getDataGranularity();
    long startMillis = start.getMillis();
    long endMillis = endExclusive.getMillis();
    long dataGranularityMillis = dataGranularity.toMillis();

    // Shrink start and end as per data granularity
    long startAlignmentDelta = startMillis % dataGranularityMillis;
    if (startAlignmentDelta != 0) {
      long startMillisAligned = startMillis + dataGranularityMillis - startAlignmentDelta;
      start = new DateTime(startMillisAligned);
    }

    long endAligmentDelta = endMillis % dataGranularityMillis;
    if (endAligmentDelta != 0) {
      long endMillisAligned = endMillis - endAligmentDelta;
      endExclusive = new DateTime(endMillisAligned);
    }

    String startQueryTime;
    String endQueryTimeExclusive;

    String timeField = timeFieldSpec.getColumnName();
    String timeFormat = timeFieldSpec.getFormat();
    if (timeFormat == null || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
      long startInConvertedUnits = dataGranularity.convertToUnit(start.getMillis());
      long endInConvertedUnits = dataGranularity.convertToUnit(endExclusive.getMillis());
      startQueryTime = String.valueOf(startInConvertedUnits);
      endQueryTimeExclusive = (endInConvertedUnits == startInConvertedUnits + 1) ?
          startQueryTime : String.valueOf(endInConvertedUnits);
    } else {
      DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZoneUTC();
      startQueryTime = dateTimeFormatter.print(start);
      endQueryTimeExclusive = dateTimeFormatter.print(endExclusive);
    }

    if (startQueryTime.equals(endQueryTimeExclusive)) {
      return String.format(" %s = %s", timeField, startQueryTime);
    } else {
      return String.format(" %s >= %s AND %s < %s", timeField, startQueryTime, timeField,
          endQueryTimeExclusive);
    }
  }

  static String getDimensionWhereClause(Multimap<String, String> dimensionValues) {
    List<String> components = new ArrayList<>();
    for (Map.Entry<String, Collection<String>> entry : dimensionValues.asMap().entrySet()) {
      String key = entry.getKey();
      Collection<String> values = entry.getValue();
      String component;
      if (values.isEmpty()) {
        continue;
      } else if (values.size() == 1) {
        component = EQUALS.join(key, String.format("'%s'", values.iterator().next().trim()));
      } else {
        List<String> quotedValues = new ArrayList<>(values.size());
        for (String value : values) {
          quotedValues.add(String.format("'%s'", value.trim()));
        }
        component = String.format("%s IN (%s)", key, COMMA.join(quotedValues));
      }
      components.add(component);
    }
    if (components.isEmpty()) {
      return null;
    }
    return AND.join(components);
  }

  static String getDimensionGroupByClause(List<String> groupBy,
      TimeGranularity aggregationGranulity, TimeSpec timeSpec) {
    String timeColumnName = timeSpec.getColumnName();
    List<String> groups = new LinkedList<String>();
    if (aggregationGranulity != null && !groups.contains(timeColumnName)) {
      groups.add(timeColumnName);
    }
    if (groupBy != null) {
      groups.addAll(groupBy);
    }
    if (groups.isEmpty()) {
      return "";
    }
    return String.format("GROUP BY %s", COMMA.join(groups));
  }

  public static String getDataTimeRangeSql(String collection, String timeColumnName) {
    return String.format("select min(%s), max(%s) from %s", timeColumnName, timeColumnName,
        collection);
  }

}
