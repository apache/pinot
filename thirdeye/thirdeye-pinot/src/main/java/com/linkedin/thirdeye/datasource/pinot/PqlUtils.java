package com.linkedin.thirdeye.datasource.pinot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean.DimensionAsMetricProperties;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

/**
 * Util class for generated PQL queries (pinot).
 */
public class PqlUtils {
  private static final Joiner AND = Joiner.on(" AND ");
  private static final Joiner COMMA = Joiner.on(",");

  private static final String PREFIX_EXCLUDE = "!";

  private static final Logger LOGGER = LoggerFactory.getLogger(PqlUtils.class);
  private static final int DEFAULT_TOP = 100000;


  /**
   * Returns sql to calculate the sum of all raw metrics required for <tt>request</tt>, grouped by
   * time within the requested date range. </br>
   * Due to the summation, all metric column values can be assumed to be doubles.
   * @throws ExecutionException
   */
  public static String getPql(ThirdEyeRequest request, MetricFunction metricFunction,
      Multimap<String, String> filterSet, TimeSpec dataTimeSpec) throws ExecutionException {
    // TODO handle request.getFilterClause()

    return getPql(metricFunction, request.getStartTimeInclusive(), request.getEndTimeExclusive(), filterSet,
        request.getGroupBy(), request.getGroupByTimeGranularity(), dataTimeSpec);
  }


  private static String getPql(MetricFunction metricFunction, DateTime startTime,
      DateTime endTimeExclusive, Multimap<String, String> filterSet, List<String> groupBy,
      TimeGranularity timeGranularity, TimeSpec dataTimeSpec) throws ExecutionException {

    MetricConfigDTO metricConfig = ThirdEyeUtils.getMetricConfigFromId(metricFunction.getMetricId());
    String dataset = metricFunction.getDataset();

    StringBuilder sb = new StringBuilder();
    String selectionClause = getSelectionClause(metricConfig, metricFunction);

    String tableName = ThirdEyeUtils.computeTableName(dataset);

    sb.append("SELECT ").append(selectionClause).append(" FROM ").append(tableName);
    String betweenClause = getBetweenClause(startTime, endTimeExclusive, dataTimeSpec, dataset);
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

  private static String getSelectionClause(MetricConfigDTO metricConfig, MetricFunction metricFunction) {
    StringBuilder builder = new StringBuilder();
    String metricName = null;
    if (metricFunction.getMetricName().equals("*")) {
      metricName = "*";
    } else {
      metricName = metricConfig.getName();
    }
    builder.append(metricFunction.getFunctionName()).append("(").append(metricName).append(")");
    return builder.toString();
  }


  /**
   * Returns pqls to handle tables where metric names are a single dimension column,
   * and the metric values are all in a single value column
   * @param request
   * @param dataTimeSpec
   * @return
   * @throws Exception
   */
  public static String getDimensionAsMetricPql(ThirdEyeRequest request, MetricFunction metricFunction,
      Multimap<String, String> filterSet, TimeSpec dataTimeSpec, DatasetConfigDTO datasetConfig) throws Exception {

    // select sum(metric_values_column) from collection
    // where time_clause and metric_names_column=metric_name
    MetricConfigDTO metricConfig = metricFunction.getMetricConfig();
    Map<String, String> metricProperties = metricConfig.getMetricProperties();
    if (metricProperties == null || metricProperties.isEmpty()) {
      throw new RuntimeException("Metric properties must have properties " + DimensionAsMetricProperties.values());
    }
    String metricNames =
        metricProperties.get(DimensionAsMetricProperties.METRIC_NAMES.toString());
    String metricNamesColumns =
        metricProperties.get(DimensionAsMetricProperties.METRIC_NAMES_COLUMNS.toString());
    String metricValuesColumn =
        metricProperties.get(DimensionAsMetricProperties.METRIC_VALUES_COLUMN.toString());
    if (StringUtils.isBlank(metricNames) || StringUtils.isBlank(metricNamesColumns) || StringUtils.isBlank(metricValuesColumn)) {
      throw new RuntimeException("Metric properties must have properties " + DimensionAsMetricProperties.values());
    }
    List<String> metricNamesList =
        Lists.newArrayList(metricNames.split(MetricConfigBean.METRIC_PROPERTIES_SEPARATOR));
    List<String> metricNamesColumnsList =
        Lists.newArrayList(metricNamesColumns.split(MetricConfigBean.METRIC_PROPERTIES_SEPARATOR));
    if (metricNamesList.size() != metricNamesColumnsList.size()) {
      throw new RuntimeException("Must provide same number of metricNames in " + metricNames
          + " as metricNamesColumns in " + metricNamesColumns);
    }

    String dimensionAsMetricPql = getDimensionAsMetricPql(metricFunction,
        request.getStartTimeInclusive(), request.getEndTimeExclusive(), filterSet,
        request.getGroupBy(), request.getGroupByTimeGranularity(), dataTimeSpec,
        metricNamesList, metricNamesColumnsList, metricValuesColumn);

    return dimensionAsMetricPql;
  }


  private static String getDimensionAsMetricPql(MetricFunction metricFunction, DateTime startTime,
      DateTime endTimeExclusive, Multimap<String, String> filterSet, List<String> groupBy,
      TimeGranularity timeGranularity, TimeSpec dataTimeSpec,
      List<String> metricNames, List<String> metricNamesColumns, String metricValuesColumn)
          throws ExecutionException {

    MetricConfigDTO metricConfig = metricFunction.getMetricConfig();
    String dataset = metricFunction.getDataset();

    StringBuilder sb = new StringBuilder();
    String selectionClause = getDimensionAsMetricSelectionClause(metricFunction, metricValuesColumn);

    String tableName = ThirdEyeUtils.computeTableName(dataset);

    sb.append("SELECT ").append(selectionClause).append(" FROM ").append(tableName);
    String betweenClause = getBetweenClause(startTime, endTimeExclusive, dataTimeSpec, dataset);
    sb.append(" WHERE ").append(betweenClause);

    String metricWhereClause = getMetricWhereClause(metricConfig, metricFunction, metricNames, metricNamesColumns);
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


  private static String getMetricWhereClause(MetricConfigDTO metricConfig, MetricFunction metricFunction,
      List<String> metricNames, List<String> metricNamesColumns) {
    StringBuilder builder = new StringBuilder();
    if (!metricFunction.getMetricName().equals("*")) {
      for (int i = 0; i < metricNamesColumns.size(); i++) {
        String metricName = metricNames.get(i);
        String metricNamesColumn = metricNamesColumns.get(i);
        builder.append(" AND ");
        builder.append(String.format("%s='%s'", metricNamesColumn, metricName));
      }
    }
    return builder.toString();
  }


  private static String getDimensionAsMetricSelectionClause(MetricFunction metricFunction, String metricValueColumn) {
    StringBuilder builder = new StringBuilder();
    String metricName = metricValueColumn;
    if (metricFunction.getMetricName().equals("*")) {
      metricName = "*";
    }
    builder.append(metricFunction.getFunctionName()).append("(").append(metricName).append(")");
    return builder.toString();
  }

  static String getBetweenClause(DateTime start, DateTime endExclusive, TimeSpec timeFieldSpec, String dataset)
      throws ExecutionException {
    TimeGranularity dataGranularity = timeFieldSpec.getDataGranularity();
    long startMillis = start.getMillis();
    long endMillis = endExclusive.getMillis();
    long dataGranularityMillis = dataGranularity.toMillis();

    String timeField = timeFieldSpec.getColumnName();
    String timeFormat = timeFieldSpec.getFormat();
    if (timeFormat == null || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
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
    }

    String startQueryTime;
    String endQueryTimeExclusive;


    if (timeFormat == null || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
      long startInConvertedUnits = dataGranularity.convertToUnit(start.getMillis());
      long endInConvertedUnits = dataGranularity.convertToUnit(endExclusive.getMillis());
      startQueryTime = String.valueOf(startInConvertedUnits);
      endQueryTimeExclusive = (endInConvertedUnits == startInConvertedUnits + 1) ?
          startQueryTime : String.valueOf(endInConvertedUnits);
    } else {
      DateTimeFormatter inputDataDateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZone(Utils.getDataTimeZone(dataset));
      startQueryTime = inputDataDateTimeFormatter.print(start);
      endQueryTimeExclusive = inputDataDateTimeFormatter.print(endExclusive);
    }

    if (startQueryTime.equals(endQueryTimeExclusive)) {
      return String.format(" %s = %s", timeField, startQueryTime);
    } else {
      return String.format(" %s >= %s AND %s < %s", timeField, startQueryTime, timeField,
          endQueryTimeExclusive);
    }
  }

  private static String getDimensionWhereClause(Multimap<String, String> dimensionValues) {
    List<String> components = new ArrayList<>();
    for (Map.Entry<String, Collection<String>> entry : dimensionValues.asMap().entrySet()) {
      String key = entry.getKey();
      Collection<String> values = entry.getValue();
      if (values.isEmpty()) {
        continue;
      }

      List<String> include = new ArrayList<>();
      List<String> exclude = new ArrayList<>();

      for (String value : values) {
        if (value.startsWith(PREFIX_EXCLUDE)) {
          exclude.add(quote(value.substring(PREFIX_EXCLUDE.length())));
        } else {
          include.add(quote(value));
        }
      }

      if (!include.isEmpty()) {
        components.add(String.format("%s IN (%s)", key, COMMA.join(include)));
      }

      if (!exclude.isEmpty()) {
        components.add(String.format("%s NOT IN (%s)", key, COMMA.join(exclude)));
      }
    }
    if (components.isEmpty()) {
      return null;
    }
    return AND.join(components);
  }

  private static String getDimensionGroupByClause(List<String> groupBy,
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

  public static String getDataTimeRangeSql(String dataset, String timeColumnName) {
    return String.format("select min(%s), max(%s) from %s", timeColumnName, timeColumnName,
        dataset);
  }

  /**
   * Surrounds a value with quote characters that are not contained in the value itself.
   *
   * @param value value to be quoted
   * @return quoted value
   * @throws IllegalArgumentException if no unused quote char can be found
   */
  private static String quote(String value) {
    String quoteChar = "\"";
    if (value.contains(quoteChar)) {
      quoteChar = "\'";
    }
    if (value.contains(quoteChar)) {
      throw new IllegalArgumentException(String.format("Could not find quote char for expression: %s", value));
    }
    return String.format("%s%s%s", quoteChar, value, quoteChar);
  }
}
