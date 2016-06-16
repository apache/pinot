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
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

/**
 * Util class for generated PQL queries (pinot).
 */
public class PqlUtils {
  private static final Joiner AND = Joiner.on(" AND ");
  private static final Joiner COMMA = Joiner.on(",");
  private static final Joiner EQUALS = Joiner.on(" = ");
  private static final int DEFAULT_TOP = 300;
  private static final Logger LOGGER = LoggerFactory.getLogger(PqlUtils.class);
  private static ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE =
      ThirdEyeCacheRegistry.getInstance();

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

  static String getPql(String collection, List<MetricFunction> metricFunctions, DateTime startTime,
      DateTime endTime, Multimap<String, String> filterSet, List<String> groupBy,
      TimeGranularity timeGranularity, TimeSpec dataTimeSpec) {
    StringBuilder sb = new StringBuilder();
    String selectionClause = getSelectionClause(metricFunctions);
    sb.append("SELECT ").append(selectionClause).append(" FROM ").append(collection);
    String betweenClause = getBetweenClause(startTime, endTime, dataTimeSpec);
    sb.append(" WHERE ").append(betweenClause);
    String dimensionWhereClause = getDimensionWhereClause(filterSet);
    if (StringUtils.isNotBlank(dimensionWhereClause)) {
      sb.append(" AND ").append(dimensionWhereClause);
    }
    String groupByClause = getDimensionGroupByClause(groupBy, timeGranularity, dataTimeSpec);
    if (StringUtils.isNotBlank(groupByClause)) {
      sb.append(" ").append(groupByClause);
      int bucketCount = Integer.MAX_VALUE;
      sb.append(" TOP ").append(bucketCount);

    }

    return sb.toString();
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

  static String getBetweenClause(DateTime start, DateTime end, TimeSpec timeFieldSpec) {
    TimeGranularity dataGranularity = timeFieldSpec.getDataGranularity();
    long startMillis = start.getMillis();
    long endMillis = end.getMillis();
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
      end = new DateTime(endMillisAligned);
    }

    String startQueryTime;
    String endQueryTime;

    String timeField = timeFieldSpec.getColumnName();
    String timeFormat = timeFieldSpec.getFormat();
    if (timeFormat == null || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
      long startInConvertedUnits = dataGranularity.convertToUnit(start.getMillis());
      long endInConvertedUnits = dataGranularity.convertToUnit(end.getMillis());
      startQueryTime = String.valueOf(startInConvertedUnits);
      endQueryTime = String.valueOf(endInConvertedUnits);
    } else {
      DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZoneUTC();
      startQueryTime = dateTimeFormatter.print(start);
      endQueryTime = dateTimeFormatter.print(end);
    }

    if (startQueryTime.equals(endQueryTime)) {
      return String.format(" %s = %s", timeField, startQueryTime);
    } else {
      return String.format(" %s >= %s AND %s <= %s", timeField, startQueryTime, timeField,
          endQueryTime);
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
