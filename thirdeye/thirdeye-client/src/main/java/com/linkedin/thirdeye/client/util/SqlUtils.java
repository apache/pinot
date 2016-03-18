package com.linkedin.thirdeye.client.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Joiner;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

public class SqlUtils {
  private static final Joiner AND = Joiner.on(" AND ");
  private static final Joiner OR = Joiner.on(" OR ");
  private static final Joiner COMMA = Joiner.on(",");
  private static final Joiner EQUALS = Joiner.on(" = ");

  public static String getThirdEyeSql(ThirdEyeRequest request) {
    String metricFunction = request.getMetricFunction().getSqlFunction();
    String collection = request.getCollection();
    DateTime startTime = request.getStartTimeInclusive();
    DateTime endTime = request.getEndTimeExclusive();
    if (metricFunction == null) {
      throw new IllegalStateException("Must provide metric function, e.g. `AGGREGATE_1_HOURS(m1)`");
    }
    if (collection == null) {
      throw new IllegalStateException("Must provide collection name");
    }
    if (startTime == null || endTime == null) {
      throw new IllegalStateException("Must provide start and end time");
    } else if (startTime.isAfter(endTime)) {
      throw new IllegalStateException(
          "Start time must come before end: " + startTime + ", " + endTime);
    }
    return getSql(metricFunction, collection, startTime, endTime, request.getDimensionValues(),
        request.getGroupBy());
  }

  static String getSql(String metricFunction, String collection, DateTime start, DateTime end,
      Multimap<String, String> dimensionValues, Set<String> groupBy) {
    StringBuilder sb = new StringBuilder();

    sb.append("SELECT ").append(metricFunction).append(" FROM ").append(collection)
        .append(" WHERE ").append(getBetweenClause(start, end));

    String dimensionWhereClause = getDimensionWhereClause(dimensionValues);
    if (dimensionWhereClause != null) {
      sb.append(" AND ").append(dimensionWhereClause);
    }

    String groupByClause = getDimensionGroupByClause(groupBy);
    if (groupByClause != null) {
      sb.append(" ").append(groupByClause);
    }

    return sb.toString();
  }

  static String getBetweenClause(DateTime start, DateTime end) {
    return String.format("time BETWEEN '%s' AND '%s'", getDateString(start), getDateString(end));
  }

  static String getDimensionWhereClause(Multimap<String, String> dimensionValues) {
    List<String> components = new ArrayList<>();
    for (Map.Entry<String, Collection<String>> entry : dimensionValues.asMap().entrySet()) {
      List<String> equals = new ArrayList<>(entry.getValue().size());
      for (String value : entry.getValue()) {
        equals.add(EQUALS.join(entry.getKey(), String.format("'%s'", value)));
      }

      components.add(OR.join(equals));
    }
    if (components.isEmpty()) {
      return null;
    }
    return AND.join(components);
  }

  static String getDimensionGroupByClause(Set<String> groupBy) {
    if (groupBy == null) {
      return null;
    }
    List<String> components = new ArrayList<>();
    for (String group : groupBy) {
      components.add(String.format("'%s'", group));
    }
    if (components.isEmpty()) {
      return null;
    }
    return String.format("GROUP BY %s", COMMA.join(components));
  }

  public static String getDateString(DateTime dateTime) {
    return ISODateTimeFormat.dateTimeNoMillis().print(dateTime.toDateTime(DateTimeZone.UTC));
  }

}
