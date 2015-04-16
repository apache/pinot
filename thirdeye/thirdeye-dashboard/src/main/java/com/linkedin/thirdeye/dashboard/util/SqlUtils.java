package com.linkedin.thirdeye.dashboard.util;

import com.google.common.base.Joiner;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlUtils {
  private static final Joiner AND = Joiner.on(" AND ");
  private static final Joiner COMMA = Joiner.on(",");
  private static final Joiner EQUALS = Joiner.on(" = ");

  public static String getSql(String metricFunction,
                              String collection,
                              DateTime start,
                              DateTime end,
                              Map<String, String> dimensionValues) {
    StringBuilder sb = new StringBuilder();

    sb.append("SELECT ")
        .append(metricFunction)
        .append(" FROM ")
        .append(collection)
        .append(" WHERE ")
        .append(getBetweenClause(start, end));

    String dimensionWhereClause = getDimensionWhereClause(dimensionValues);
    if (dimensionWhereClause != null) {
      sb.append(" AND ").append(dimensionWhereClause);
    }

    String groupByClause = getDimensionGroupByClause(dimensionValues);
    if (groupByClause != null) {
      sb.append(" ").append(groupByClause);
    }

    return sb.toString();
  }

  public static String getBetweenClause(DateTime start, DateTime end) {
    return String.format("time BETWEEN '%s' AND '%s'", getDateString(start), getDateString(end));
  }

  public static String getDimensionWhereClause(Map<String, String> dimensionValues) {
    List<String> components = new ArrayList<>();
    for (Map.Entry<String, String> entry : dimensionValues.entrySet()) {
      if ("!".equals(entry.getValue())) {
        // Part of group by clause
        continue;
      }
      components.add(EQUALS.join(entry.getKey(), String.format("'%s'", entry.getValue())));
    }
    if (components.isEmpty()) {
      return null;
    }
    return AND.join(components);
  }

  public static String getDimensionGroupByClause(Map<String, String> dimensionValues) {
    List<String> components = new ArrayList<>();
    for (Map.Entry<String, String> entry : dimensionValues.entrySet()) {
      if ("!".equals(entry.getValue())) {
        components.add(entry.getKey());
      }
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
