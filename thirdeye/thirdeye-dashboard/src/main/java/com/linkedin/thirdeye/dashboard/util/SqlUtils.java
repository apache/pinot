package com.linkedin.thirdeye.dashboard.util;

import com.google.common.base.Joiner;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlUtils {
  private static final Joiner AND = Joiner.on(" AND ");
  private static final Joiner OR = Joiner.on(" OR ");
  private static final Joiner COMMA = Joiner.on(",");
  private static final Joiner EQUALS = Joiner.on(" = ");

  public static String getSql(String metricFunction,
                              String collection,
                              DateTime start,
                              DateTime end,
                              Map<String, String> dimensionValues) {
    return getSql(metricFunction, collection, start, end, toMultivaluedMap(dimensionValues));
  }

  public static String getSql(String metricFunction,
                              String collection,
                              DateTime start,
                              DateTime end,
                              MultivaluedMap<String, String> dimensionValues) {
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

  public static String getDimensionWhereClause(MultivaluedMap<String, String> dimensionValues) {
    List<String> components = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : dimensionValues.entrySet()) {
      if (entry.getValue().size() == 1 && "!".equals(entry.getValue().get(0))) {
        // Part of group by clause
        continue;
      }

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

  public static String getDimensionGroupByClause(MultivaluedMap<String, String> dimensionValues) {
    List<String> components = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : dimensionValues.entrySet()) {
      if (entry.getValue().size() == 1 && "!".equals(entry.getValue().get(0))) {
        components.add(String.format("'%s'", entry.getKey()));
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

  public static MultivaluedMap<String, String> toMultivaluedMap(Map<String, String> dimensionValues) {
    MultivaluedMap<String, String> multiMap = new MultivaluedMapImpl();
    for (Map.Entry<String, String> entry : dimensionValues.entrySet()) {
      multiMap.putSingle(entry.getKey(), entry.getValue());
    }
    return multiMap;
  }
}
