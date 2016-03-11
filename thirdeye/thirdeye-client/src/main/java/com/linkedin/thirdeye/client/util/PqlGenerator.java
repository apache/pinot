package com.linkedin.thirdeye.client.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import com.google.common.base.Joiner;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

/**
 * PQL string generator (pinot).
 * @author jteoh
 */
public class PqlGenerator {
  private static final Joiner AND = Joiner.on(" AND ");
  private static final Joiner COMMA = Joiner.on(",");
  private static final Joiner EQUALS = Joiner.on(" = ");
  private static final int DEFAULT_TOP = Integer.MAX_VALUE;

  public PqlGenerator() {
  };

  /**
   * Returns sql to calculate the sum of all raw metrics required for <tt>request</tt>, grouped by
   * time within the requested date range. </br>
   * Due to the summation, all metric column values can be assumed to be doubles.
   */
  public String getPql(ThirdEyeRequest request, TimeSpec dataTimeSpec) {
    return getPql(request.getCollection(), request.getRawMetricNames(), request.getStartTime(),
        request.getEndTime(), request.getDimensionValues(), request.getGroupBy(), dataTimeSpec,
        request.shouldGroupByTime());
  }

  String getPql(String collection, List<String> metrics, DateTime startTime, DateTime endTime,
      Multimap<String, String> dimensionValues, Set<String> groupBy, TimeSpec dataTimeSpec,
      boolean shouldGroupByTime) {
    StringBuilder sb = new StringBuilder();
    String selectionClause = getSelectionClause(metrics, dataTimeSpec);
    sb.append("SELECT ").append(selectionClause).append(" FROM ").append(collection);
    String betweenClause = getBetweenClause(startTime, endTime, dataTimeSpec);
    sb.append(" WHERE ").append(betweenClause);
    String dimensionWhereClause = getDimensionWhereClause(dimensionValues);
    if (StringUtils.isNotBlank(dimensionWhereClause)) {
      sb.append(" AND ").append(dimensionWhereClause);
    }
    String groupByClause = getDimensionGroupByClause(groupBy, dataTimeSpec, shouldGroupByTime);
    if (StringUtils.isNotBlank(groupByClause)) {
      sb.append(" ").append(groupByClause);
    }

    int bucketCount = DEFAULT_TOP;
    if (groupBy == null || groupBy.isEmpty()) {
      long duration = endTime.getMillis() - startTime.getMillis();
      bucketCount =
          (int) dataTimeSpec.getBucket().getUnit().convert(duration, TimeUnit.MILLISECONDS) + 1;
    }

    sb.append(" TOP ").append(bucketCount);
    return sb.toString();
  }

  /**
   * SUM each metric over the current time bucket. Ignores the time column if requested, since
   * it is already grouped and summation over that value does not make sense.
   */
  String getSelectionClause(List<String> metrics, TimeSpec timeSpec) {
    String timeColumnName = timeSpec.getColumnName();
    List<String> updatedMetrics = new ArrayList<String>(metrics.size());
    for (int i = 0; i < metrics.size(); i++) {
      String metric = metrics.get(i);
      if (timeColumnName.equals(metric)) {
        continue;
      }
      metric = String.format("SUM(%s)", metric);
      updatedMetrics.add(metric);
    }
    return COMMA.join(updatedMetrics);
  }

  String getBetweenClause(DateTime start, DateTime end, TimeSpec timeFieldSpec) {
    String timeField = timeFieldSpec.getColumnName();
    TimeUnit timeFieldUnit = timeFieldSpec.getBucket().getUnit();
    long startInConvertedUnits = timeFieldUnit.convert(start.getMillis(), TimeUnit.MILLISECONDS);
    long endInConvertedUnits = timeFieldUnit.convert(end.getMillis(), TimeUnit.MILLISECONDS);
    return String.format("%s BETWEEN '%s' AND '%s'", timeField, startInConvertedUnits,
        endInConvertedUnits);
  }

  String getDimensionWhereClause(Multimap<String, String> dimensionValues) {
    List<String> components = new ArrayList<>();
    for (Map.Entry<String, Collection<String>> entry : dimensionValues.asMap().entrySet()) {
      String key = entry.getKey();
      Collection<String> values = entry.getValue();
      String component;
      if (values.isEmpty()) {
        continue;
      } else if (values.size() == 1) {
        component = EQUALS.join(key, String.format("'%s'", values.iterator().next()));
      } else {
        List<String> quotedValues = new ArrayList<>(values.size());
        for (String value : values) {
          quotedValues.add(String.format("'%s'", value));
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

  String getDimensionGroupByClause(Set<String> groupBy, TimeSpec timeSpec,
      boolean shouldGroupByTime) {
    String timeColumnName = timeSpec.getColumnName();
    List<String> groups = new LinkedList<String>();
    if (groupBy != null) {
      groups.addAll(groupBy);
    }
    if (shouldGroupByTime && !groups.contains(timeColumnName)) {
      groups.add(0, timeColumnName);
    }
    return String.format("GROUP BY %s", COMMA.join(groups));
  }

  public String getDataTimeRangeSql(String collection, String timeColumnName) {
    return String.format("select min(%s), max(%s) from %s", timeColumnName, timeColumnName,
        collection);
  }

}
