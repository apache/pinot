package com.linkedin.thirdeye.client.util;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;

public class TestPqlUtils {

  private static final TimeSpec DEFAULT_TIME_SPEC = new TimeSpec("timeColumn",
      new TimeGranularity(5, TimeUnit.MILLISECONDS), new TimeGranularity(1, TimeUnit.HOURS), null);

  @Test
  public void testGetSelectionClause() {
    String selectionClause =
        PqlUtils.getSelectionClause(Arrays.asList("A", "B", "timeColumn"), DEFAULT_TIME_SPEC);
    Assert.assertEquals(selectionClause, "SUM(A),SUM(B)");
  }

  @Test
  public void testGetBetweenClause() {
    DateTime start =
        ISODateTimeFormat.dateTimeParser().parseDateTime("2016-01-01T00:00:00.000+00:00");
    DateTime end =
        ISODateTimeFormat.dateTimeParser().parseDateTime("2016-01-02T00:00:00.000+00:00");
    String betweenClause = PqlUtils.getBetweenClause(start, end, DEFAULT_TIME_SPEC);
    Assert.assertEquals(betweenClause, "timeColumn BETWEEN '403224' AND '403248'");
  }

  @Test
  public void testGetDimensionWhereClause() {
    Multimap<String, String> dimensionValues =
        ImmutableMultimap.of("A", "A1", "B", "B1", "B", "B2");
    String dimensionWhereClause = PqlUtils.getDimensionWhereClause(dimensionValues);
    Assert.assertEquals(dimensionWhereClause, "A = 'A1' AND B IN ('B1','B2')");
  }

  @Test
  public void testGetGroupByClause() {
    Set<String> groupBy = new LinkedHashSet<String>(Arrays.asList("C", "D"));
    String groupByClause = PqlUtils.getDimensionGroupByClause(groupBy, DEFAULT_TIME_SPEC);
    Assert.assertEquals(groupByClause, "GROUP BY timeColumn,C,D");
  }

  @Test
  public void testGetDataTimeRangeSql() {
    String dataTimeRangeSql = PqlUtils.getDataTimeRangeSql("abook", "timeColumn");
    Assert.assertEquals(dataTimeRangeSql, "select min(timeColumn), max(timeColumn) from abook");
  }
}
