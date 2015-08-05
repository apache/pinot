package com.linkedin.thirdeye.client.util;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSqlUtils {
  @Test
  public void testGetBetweenClause() {
    DateTime start = ISODateTimeFormat.dateTimeParser().parseDateTime("2015-01-01TZ");
    DateTime end = ISODateTimeFormat.dateTimeParser().parseDateTime("2015-01-02TZ");
    String betweenClause = SqlUtils.getBetweenClause(start, end);
    Assert.assertEquals(betweenClause, "time BETWEEN '2015-01-01T00:00:00Z' AND '2015-01-02T00:00:00Z'");
  }

  @Test
  public void testGetDimensionWhereClause() {
    Multimap<String, String> dimensionValues = ImmutableMultimap.of(
        "A", "A1",
        "B", "B1",
        "C", "!");
    String dimensionWhereClause = SqlUtils.getDimensionWhereClause(dimensionValues);
    Assert.assertEquals(dimensionWhereClause, "A = 'A1' AND B = 'B1'");
  }

  @Test
  public void testGetGroupByClause() {
    Multimap<String, String> dimensionValues = ImmutableMultimap.of(
        "A", "A1",
        "B", "B1",
        "C", "!");
    String groupByClause = SqlUtils.getDimensionGroupByClause(dimensionValues);
    Assert.assertEquals(groupByClause, "GROUP BY 'C'");
  }
}
