package com.linkedin.thirdeye.util;

import com.google.common.collect.ImmutableSortedMap;
import com.linkedin.thirdeye.dashboard.util.SqlUtils;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.core.util.UnmodifiableMultivaluedMap;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.util.Map;

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
    MultivaluedMap<String, String> dimensionValues = new MultivaluedMapImpl();
    dimensionValues.putSingle("A", "A1");
    dimensionValues.putSingle("B", "B1");
    dimensionValues.putSingle("C", "!");
    String dimensionWhereClause = SqlUtils.getDimensionWhereClause(dimensionValues, null);
    Assert.assertEquals(dimensionWhereClause, "A = 'A1' AND B = 'B1'");
  }

  @Test
  public void testGetGroupByClause() {
    MultivaluedMap<String, String> dimensionValues = new MultivaluedMapImpl();
    dimensionValues.putSingle("A", "A1");
    dimensionValues.putSingle("B", "B1");
    dimensionValues.putSingle("C", "!");
    dimensionValues.putSingle("D", "!");
    String groupByClause = SqlUtils.getDimensionGroupByClause(dimensionValues);
    Assert.assertEquals(groupByClause, "GROUP BY 'C','D'");
  }
}
