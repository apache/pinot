package com.linkedin.thirdeye.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestThirdEyeQueryParser {
  private ThirdEyeQuery query;
  private DateTime start;
  private DateTime end;

  @BeforeClass
  public void beforeClass() {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY-MM-DD");
    start = formatter.parseDateTime("2015-01-07");
    end = formatter.parseDateTime("2015-01-08");
  }

  // Positive

  @Test
  public void testValid_oneMetric_noDimension() throws Exception {
    query = parse("SELECT m FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("m"));
    Assert.assertTrue(query.getDimensionValues().isEmpty());
    Assert.assertTrue(query.getFunctions().isEmpty());
  }

  @Test
  public void testValid_manyMetrics_noDimension() throws Exception {
    query = parse("SELECT m1, m2 FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("m1", "m2"));
    Assert.assertTrue(query.getDimensionValues().isEmpty());
    Assert.assertTrue(query.getFunctions().isEmpty());
  }

  @Test
  public void testValid_oneMetric_oneDimension() throws Exception {
    query = parse("SELECT m FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08' AND a = 'A1'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("m"));
    Assert.assertEquals(query.getDimensionValues(), ImmutableMultimap.of("a", "A1"));
    Assert.assertTrue(query.getFunctions().isEmpty());
  }

  @Test
  public void testValid_oneMetric_manyDimensions() throws Exception {
    query = parse("SELECT m FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08' AND a = 'A1' AND b = 'B1'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("m"));
    Assert.assertEquals(query.getDimensionValues(), ImmutableMultimap.of("a", "A1", "b", "B1"));
    Assert.assertTrue(query.getFunctions().isEmpty());
  }

  @Test
  public void testValid_oneMetric_orDimensions() throws Exception {
    query = parse("SELECT m FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08' AND a = 'A1' OR a = 'A2' AND b = 'B1'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("m"));
    Assert.assertEquals(query.getDimensionValues(), ImmutableMultimap.of("a", "A1", "a", "A2", "b", "B1"));
    Assert.assertTrue(query.getFunctions().isEmpty());
  }

  @Test
  public void testValid_oneMetric_manyDimensions_betweenAtEnd() throws Exception {
    query = parse("SELECT m FROM collection WHERE a = 'A1' AND b = 'B1' AND time BETWEEN '2015-01-07' AND '2015-01-08'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("m"));
    Assert.assertEquals(query.getDimensionValues(), ImmutableMultimap.of("a", "A1", "b", "B1"));
    Assert.assertTrue(query.getFunctions().isEmpty());
  }

  @Test
  public void testValid_aggregate() throws Exception {
    query = parse("SELECT AGGREGATE_1_HOURS(m) FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("m"));
    Assert.assertTrue(query.getDimensionValues().isEmpty());
    Assert.assertEquals(query.getFunctions().size(), 1);
    Assert.assertEquals(query.getFunctions().get(0).getClass(), ThirdEyeAggregateFunction.class);
  }

  @Test
  public void testValid_derivedAndAggregate() throws Exception {
    query = parse("SELECT AGGREGATE_1_HOURS(RATIO(m1,m2)) FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertTrue(query.getMetricNames().isEmpty());
    Assert.assertTrue(query.getDimensionValues().isEmpty());
    Assert.assertEquals(query.getFunctions().size(), 1);
    Assert.assertEquals(query.getFunctions().get(0).getClass(), ThirdEyeAggregateFunction.class);
    Assert.assertEquals(query.getDerivedMetrics().size(), 1);
    Assert.assertEquals(query.getDerivedMetrics().get(0).getClass(), ThirdEyeRatioFunction.class);
  }

  @Test
  public void testValid_withDots_metricName() throws Exception {
    query = parse("SELECT 'root.m1' FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("root.m1"));
    Assert.assertTrue(query.getDimensionValues().isEmpty());
    Assert.assertTrue(query.getFunctions().isEmpty());
  }

  @Test
  public void testValid_withDots_groupBy() throws Exception {
    query = parse("SELECT 'root.m2' FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08' GROUP BY 'root.m1'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("root.m2"));
    Assert.assertTrue(query.getDimensionValues().isEmpty());
    Assert.assertTrue(query.getFunctions().isEmpty());
    Assert.assertEquals(query.getGroupByColumns(), ImmutableList.of("root.m1"));
  }

  @Test
  public void testValid_ratiosAndRaw() throws Exception {
    query = parse("SELECT m1, m2, RATIO(m2, m3) FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("m1", "m2"));
    Assert.assertTrue(query.getDimensionValues().isEmpty());
    Assert.assertTrue(query.getFunctions().isEmpty());
    Assert.assertEquals(query.getDerivedMetrics().size(), 1);
  }

  @Test
  public void testValid_quotedMetrics() throws Exception {
    query = parse("SELECT 'm1', 'm2', RATIO('m2', 'm3') FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08'");
    Assert.assertEquals(query.getCollection(), "collection");
    Assert.assertEquals(query.getStart(), start);
    Assert.assertEquals(query.getEnd(), end);
    Assert.assertEquals(query.getMetricNames(), ImmutableList.of("m1", "m2"));
    Assert.assertTrue(query.getDimensionValues().isEmpty());
    Assert.assertTrue(query.getFunctions().isEmpty());
    Assert.assertEquals(query.getDerivedMetrics().size(), 1);
  }

  // Negative

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInvalid_allMetrics() throws Exception {
    parse("SELECT * FROM collection WHERE time BETWEEN '2015-01-07' AND '2015-01-08'");
  }

  private ThirdEyeQuery parse(String sql) throws Exception {
    return new ThirdEyeQueryParser(sql).getQuery();
  }
}
