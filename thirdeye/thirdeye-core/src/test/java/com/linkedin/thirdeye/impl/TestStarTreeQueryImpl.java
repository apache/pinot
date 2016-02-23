package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.TimeRange;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;

public class TestStarTreeQueryImpl {
  private StarTreeConfig config;

  @BeforeClass
  public void beforeClass() throws Exception {
    config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("sample-config.yml"));
  }

  @Test
  public void testGetStarDimensionNames() throws Exception {
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
        .setDimensionKey(getDimensionKey("*", "B0", "*")).build(config);

    Assert.assertEquals(query.getStarDimensionNames(),
        new HashSet<String>(Arrays.asList("A", "C")));
  }

  @Test
  public void testEqualsNoTime() throws Exception {
    StarTreeQuery q1 = new StarTreeQueryImpl.Builder()
        .setDimensionKey(getDimensionKey("*", "B0", "*")).build(config);

    StarTreeQuery q2 = new StarTreeQueryImpl.Builder()
        .setDimensionKey(getDimensionKey("*", "B0", "*")).build(config);

    StarTreeQuery q3 = new StarTreeQueryImpl.Builder()
        .setDimensionKey(getDimensionKey("*", "*", "*")).build(config);

    Assert.assertEquals(q1, q2);
    Assert.assertNotEquals(q1, q3);
  }

  @Test
  public void testEqualsTimeRange() throws Exception {
    StarTreeQuery q1 =
        new StarTreeQueryImpl.Builder().setDimensionKey(getDimensionKey("*", "B0", "*"))
            .setTimeRange(new TimeRange(1L, 3L)).build(config);

    StarTreeQuery q2 =
        new StarTreeQueryImpl.Builder().setDimensionKey(getDimensionKey("*", "B0", "*"))
            .setTimeRange(new TimeRange(1L, 3L)).build(config);

    StarTreeQuery q3 = new StarTreeQueryImpl.Builder() // no buckets
        .setDimensionKey(getDimensionKey("*", "B0", "*")).build(config);

    StarTreeQuery q4 =
        new StarTreeQueryImpl.Builder().setDimensionKey(getDimensionKey("*", "B0", "*"))
            .setTimeRange(new TimeRange(1L, 4L)).build(config);

    Assert.assertEquals(q2, q1);
    Assert.assertNotEquals(q3, q1);
    Assert.assertNotEquals(q4, q1);
  }

  private DimensionKey getDimensionKey(String a, String b, String c) {
    return new DimensionKey(new String[] {
        a, b, c
    });
  }
}
